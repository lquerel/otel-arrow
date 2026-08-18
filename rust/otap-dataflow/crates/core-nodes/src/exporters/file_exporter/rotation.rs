// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Crash-recoverable rotation manifests and bounded finalized-file retention.
//!
//! Each signal writer owns two alternating manifest slots beside its active file. A new state is
//! fully written and synchronized to the older slot before filesystem mutation begins, so a torn
//! update leaves the other slot readable on every supported platform. A pending rotation record
//! makes rename/create recovery deterministic without discovering or deleting unrelated files.

use super::config::{OpenMode, RotationConfig};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions, try_exists};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

/// Current on-disk manifest schema version.
const MANIFEST_VERSION: u8 = 1;
/// Maximum bytes accepted from one manifest slot.
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
/// Hard safety bound for finalized entries decoded before configuration pruning.
const MAX_MANIFEST_SEGMENTS: usize = 1_001;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    state: ManifestState,
    checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestState {
    version: u8,
    revision: u64,
    next_sequence: u64,
    active_started_unix_millis: u64,
    segments: Vec<SegmentRecord>,
    pending: Option<SegmentRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecord {
    sequence: u64,
    finalized_unix_millis: u64,
}

/// Rotation and retention state for one active signal file.
pub struct RotationState {
    active_path: PathBuf,
    slots: [PathBuf; 2],
    config: RotationConfig,
    state: ManifestState,
    active_deadline: Option<Instant>,
}

impl RotationState {
    /// Loads the newest valid manifest slot and reconciles any interrupted rotation.
    pub async fn open(
        active_path: &Path,
        config: RotationConfig,
        open_mode: OpenMode,
    ) -> io::Result<Self> {
        let slots = manifest_paths(active_path);
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        for slot in &slots {
            match read_slot(slot).await {
                Ok(Some(state)) => valid.push(state),
                Ok(None) => {}
                Err(error) => invalid.push(error),
            }
        }
        let now = unix_millis()?;
        let state = match valid.into_iter().max_by_key(|state| state.revision) {
            Some(state) => state,
            None if invalid.is_empty() => ManifestState {
                version: MANIFEST_VERSION,
                revision: 0,
                next_sequence: 0,
                active_started_unix_millis: now,
                segments: Vec::new(),
                pending: None,
            },
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("no valid rotation manifest slot: {}", invalid.remove(0)),
                ));
            }
        };
        let mut rotation = Self {
            active_path: active_path.to_owned(),
            slots,
            config,
            state,
            active_deadline: None,
        };
        rotation.recover_pending().await?;
        if open_mode != OpenMode::Append {
            rotation.state.active_started_unix_millis = now;
            rotation.persist().await?;
        } else if rotation.state.revision == 0 {
            rotation.persist().await?;
        }
        rotation.prune().await?;
        rotation.reset_active_deadline()?;
        Ok(rotation)
    }

    /// Returns whether the next frame requires size-based rotation first.
    #[must_use]
    pub fn size_due(&self, active_bytes: u64, frame_bytes: usize) -> bool {
        active_bytes != 0
            && self
                .config
                .max_bytes
                .is_some_and(|limit| active_bytes.saturating_add(frame_bytes as u64) > limit)
    }

    /// Returns the remaining time until a non-empty active file must rotate.
    pub fn time_until_due(&self, active_bytes: u64) -> io::Result<Option<Duration>> {
        if active_bytes == 0 {
            return Ok(None);
        }
        let Some(deadline) = self.active_deadline else {
            return Ok(None);
        };
        Ok(Some(deadline.saturating_duration_since(Instant::now())))
    }

    /// Returns the remaining time until the oldest finalized file expires by age.
    pub fn time_until_retention(&self) -> io::Result<Option<Duration>> {
        let Some(max_age) = self.config.retention.max_age else {
            return Ok(None);
        };
        let Some(oldest_millis) = self
            .state
            .segments
            .iter()
            .map(|segment| segment.finalized_unix_millis)
            .min()
        else {
            return Ok(None);
        };
        let elapsed_millis = unix_millis()?.saturating_sub(oldest_millis);
        Ok(Some(
            max_age.saturating_sub(Duration::from_millis(elapsed_millis)),
        ))
    }

    /// Applies age retention when its oldest finalized segment has expired.
    pub async fn prune_if_due(&mut self) -> io::Result<bool> {
        let due = self
            .time_until_retention()?
            .is_some_and(|remaining| remaining.is_zero());
        if due {
            self.prune().await?;
        }
        Ok(due)
    }

    /// Persists a pending record before the active file is renamed.
    pub async fn begin_rotation(&mut self) -> io::Result<PathBuf> {
        let sequence = self.state.next_sequence;
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "rotation sequence exhausted")
        })?;
        let target = segment_path(&self.active_path, sequence);
        if try_exists(&target).await? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "rotation target already exists",
            ));
        }
        self.state.next_sequence = next_sequence;
        self.state.pending = Some(SegmentRecord {
            sequence,
            finalized_unix_millis: unix_millis()?,
        });
        self.persist().await?;
        Ok(target)
    }

    /// Commits the pending segment, resets the active-file clock, and applies retention.
    pub async fn finish_rotation(&mut self) -> io::Result<()> {
        let pending = self.state.pending.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "rotation has no pending segment",
            )
        })?;
        if !self
            .state
            .segments
            .iter()
            .any(|segment| segment.sequence == pending.sequence)
        {
            self.state.segments.push(pending);
        }
        self.state.active_started_unix_millis = unix_millis()?;
        self.persist().await?;
        self.prune().await?;
        self.reset_active_deadline()
    }

    async fn recover_pending(&mut self) -> io::Result<()> {
        let Some(pending) = self.state.pending else {
            return Ok(());
        };
        let target = segment_path(&self.active_path, pending.sequence);
        if try_exists(&target).await? {
            if !self
                .state
                .segments
                .iter()
                .any(|segment| segment.sequence == pending.sequence)
            {
                self.state.segments.push(pending);
            }
            self.state.active_started_unix_millis = unix_millis()?;
        }
        self.state.pending = None;
        self.persist().await
    }

    async fn prune(&mut self) -> io::Result<()> {
        let now = unix_millis()?;
        let cutoff = self.config.retention.max_age.map(|max_age| {
            now.saturating_sub(u64::try_from(max_age.as_millis()).unwrap_or(u64::MAX))
        });
        let count_excess = self
            .state
            .segments
            .len()
            .saturating_sub(self.config.retention.max_backups);
        let mut retained = Vec::with_capacity(self.state.segments.len());
        for (index, segment) in self.state.segments.iter().copied().enumerate() {
            let expired_by_count = index < count_excess;
            let expired_by_age =
                cutoff.is_some_and(|cutoff| segment.finalized_unix_millis <= cutoff);
            if expired_by_count || expired_by_age {
                let path = segment_path(&self.active_path, segment.sequence);
                match tokio::fs::remove_file(path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            } else {
                let path = segment_path(&self.active_path, segment.sequence);
                if try_exists(path).await? {
                    retained.push(segment);
                }
            }
        }
        if retained.len() != self.state.segments.len() {
            self.state.segments = retained;
            self.persist().await?;
        }
        Ok(())
    }

    async fn persist(&mut self) -> io::Result<()> {
        let revision = self.state.revision.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "manifest revision exhausted")
        })?;
        self.state.revision = revision;
        let state_bytes = serde_json::to_vec(&self.state).map_err(io::Error::other)?;
        let envelope = ManifestEnvelope {
            checksum: blake3::hash(&state_bytes).to_hex().to_string(),
            state: self.state.clone(),
        };
        let bytes = serde_json::to_vec(&envelope).map_err(io::Error::other)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rotation manifest exceeds its size limit",
            ));
        }
        let slot = &self.slots[(revision % 2) as usize];
        let mut options = OpenOptions::new();
        _ = options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            _ = options.mode(0o600);
        }
        let mut file = options.open(slot).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_data().await
    }

    fn reset_active_deadline(&mut self) -> io::Result<()> {
        self.active_deadline = match self.config.max_duration {
            Some(max_duration) => {
                let elapsed_millis =
                    unix_millis()?.saturating_sub(self.state.active_started_unix_millis);
                let remaining = max_duration.saturating_sub(Duration::from_millis(elapsed_millis));
                Some(Instant::now() + remaining)
            }
            None => None,
        };
        Ok(())
    }
}

async fn read_slot(path: &Path) -> io::Result<Option<ManifestState>> {
    let file = match File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    _ = file
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rotation manifest exceeds its size limit",
        ));
    }
    let envelope: ManifestEnvelope = serde_json::from_slice(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid rotation manifest: {error}"),
        )
    })?;
    let state_bytes = serde_json::to_vec(&envelope.state).map_err(io::Error::other)?;
    if envelope.checksum != blake3::hash(&state_bytes).to_hex().as_str() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rotation manifest checksum mismatch",
        ));
    }
    validate_state(&envelope.state)?;
    Ok(Some(envelope.state))
}

fn validate_state(state: &ManifestState) -> io::Result<()> {
    if state.version != MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported rotation manifest version",
        ));
    }
    if state.segments.len() > MAX_MANIFEST_SEGMENTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rotation manifest contains too many segments",
        ));
    }
    let mut previous = None;
    for segment in &state.segments {
        if previous.is_some_and(|value| segment.sequence <= value)
            || segment.sequence >= state.next_sequence
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rotation manifest segment sequence is invalid",
            ));
        }
        previous = Some(segment.sequence);
    }
    if state.pending.is_some_and(|pending| {
        pending.sequence >= state.next_sequence
            || state
                .segments
                .iter()
                .any(|segment| segment.sequence == pending.sequence)
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rotation manifest pending sequence is invalid",
        ));
    }
    Ok(())
}

fn unix_millis() -> io::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::other("system clock is before the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| io::Error::other("system time exceeds manifest range"))
}

fn suffixed_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn manifest_paths(active_path: &Path) -> [PathBuf; 2] {
    [
        suffixed_path(active_path, ".manifest.0.json"),
        suffixed_path(active_path, ".manifest.1.json"),
    ]
}

/// Returns the deterministic uncompressed path for a finalized sequence.
pub(crate) fn segment_path(active_path: &Path, sequence: u64) -> PathBuf {
    suffixed_path(active_path, &format!(".{sequence:020}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::file_exporter::config::RetentionConfig;
    use tempfile::tempdir;

    fn config(max_backups: usize) -> RotationConfig {
        RotationConfig {
            max_bytes: Some(128),
            max_duration: None,
            retention: RetentionConfig {
                max_backups,
                max_age: None,
            },
        }
    }

    /// Scenario: Finalized segment and manifest names are derived from a non-UTF8-safe path type.
    /// Guarantees: Naming appends fixed suffixes without changing the configured active path bytes.
    #[test]
    fn derived_paths_append_deterministic_suffixes() {
        let active = Path::new("/tmp/capture.jsonl");
        assert_eq!(
            segment_path(active, 42),
            PathBuf::from("/tmp/capture.jsonl.00000000000000000042")
        );
        assert_eq!(
            manifest_paths(active),
            [
                PathBuf::from("/tmp/capture.jsonl.manifest.0.json"),
                PathBuf::from("/tmp/capture.jsonl.manifest.1.json")
            ]
        );
    }

    /// Scenario: One of two manifest slots is truncated after a newer valid state was persisted.
    /// Guarantees: Startup selects the remaining valid slot instead of losing owned segment state.
    #[tokio::test]
    async fn alternating_manifest_recovers_from_one_torn_slot() {
        let dir = tempdir().unwrap();
        let active = dir.path().join("capture.jsonl");
        tokio::fs::write(&active, b"{}\n").await.unwrap();
        let mut rotation = RotationState::open(&active, config(10), OpenMode::Append)
            .await
            .unwrap();
        let target = rotation.begin_rotation().await.unwrap();
        tokio::fs::rename(&active, &target).await.unwrap();
        tokio::fs::write(&active, b"").await.unwrap();
        rotation.finish_rotation().await.unwrap();
        let newest_slot = rotation.slots[(rotation.state.revision % 2) as usize].clone();
        tokio::fs::write(newest_slot, b"{").await.unwrap();
        drop(rotation);

        let recovered = RotationState::open(&active, config(10), OpenMode::Append)
            .await
            .unwrap();
        assert_eq!(recovered.state.segments.len(), 1);
        assert!(target.exists());
    }

    /// Scenario: More finalized files exist than the configured count retention permits.
    /// Guarantees: Only manifest-owned oldest segments are removed and retained state stays bounded.
    #[tokio::test]
    async fn count_retention_prunes_oldest_owned_segments() {
        let dir = tempdir().unwrap();
        let active = dir.path().join("capture.jsonl");
        tokio::fs::write(&active, b"{}\n").await.unwrap();
        let unrelated = dir.path().join("unrelated");
        tokio::fs::write(&unrelated, b"keep").await.unwrap();
        let mut rotation = RotationState::open(&active, config(1), OpenMode::Append)
            .await
            .unwrap();
        let first = rotation.begin_rotation().await.unwrap();
        tokio::fs::rename(&active, &first).await.unwrap();
        tokio::fs::write(&active, b"{}\n").await.unwrap();
        rotation.finish_rotation().await.unwrap();
        let second = rotation.begin_rotation().await.unwrap();
        tokio::fs::rename(&active, &second).await.unwrap();
        tokio::fs::write(&active, b"").await.unwrap();
        rotation.finish_rotation().await.unwrap();

        assert!(!first.exists());
        assert!(second.exists());
        assert!(unrelated.exists());
        assert_eq!(rotation.state.segments.len(), 1);
    }

    /// Scenario: A finalized segment becomes older than the configured age retention duration.
    /// Guarantees: Age pruning deletes the owned segment even below the backup-count limit.
    #[tokio::test]
    async fn age_retention_expires_owned_segments() {
        let dir = tempdir().unwrap();
        let active = dir.path().join("capture.jsonl");
        tokio::fs::write(&active, b"{}\n").await.unwrap();
        let mut rotation_config = config(10);
        rotation_config.retention.max_age = Some(Duration::from_millis(100));
        let mut rotation = RotationState::open(&active, rotation_config, OpenMode::Append)
            .await
            .unwrap();
        let finalized = rotation.begin_rotation().await.unwrap();
        tokio::fs::rename(&active, &finalized).await.unwrap();
        tokio::fs::write(&active, b"").await.unwrap();
        rotation.finish_rotation().await.unwrap();
        tokio::time::sleep(Duration::from_millis(125)).await;
        assert!(rotation.prune_if_due().await.unwrap());

        assert!(!finalized.exists());
        assert!(rotation.state.segments.is_empty());
    }
}
