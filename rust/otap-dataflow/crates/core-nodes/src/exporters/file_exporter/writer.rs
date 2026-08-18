// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Async file lifecycle, tail recovery, and transactional frame writes.
//!
//! Each signal writer owns one active file, a process-local path lease, and optional manifest state.
//! Frame writes and lifecycle transitions stay ordered on the local runtime. At most one blocking
//! compression task per writer may overlap active writes and is drained before dependent work.

use super::compression::CompressionWorker;
use super::config::{
    Durability, FileCompression, FileExporterConfig, FileFormat, OpenMode, TailRecovery,
};
use super::metrics::FileOperation;
use super::rotation::RotationState;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

static PATH_LEASES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

/// Maximum delay before the run loop observes a completed background compression job.
const COMPRESSION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Tail bytes removed while opening an append-mode writer.
#[derive(Debug, Clone, Copy, Default)]
pub struct TailRecoveryResult {
    /// Number of bytes truncated from the incomplete final frame.
    pub recovered_bytes: u64,
}

/// Failure of one writer operation, optionally with a failed rollback.
#[derive(Debug)]
pub struct WriterFailure {
    /// Operation that first failed.
    pub operation: FileOperation,
    /// Human-readable error without a destination path.
    pub error: String,
    /// Rollback error that makes the file state indeterminate.
    pub rollback_error: Option<String>,
    /// Fatal state error that requires writer reconstruction before more data is accepted.
    pub fatal_error: Option<String>,
}

/// Lifecycle work completed while servicing a write or timer deadline.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriterProgress {
    /// Whether the active file was finalized by rotation.
    pub rotated: bool,
    /// Number of compressed files committed to the rotation manifest.
    pub compressions: u64,
}

impl WriterFailure {
    fn new(operation: FileOperation, error: impl ToString) -> Self {
        Self {
            operation,
            error: error.to_string(),
            rollback_error: None,
            fatal_error: None,
        }
    }

    fn with_rollback(mut self, error: impl ToString) -> Self {
        let error = error.to_string();
        self.rollback_error = Some(error.clone());
        self.fatal_error = Some(error);
        self
    }

    fn with_fatal(mut self, error: impl ToString) -> Self {
        self.fatal_error = Some(error.to_string());
        self
    }

    /// Returns whether continuing with this writer could corrupt or mis-own output.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        self.fatal_error.is_some()
    }
}

/// Exclusively owned async writer for one resolved signal path.
pub struct SignalWriter {
    file: Option<File>,
    active_path: PathBuf,
    active_bytes: u64,
    durability: Durability,
    rotation: Option<RotationState>,
    configured_compression: Option<FileCompression>,
    compression_worker: Option<CompressionWorker>,
    _lease: PathLease,
}

impl SignalWriter {
    /// Opens and repairs a signal writer before returning it as ready.
    pub async fn open(
        path: &Path,
        config: &FileExporterConfig,
    ) -> Result<(Self, TailRecoveryResult), WriterFailure> {
        let lease = PathLease::acquire(path)
            .await
            .map_err(|error| WriterFailure::new(FileOperation::Open, error))?;
        if config.create_directories {
            create_parent_directories(path)
                .await
                .map_err(|error| WriterFailure::new(FileOperation::Open, error))?;
        }
        let mut options = OpenOptions::new();
        _ = options.read(true);
        match config.open_mode {
            OpenMode::Append => {
                #[cfg(windows)]
                {
                    // Windows append-only handles cannot call set_len(). Each frame write seeks
                    // to EOF first, so write access preserves exporter append behavior while
                    // allowing tail recovery and transactional rollback.
                    _ = options.write(true).create(true);
                }
                #[cfg(not(windows))]
                {
                    _ = options.append(true).create(true);
                }
            }
            OpenMode::Truncate => {
                _ = options.write(true).create(true).truncate(true);
            }
            OpenMode::CreateNew => {
                #[cfg(windows)]
                {
                    // Use the same writable Windows handle as append mode so rollback can
                    // truncate a partially written first frame.
                    _ = options.write(true).create_new(true);
                }
                #[cfg(not(windows))]
                {
                    _ = options.append(true).create_new(true);
                }
            }
        }
        #[cfg(unix)]
        {
            _ = options.mode(0o600);
        }
        let file = options
            .open(path)
            .await
            .map_err(|error| WriterFailure::new(FileOperation::Open, error))?;
        let mut writer = Self {
            file: Some(file),
            active_path: path.to_owned(),
            active_bytes: 0,
            durability: config.durability,
            rotation: None,
            configured_compression: config.compression,
            compression_worker: None,
            _lease: lease,
        };
        let recovery = if let Some(policy) = config.effective_tail_recovery() {
            writer
                .recover_tail(config.format, policy, config.max_frame_bytes)
                .await
                .map_err(|error| WriterFailure::new(FileOperation::Open, error))?
        } else {
            TailRecoveryResult::default()
        };
        writer.active_bytes = writer
            .file_mut()
            .metadata()
            .await
            .map_err(|error| WriterFailure::new(FileOperation::Open, error))?
            .len();
        if let Some(rotation) = config.rotation.clone() {
            if tokio::fs::symlink_metadata(path)
                .await
                .map_err(|error| WriterFailure::new(FileOperation::Open, error))?
                .file_type()
                .is_symlink()
            {
                return Err(WriterFailure::new(
                    FileOperation::Open,
                    "rotation does not support a symlink as the active file",
                ));
            }
            writer.rotation = Some(
                RotationState::open(path, rotation, config.open_mode)
                    .await
                    .map_err(|error| WriterFailure::new(FileOperation::Open, error))?,
            );
            writer
                .start_next_compression()
                .await
                .map_err(|failure| WriterFailure::new(FileOperation::Open, failure.error))?;
        }
        Ok((writer, recovery))
    }

    /// Writes one complete frame and rolls back to the prior length on failure.
    pub async fn write_frame(&mut self, frame: &[u8]) -> Result<WriterProgress, WriterFailure> {
        let mut progress = WriterProgress::default();
        progress.compressions += self.finish_compression(false).await?;
        let (rotation_due, retention_due) = match &self.rotation {
            Some(rotation) => {
                let rotation_due = rotation.size_due(self.active_bytes, frame.len())
                    || rotation
                        .time_until_due(self.active_bytes)
                        .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?
                        .is_some_and(|remaining| remaining.is_zero());
                let retention_due = rotation
                    .time_until_retention()
                    .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?
                    .is_some_and(|remaining| remaining.is_zero());
                (rotation_due, retention_due)
            }
            None => (false, false),
        };
        if rotation_due {
            progress.compressions += self.rotate().await?;
            progress.rotated = true;
        } else if retention_due {
            progress.compressions += self.drain_compression().await?;
            _ = self
                .rotation
                .as_mut()
                .expect("retention exists only with rotation state")
                .prune_if_due()
                .await
                .map_err(|error| {
                    WriterFailure::new(FileOperation::Rotate, error)
                        .with_fatal("rotation manifest requires restart recovery")
                })?;
        }
        let start_len = self.active_bytes;
        _ = self
            .file_mut()
            .seek(io::SeekFrom::End(0))
            .await
            .map_err(|error| WriterFailure::new(FileOperation::Write, error))?;
        if let Err(error) = async {
            self.file_mut().write_all(frame).await?;
            self.file_mut().flush().await
        }
        .await
        {
            let failure = WriterFailure::new(FileOperation::Write, error);
            return match self.rollback(start_len).await {
                Ok(()) => Err(failure),
                Err(rollback_error) => Err(failure.with_rollback(rollback_error)),
            };
        }
        if self.durability == Durability::SyncData
            && let Err(error) = self.file_mut().sync_data().await
        {
            let failure = WriterFailure::new(FileOperation::Sync, error);
            return match self.rollback(start_len).await {
                Ok(()) => Err(failure),
                Err(rollback_error) => Err(failure.with_rollback(rollback_error)),
            };
        }
        self.active_bytes = start_len.saturating_add(frame.len() as u64);
        Ok(progress)
    }

    /// Returns the duration until time rotation or age retention next needs service.
    pub fn time_until_lifecycle(&self) -> Result<Option<std::time::Duration>, WriterFailure> {
        match &self.rotation {
            Some(rotation) => {
                let rotation_delay = rotation
                    .time_until_due(self.active_bytes)
                    .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?;
                let retention_delay = rotation
                    .time_until_retention()
                    .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?;
                let lifecycle_delay = match (rotation_delay, retention_delay) {
                    (Some(rotation), Some(retention)) => Some(rotation.min(retention)),
                    (Some(delay), None) | (None, Some(delay)) => Some(delay),
                    (None, None) => None,
                };
                Ok(match (lifecycle_delay, self.compression_delay()) {
                    (Some(lifecycle), Some(compression)) => Some(lifecycle.min(compression)),
                    (Some(delay), None) | (None, Some(delay)) => Some(delay),
                    (None, None) => None,
                })
            }
            None => Ok(None),
        }
    }

    /// Services elapsed time rotation and age retention deadlines.
    pub async fn maintain_if_due(&mut self) -> Result<WriterProgress, WriterFailure> {
        let mut progress = WriterProgress::default();
        progress.compressions += self.finish_compression(false).await?;
        let rotation_due = self
            .rotation
            .as_ref()
            .map(|rotation| rotation.time_until_due(self.active_bytes))
            .transpose()
            .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?
            .flatten()
            .is_some_and(|remaining| remaining.is_zero());
        if rotation_due {
            progress.compressions += self.rotate().await?;
            progress.rotated = true;
            return Ok(progress);
        }
        let retention_due = self
            .rotation
            .as_ref()
            .map(|rotation| rotation.time_until_retention())
            .transpose()
            .map_err(|error| WriterFailure::new(FileOperation::Rotate, error))?
            .flatten()
            .is_some_and(|remaining| remaining.is_zero());
        if retention_due {
            progress.compressions += self.drain_compression().await?;
        }
        if let Some(rotation) = &mut self.rotation {
            _ = rotation.prune_if_due().await.map_err(|error| {
                WriterFailure::new(FileOperation::Rotate, error)
                    .with_fatal("rotation manifest requires restart recovery")
            })?;
        }
        Ok(progress)
    }

    /// Flushes and synchronizes a ready writer during graceful shutdown.
    pub async fn finalize(&mut self) -> Result<u64, WriterFailure> {
        let flush_result = self.file_mut().flush().await;
        let sync_result = self.file_mut().sync_data().await;
        match (flush_result, sync_result) {
            (Err(error), _) => Err(WriterFailure::new(FileOperation::Write, error)),
            (Ok(()), Err(error)) => Err(WriterFailure::new(FileOperation::Sync, error)),
            (Ok(()), Ok(())) => {
                let compressions = self.drain_compression().await?;
                Ok(compressions)
            }
        }
    }

    async fn rollback(&mut self, length: u64) -> io::Result<()> {
        self.file_mut().set_len(length).await?;
        _ = self.file_mut().seek(io::SeekFrom::End(0)).await?;
        self.file_mut().sync_data().await?;
        self.active_bytes = length;
        Ok(())
    }

    async fn rotate(&mut self) -> Result<u64, WriterFailure> {
        if self.active_bytes == 0 {
            return Ok(0);
        }
        let compressions = self.drain_compression().await?;
        let target = self
            .rotation
            .as_mut()
            .ok_or_else(|| WriterFailure::new(FileOperation::Rotate, "rotation is disabled"))?
            .begin_rotation()
            .await
            .map_err(|error| {
                WriterFailure::new(FileOperation::Rotate, error)
                    .with_fatal("rotation manifest requires restart recovery")
            })?;
        if let Err(error) = async {
            self.file_mut().flush().await?;
            self.file_mut().sync_data().await
        }
        .await
        {
            return Err(WriterFailure::new(FileOperation::Rotate, error)
                .with_fatal("rotation manifest requires restart recovery"));
        }
        drop(self.file.take());
        if let Err(error) = tokio::fs::rename(&self.active_path, &target).await {
            return Err(WriterFailure::new(FileOperation::Rotate, error)
                .with_fatal("rotation manifest requires restart recovery"));
        }
        let file = match open_new_active_file(&self.active_path).await {
            Ok(file) => file,
            Err(error) => {
                return Err(WriterFailure::new(FileOperation::Rotate, error)
                    .with_fatal("rotation manifest requires restart recovery"));
            }
        };
        self.file = Some(file);
        self.active_bytes = 0;
        if let Err(error) = self
            .rotation
            .as_mut()
            .expect("rotation exists throughout rotation")
            .finish_rotation()
            .await
        {
            return Err(WriterFailure::new(FileOperation::Rotate, error)
                .with_fatal("rotation manifest requires restart recovery"));
        }
        self.start_next_compression().await?;
        Ok(compressions)
    }

    fn compression_delay(&self) -> Option<std::time::Duration> {
        self.compression_worker.as_ref().map(|worker| {
            if worker.is_finished() {
                std::time::Duration::ZERO
            } else {
                COMPRESSION_POLL_INTERVAL
            }
        })
    }

    async fn start_next_compression(&mut self) -> Result<(), WriterFailure> {
        if self.compression_worker.is_some() {
            return Ok(());
        }
        let Some(rotation) = &mut self.rotation else {
            return Ok(());
        };
        let request = rotation
            .next_compression_request(self.configured_compression)
            .await
            .map_err(|error| {
                WriterFailure::new(FileOperation::Compress, error)
                    .with_fatal("compression manifest state requires restart recovery")
            })?;
        if let Some(request) = request {
            self.compression_worker = Some(CompressionWorker::start(request));
        }
        Ok(())
    }

    async fn finish_compression(&mut self, wait: bool) -> Result<u64, WriterFailure> {
        let Some(worker) = &self.compression_worker else {
            self.start_next_compression().await?;
            return Ok(0);
        };
        if !wait && !worker.is_finished() {
            return Ok(0);
        }
        let worker = self
            .compression_worker
            .take()
            .expect("compression worker was checked above");
        let request = worker.finish().await.map_err(|error| {
            WriterFailure::new(FileOperation::Compress, error)
                .with_fatal("background compression requires restart recovery")
        })?;
        self.rotation
            .as_mut()
            .expect("compression is paired with rotation")
            .complete_compression(request.sequence, request.codec)
            .await
            .map_err(|error| {
                WriterFailure::new(FileOperation::Compress, error)
                    .with_fatal("compression manifest state requires restart recovery")
            })?;
        self.start_next_compression().await?;
        Ok(1)
    }

    async fn drain_compression(&mut self) -> Result<u64, WriterFailure> {
        let mut completed = 0_u64;
        loop {
            self.start_next_compression().await?;
            if self.compression_worker.is_none() {
                return Ok(completed);
            }
            completed = completed.saturating_add(self.finish_compression(true).await?);
        }
    }

    fn file_mut(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("active file is present outside a fatal rotation failure")
    }

    async fn recover_tail(
        &mut self,
        format: FileFormat,
        policy: TailRecovery,
        max_frame_bytes: usize,
    ) -> io::Result<TailRecoveryResult> {
        match format {
            FileFormat::OtlpJson => self.recover_json_tail(policy, max_frame_bytes).await,
            FileFormat::OtlpProto => self.recover_proto_tail(policy, max_frame_bytes).await,
        }
    }

    async fn recover_json_tail(
        &mut self,
        policy: TailRecovery,
        max_frame_bytes: usize,
    ) -> io::Result<TailRecoveryResult> {
        let len = self.file_mut().metadata().await?.len();
        if len == 0 {
            return Ok(TailRecoveryResult::default());
        }
        _ = self.file_mut().seek(io::SeekFrom::End(-1)).await?;
        let mut final_byte = [0_u8; 1];
        _ = self.file_mut().read_exact(&mut final_byte).await?;
        if final_byte[0] == b'\n' {
            return Ok(TailRecoveryResult::default());
        }
        if policy == TailRecovery::Fail {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "append target ends with an incomplete frame",
            ));
        }
        let scan_len = len.min(max_frame_bytes as u64);
        let scan_start = len - scan_len;
        _ = self
            .file_mut()
            .seek(io::SeekFrom::Start(scan_start))
            .await?;
        let mut tail = vec![0_u8; scan_len as usize];
        _ = self.file_mut().read_exact(&mut tail).await?;
        let Some(boundary) = tail.iter().rposition(|byte| *byte == b'\n') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no complete frame boundary found within max_frame_bytes",
            ));
        };
        let recovered_len = scan_start + boundary as u64 + 1;
        self.file_mut().set_len(recovered_len).await?;
        self.file_mut().sync_data().await?;
        Ok(TailRecoveryResult {
            recovered_bytes: len - recovered_len,
        })
    }

    async fn recover_proto_tail(
        &mut self,
        policy: TailRecovery,
        max_frame_bytes: usize,
    ) -> io::Result<TailRecoveryResult> {
        const PREFIX_BYTES: u64 = size_of::<u32>() as u64;

        let len = self.file_mut().metadata().await?.len();
        let mut offset = 0_u64;
        while offset < len {
            let remaining = len - offset;
            if remaining < PREFIX_BYTES {
                return self.handle_partial_proto_tail(offset, len, policy).await;
            }

            _ = self.file_mut().seek(io::SeekFrom::Start(offset)).await?;
            let mut prefix = [0_u8; size_of::<u32>()];
            _ = self.file_mut().read_exact(&mut prefix).await?;
            let payload_len = u32::from_be_bytes(prefix) as u64;
            let frame_len = PREFIX_BYTES + payload_len;
            if frame_len > max_frame_bytes as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "protobuf frame exceeds max_frame_bytes",
                ));
            }
            if frame_len > remaining {
                return self.handle_partial_proto_tail(offset, len, policy).await;
            }
            offset += frame_len;
        }
        Ok(TailRecoveryResult::default())
    }

    async fn handle_partial_proto_tail(
        &mut self,
        complete_len: u64,
        original_len: u64,
        policy: TailRecovery,
    ) -> io::Result<TailRecoveryResult> {
        if policy == TailRecovery::Fail {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "append target ends with an incomplete frame",
            ));
        }
        if complete_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no complete protobuf frame boundary found",
            ));
        }
        self.file_mut().set_len(complete_len).await?;
        self.file_mut().sync_data().await?;
        Ok(TailRecoveryResult {
            recovered_bytes: original_len - complete_len,
        })
    }
}

async fn open_new_active_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    _ = options.read(true).create_new(true);
    #[cfg(windows)]
    {
        _ = options.write(true);
    }
    #[cfg(not(windows))]
    {
        _ = options.append(true);
    }
    #[cfg(unix)]
    {
        _ = options.mode(0o600);
    }
    options.open(path).await
}

struct PathLease {
    leased_path: PathBuf,
}

impl PathLease {
    async fn acquire(path: &Path) -> io::Result<Self> {
        let path = path.to_owned();
        let leased_path = tokio::task::spawn_blocking(move || resolve_for_lease(&path))
            .await
            .map_err(|error| io::Error::other(format!("path resolution task failed: {error}")))??;
        let leases = PATH_LEASES.get_or_init(|| Mutex::new(HashSet::new()));
        let mut leases = leases
            .lock()
            .map_err(|_| io::Error::other("file path lease registry is poisoned"))?;
        if !leases.insert(leased_path.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "resolved file path is already leased by another writer",
            ));
        }
        Ok(Self { leased_path })
    }
}

fn resolve_for_lease(path: &Path) -> io::Result<PathBuf> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(canonical);
    }
    let mut ancestor = path;
    let mut suffix = Vec::new();
    let canonical_ancestor = loop {
        if let Ok(canonical) = std::fs::canonicalize(ancestor) {
            break canonical;
        }
        let Some(name) = ancestor.file_name() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file path has no canonicalizable ancestor",
            ));
        };
        suffix.push(name.to_owned());
        ancestor = ancestor.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "file path has no canonicalizable ancestor",
            )
        })?;
    };
    Ok(suffix
        .into_iter()
        .rev()
        .fold(canonical_ancestor, |path, component| path.join(component)))
}

impl Drop for PathLease {
    fn drop(&mut self) {
        if let Some(leases) = PATH_LEASES.get()
            && let Ok(mut leases) = leases.lock()
        {
            let _ = leases.remove(&self.leased_path);
        }
    }
}

async fn create_parent_directories(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file destination has no parent directory",
        ));
    };
    let parent = parent.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut builder = std::fs::DirBuilder::new();
        _ = builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            _ = builder.mode(0o700);
        }
        builder.create(parent)
    })
    .await
    .map_err(|error| io::Error::other(format!("directory creation task failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn config(path: &Path, extra: serde_json::Value) -> FileExporterConfig {
        let mut value = json!({"path": path.to_string_lossy()});
        value.as_object_mut().unwrap().extend(
            extra
                .as_object()
                .expect("test config extras must be an object")
                .clone(),
        );
        FileExporterConfig::parse(&value).unwrap()
    }

    fn template_path(root: &Path) -> PathBuf {
        root.join("data-{signal}-{core_id}-{generation}.jsonl")
    }

    /// Scenario: Append mode opens a file ending in a complete JSON line and writes another frame.
    /// Guarantees: Existing complete frames are retained before the first exporter frame is written.
    #[tokio::test]
    async fn append_preserves_existing_frames() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        tokio::fs::write(&path, b"{\"old\":true}\n").await.unwrap();
        let config = config(&template_path(dir.path()), json!({}));
        let (mut writer, recovery) = SignalWriter::open(&path, &config).await.unwrap();
        assert_eq!(recovery.recovered_bytes, 0);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"{\"old\":true}\n");
        _ = writer.write_frame(b"{\"new\":true}\n").await.unwrap();
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"{\"old\":true}\n{\"new\":true}\n"
        );
    }

    /// Scenario: Append mode opens a file with one complete frame and an incomplete crash tail.
    /// Guarantees: Only the incomplete final bytes are removed before the next frame is written.
    #[tokio::test]
    async fn append_truncates_one_bounded_partial_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        tokio::fs::write(&path, b"{\"ok\":true}\n{\"partial\"")
            .await
            .unwrap();
        let config = config(&template_path(dir.path()), json!({}));
        let (_writer, recovery) = SignalWriter::open(&path, &config).await.unwrap();
        assert_eq!(recovery.recovered_bytes, 10);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"{\"ok\":true}\n");
    }

    /// Scenario: Append mode is configured to reject a destination with a partial final frame.
    /// Guarantees: Tail recovery failure leaves every existing byte unchanged.
    #[tokio::test]
    async fn append_fail_policy_rejects_and_preserves_partial_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        let original = b"{\"ok\":true}\n{\"partial\"";
        tokio::fs::write(&path, original).await.unwrap();
        let config = config(&template_path(dir.path()), json!({"tail_recovery": "fail"}));
        assert!(SignalWriter::open(&path, &config).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    }

    /// Scenario: The nearest complete boundary lies outside the configured tail scan bound.
    /// Guarantees: Recovery refuses to guess a truncation point and preserves the destination.
    #[tokio::test]
    async fn append_recovery_rejects_a_tail_larger_than_the_frame_bound() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        let original = b"{}\npartial";
        tokio::fs::write(&path, original).await.unwrap();
        let config = config(&template_path(dir.path()), json!({"max_frame_bytes": 4}));
        assert!(SignalWriter::open(&path, &config).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    }

    /// Scenario: A protobuf file contains one complete frame and one crash-truncated frame.
    /// Guarantees: Append recovery retains the complete length-prefixed frame and removes the tail.
    #[tokio::test]
    async fn append_recovers_incomplete_protobuf_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.bin");
        let original = [0, 0, 0, 2, b'o', b'k', 0, 0, 0, 3, b'x'];
        tokio::fs::write(&path, original).await.unwrap();
        let config = config(
            &dir.path().join("data-{signal}-{core_id}-{generation}.bin"),
            json!({"format": "otlp_proto"}),
        );
        let (_writer, recovery) = SignalWriter::open(&path, &config).await.unwrap();
        assert_eq!(recovery.recovered_bytes, 5);
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            [0, 0, 0, 2, b'o', b'k']
        );
    }

    /// Scenario: A protobuf append target contains only an incomplete first frame.
    /// Guarantees: Recovery refuses to erase a destination without one proven frame boundary.
    #[tokio::test]
    async fn append_rejects_incomplete_first_protobuf_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.bin");
        let original = [0, 0, 0, 3, b'x'];
        tokio::fs::write(&path, original).await.unwrap();
        let config = config(
            &dir.path().join("data-{signal}-{core_id}-{generation}.bin"),
            json!({"format": "proto"}),
        );
        assert!(SignalWriter::open(&path, &config).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    }

    /// Scenario: Truncate and create-new modes open existing and absent destinations.
    /// Guarantees: Truncate discards old bytes, while create-new rejects a second ownership file.
    #[tokio::test]
    async fn destructive_open_modes_have_explicit_first_open_behavior() {
        let dir = tempdir().unwrap();
        let truncate_path = dir.path().join("truncate-logs-0-1.jsonl");
        tokio::fs::write(&truncate_path, b"old\n").await.unwrap();
        let truncate_config = config(
            &dir.path()
                .join("truncate-{signal}-{core_id}-{generation}.jsonl"),
            json!({"open_mode": "truncate"}),
        );
        let (mut truncate_writer, _) = SignalWriter::open(&truncate_path, &truncate_config)
            .await
            .unwrap();
        assert!(tokio::fs::read(&truncate_path).await.unwrap().is_empty());
        _ = truncate_writer.write_frame(b"{}\n").await.unwrap();
        assert_eq!(tokio::fs::read(&truncate_path).await.unwrap(), b"{}\n");
        drop(truncate_writer);

        let create_path = dir.path().join("new-logs-0-1.jsonl");
        let create_config = config(
            &dir.path().join("new-{signal}-{core_id}-{generation}.jsonl"),
            json!({"open_mode": "create_new"}),
        );
        let (create_writer, _) = SignalWriter::open(&create_path, &create_config)
            .await
            .unwrap();
        drop(create_writer);
        assert!(
            SignalWriter::open(&create_path, &create_config)
                .await
                .is_err()
        );
    }

    /// Scenario: Two live writer instances attempt to own the same normalized signal path.
    /// Guarantees: The second open fails until the first writer releases its process-local lease.
    #[tokio::test]
    async fn live_writers_cannot_share_a_resolved_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        let config = config(&template_path(dir.path()), json!({}));
        let (writer, _) = SignalWriter::open(&path, &config).await.unwrap();
        assert!(SignalWriter::open(&path, &config).await.is_err());
        drop(writer);
        assert!(SignalWriter::open(&path, &config).await.is_ok());
    }

    #[cfg(unix)]
    /// Scenario: Two configured paths reach one file through real and symlinked parent directories.
    /// Guarantees: Canonical process-local leasing rejects filesystem aliases while either writer lives.
    #[tokio::test]
    async fn live_writers_cannot_bypass_leases_with_a_symlinked_parent() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        let alias = dir.path().join("alias");
        std::fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let target_path = target.join("data-logs-0-1.jsonl");
        let alias_path = alias.join("data-logs-0-1.jsonl");
        let config = config(&template_path(&target), json!({}));
        let (writer, _) = SignalWriter::open(&target_path, &config).await.unwrap();
        assert!(SignalWriter::open(&alias_path, &config).await.is_err());
        drop(writer);
        assert!(SignalWriter::open(&alias_path, &config).await.is_ok());
    }

    #[cfg(unix)]
    /// Scenario: A symlink followed by `..` aliases the destination owned by another live writer.
    /// Guarantees: Lease resolution follows filesystem traversal order and rejects the alias.
    #[tokio::test]
    async fn live_writers_cannot_bypass_leases_with_symlinked_parent_traversal() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        let child = target.join("child");
        let alias = dir.path().join("alias");
        std::fs::create_dir_all(&child).unwrap();
        std::os::unix::fs::symlink(&child, &alias).unwrap();
        let target_path = target.join("data-logs-0-1.jsonl");
        let alias_path = alias.join("../data-logs-0-1.jsonl");
        let config = config(&template_path(&target), json!({}));
        let (writer, _) = SignalWriter::open(&target_path, &config).await.unwrap();
        assert!(SignalWriter::open(&alias_path, &config).await.is_err());
        drop(writer);
        assert!(SignalWriter::open(&alias_path, &config).await.is_ok());
    }

    /// Scenario: Directory creation is enabled for a destination with missing nested parents.
    /// Guarantees: The writer creates parents lazily and writes a frame to the requested file.
    #[tokio::test]
    async fn creates_missing_parent_directories_when_enabled() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("one/two");
        let path = root.join("data-logs-0-1.jsonl");
        let config = config(&template_path(&root), json!({"create_directories": true}));
        let (mut writer, _) = SignalWriter::open(&path, &config).await.unwrap();
        _ = writer.write_frame(b"{}\n").await.unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"{}\n");
    }

    /// Scenario: The next complete frame would cross a configured active-file byte limit.
    /// Guarantees: Rotation occurs before that frame and never splits either JSON Lines frame.
    #[tokio::test]
    async fn size_rotation_preserves_complete_frame_boundaries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        let config = config(
            &template_path(dir.path()),
            json!({
                "max_frame_bytes": 8,
                "rotation": {
                    "max_bytes": 8,
                    "retention": {"max_backups": 2}
                }
            }),
        );
        let (mut writer, _) = SignalWriter::open(&path, &config).await.unwrap();
        assert!(!writer.write_frame(b"{}\n").await.unwrap().rotated);
        assert!(!writer.write_frame(b"{}\n").await.unwrap().rotated);
        assert!(writer.write_frame(b"{}\n").await.unwrap().rotated);

        let finalized = super::super::rotation::segment_path(&path, 0);
        assert_eq!(tokio::fs::read(finalized).await.unwrap(), b"{}\n{}\n");
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"{}\n");
    }

    /// Scenario: A non-empty active file reaches its elapsed-time rotation deadline while idle.
    /// Guarantees: The writer can finalize it without requiring another telemetry frame.
    #[tokio::test]
    async fn time_rotation_finalizes_an_idle_active_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data-logs-0-1.jsonl");
        let config = config(
            &template_path(dir.path()),
            json!({
                "max_frame_bytes": 8,
                "rotation": {
                    "max_duration": "1ms",
                    "retention": {"max_backups": 2}
                }
            }),
        );
        let (mut writer, _) = SignalWriter::open(&path, &config).await.unwrap();
        _ = writer.write_frame(b"{}\n").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(writer.maintain_if_due().await.unwrap().rotated);

        let finalized = super::super::rotation::segment_path(&path, 0);
        assert_eq!(tokio::fs::read(finalized).await.unwrap(), b"{}\n");
        assert!(tokio::fs::read(path).await.unwrap().is_empty());
    }

    #[cfg(unix)]
    /// Scenario: Rotation is enabled for an active path that is itself a symbolic link.
    /// Guarantees: Opening fails before rename semantics could rotate the link instead of its file.
    #[tokio::test]
    async fn rotation_rejects_a_symlink_as_the_active_file() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target.jsonl");
        let path = dir.path().join("data-logs-0-1.jsonl");
        tokio::fs::write(&target, b"{}\n").await.unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let config = config(
            &template_path(dir.path()),
            json!({
                "max_frame_bytes": 8,
                "rotation": {"max_bytes": 8}
            }),
        );

        assert!(SignalWriter::open(&path, &config).await.is_err());
        assert_eq!(tokio::fs::read(target).await.unwrap(), b"{}\n");
    }

    /// Scenario: Size rotation is paired with either supported background compression codec.
    /// Guarantees: Finalized files become standard streams, sources are removed after commit, and
    /// the active file remains uncompressed and writable.
    #[tokio::test]
    async fn rotated_files_are_compressed_with_standard_codecs() {
        use std::io::Read;

        for (name, codec) in [
            ("gzip", FileCompression::Gzip),
            ("zstd", FileCompression::Zstd),
        ] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("data-logs-0-1.jsonl");
            let config = config(
                &template_path(dir.path()),
                json!({
                    "max_frame_bytes": 8,
                    "compression": name,
                    "rotation": {
                        "max_bytes": 8,
                        "retention": {"max_backups": 2}
                    }
                }),
            );
            let (mut writer, _) = SignalWriter::open(&path, &config).await.unwrap();
            _ = writer.write_frame(b"{}\n").await.unwrap();
            _ = writer.write_frame(b"{}\n").await.unwrap();
            assert!(writer.write_frame(b"{}\n").await.unwrap().rotated);
            assert!(writer.compression_worker.is_some());
            _ = writer.finalize().await.unwrap();

            let source = super::super::rotation::segment_path(&path, 0);
            let compressed = super::super::rotation::compressed_segment_path(&path, 0, codec);
            let decoded = match codec {
                FileCompression::Gzip => {
                    let mut decoder =
                        flate2::read::GzDecoder::new(std::fs::File::open(compressed).unwrap());
                    let mut decoded = Vec::new();
                    _ = decoder.read_to_end(&mut decoded).unwrap();
                    decoded
                }
                FileCompression::Zstd => {
                    zstd::stream::decode_all(std::fs::File::open(compressed).unwrap()).unwrap()
                }
            };
            assert_eq!(decoded, b"{}\n{}\n");
            assert!(!source.exists());
            assert_eq!(tokio::fs::read(path).await.unwrap(), b"{}\n");
        }
    }
}
