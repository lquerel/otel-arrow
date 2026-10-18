// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Durable value types shared by production and checkpoint transition proofs.

#[cfg(creusot)]
use creusot_std::prelude::*;
#[cfg(creusot)]
use creusot_std::prelude::{Clone, PartialEq};

/// Raw-byte window covered by committed-frontier evidence.
pub const COMMITTED_FRONTIER_GUARD_WINDOW_BYTES: u16 = 64;

pub(crate) const EMPTY_FRONTIER_GUARD_DIGEST: [u8; 32] = [
    0xbe, 0x47, 0xd0, 0x23, 0xa0, 0x6e, 0x82, 0xfd, 0x6d, 0xa2, 0xda, 0xa0, 0x63, 0x15, 0x47, 0xd6,
    0xec, 0xa2, 0x97, 0xb7, 0xac, 0x53, 0x2c, 0xba, 0x64, 0x71, 0xab, 0x90, 0x82, 0x9e, 0xc5, 0xb9,
];

/// Opaque durable 128-bit file identity.
#[cfg_attr(
    not(creusot),
    derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)
)]
pub struct FileId(pub(crate) [u8; 16]);

/// Durable lifecycle state.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum LifecycleState {
    /// File is actively tracked.
    Active,
    /// Rotation finalization completed.
    RotatedFinalized,
    /// Reading is blocked pending administration.
    Quarantined,
}

/// Normalized platform-neutral file locator.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq, Hash))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum Locator {
    /// No locator is available. Version 1 snapshot and registration state
    /// rejects this value, but the discriminant remains part of the codec.
    Unspecified,
    /// POSIX device and inode values widened to `u64`.
    PosixDevIno {
        /// Device identifier.
        dev: u64,
        /// Inode number.
        ino: u64,
    },
    /// Windows volume serial and opaque 128-bit file ID.
    WindowsVolumeFileId {
        /// Full volume serial value.
        volume_serial: u64,
        /// Opaque `FILE_ID_INFO.FileId` bytes.
        file_id: [u8; 16],
    },
}

/// Durable framing resume state.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum FramingResume {
    /// The next unit starts a new record.
    Clean,
    /// A split record remains in progress.
    Continuation {
        /// Original record start offset.
        record_start_offset: u64,
        /// Known record end, or zero for scan-to-LF mode.
        record_end_offset: u64,
        /// Index of the next fragment.
        next_fragment_index: u32,
    },
}

/// Advisory path encoding kind.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AdvisoryPathKind {
    /// No path is available.
    Unavailable,
    /// Native Unix bytes.
    UnixBytes,
    /// Native UTF-16 code units serialized little-endian.
    WindowsUtf16Le,
}

/// Bounded filelog advisory-path evidence.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct AdvisoryPath {
    pub(crate) kind: AdvisoryPathKind,
    pub(crate) truncated: bool,
    pub(crate) full_path_len: u64,
    pub(crate) stored_path_bytes: Vec<u8>,
    pub(crate) full_path_digest: [u8; 32],
}

/// Fixed-width digest evidence immediately preceding committed progress.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub struct CommittedFrontierGuard {
    /// Required raw window length.
    pub window_len: u16,
    /// Domain-separated SHA-256 digest.
    pub digest: [u8; 32],
}

/// Immutable evidence attached to a quarantined record.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct QuarantineEvidence {
    /// Opaque diagnostic reason code.
    pub reason_code: u16,
    /// Observed file size at quarantine time.
    pub observed_size: u64,
    /// File epoch at quarantine time.
    pub quarantine_epoch: u32,
    /// Quarantine time as Unix nanoseconds.
    pub quarantine_time_unix_nano: u64,
}

/// One durable version 1 checkpoint record.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct SnapshotRecord {
    /// Opaque durable record key.
    pub file_id: FileId,
    /// Current file epoch.
    pub file_epoch: u32,
    /// Acked source-byte frontier.
    pub committed_offset: u64,
    /// Evidence for bytes immediately before the frontier.
    pub committed_frontier_guard: CommittedFrontierGuard,
    /// Current bounded fingerprint evidence.
    pub fingerprint: Vec<u8>,
    /// Bytes excluded before fingerprinting.
    pub ignored_header_bytes: u32,
    /// Current or quarantined immutable locator.
    pub locator: Locator,
    /// Framing-profile structural version.
    pub framing_profile_version: u16,
    /// Framing-profile canonical digest.
    pub framing_profile_digest: [u8; 32],
    /// Durable record-framing resume state.
    pub framing_resume: FramingResume,
    /// Durable lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Evidence present exactly for quarantined records.
    pub quarantine_evidence: Option<QuarantineEvidence>,
    /// Last-observed time as Unix nanoseconds.
    pub last_seen_time_unix_nano: u64,
    /// Bounded advisory path evidence.
    pub advisory_path: AdvisoryPath,
}

/// Advances one file's committed progress.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct UpdateProgress {
    /// Target file key.
    pub file_id: FileId,
    /// Expected stored offset.
    pub expected_committed_offset: u64,
    /// Expected stored epoch.
    pub expected_file_epoch: u32,
    /// New committed offset.
    pub new_committed_offset: u64,
    /// New frontier evidence.
    pub new_committed_frontier_guard: CommittedFrontierGuard,
    /// New framing resume.
    pub new_framing_resume: FramingResume,
    /// New last-observed time as Unix nanoseconds.
    pub new_last_seen_time_unix_nano: u64,
    /// Whether to finalize the rotated file atomically.
    pub finalize: bool,
}

/// Resets a file after truncation.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct ResetAfterTruncate {
    /// Target file key.
    pub file_id: FileId,
    /// Expected active epoch.
    pub expected_active_epoch: u32,
    /// Observed truncated size.
    pub observed_truncated_size: u64,
    /// Resulting epoch.
    pub resulting_epoch: u32,
    /// Resulting committed offset.
    pub new_committed_offset: u64,
    /// Resulting framing resume.
    pub new_framing_resume: FramingResume,
    /// Replacement fingerprint.
    pub new_fingerprint: Vec<u8>,
    /// Reset time as Unix nanoseconds.
    pub reset_time_unix_nano: u64,
    /// Opaque reset reason.
    pub reason_code: u16,
}

/// Administrative reset decision for a quarantined file.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum ResetQuarantineAction {
    /// Resume at offset zero with a new epoch.
    ResetToBeginning,
    /// Resume at an externally selected end offset with a new epoch.
    ResetToEnd,
    /// Keep the immutable quarantined record unchanged.
    KeepFailed,
}

/// Audited operation over one quarantined file.
#[cfg_attr(not(creusot), derive(Debug, Clone, PartialEq, Eq))]
pub struct ResetQuarantinedFile {
    /// Target file key.
    pub file_id: FileId,
    /// Expected quarantine epoch.
    pub expected_quarantine_epoch: u32,
    /// Requested action.
    pub action: ResetQuarantineAction,
    /// Resulting epoch supplied by the caller.
    pub resulting_epoch: u32,
    /// Resulting offset supplied by the caller.
    pub resulting_offset: u64,
    /// Resulting frontier evidence supplied by the caller.
    pub new_committed_frontier_guard: CommittedFrontierGuard,
    /// Resulting framing resume supplied by the caller.
    pub new_framing_resume: FramingResume,
    /// Resulting fingerprint supplied by the caller.
    pub new_fingerprint: Vec<u8>,
    /// Action time as Unix nanoseconds.
    pub action_time_unix_nano: u64,
    /// Exact checkpoint namespace ID.
    pub namespace_id: String,
    /// Mandatory operator audit reason.
    pub audit_reason: String,
}

impl FramingResume {
    #[cfg_attr(creusot, ensures(result == self.well_formed(offset)))]
    pub(crate) const fn valid_for_offset(self, offset: u64) -> bool {
        match self {
            Self::Clean => true,
            Self::Continuation {
                record_start_offset,
                record_end_offset,
                next_fragment_index,
            } => {
                next_fragment_index >= 1
                    && record_start_offset < offset
                    && (record_end_offset == 0 || offset < record_end_offset)
            }
        }
    }
}

impl CommittedFrontierGuard {
    #[cfg_attr(creusot, ensures(result == self.well_formed(offset)))]
    pub(crate) fn valid_for_offset(&self, offset: u64) -> bool {
        let expected_len = offset.min(u64::from(COMMITTED_FRONTIER_GUARD_WINDOW_BYTES));
        self.window_len as u64 == expected_len
            && (offset != 0 || self.digest == EMPTY_FRONTIER_GUARD_DIGEST)
    }
}

impl CommittedFrontierGuard {
    /// Computes the canonical empty guard at offset zero.
    #[must_use]
    #[cfg_attr(creusot, ensures(result.window_len@ == 0 && result.digest.deep_model() == EMPTY_FRONTIER_GUARD_DIGEST.deep_model()))]
    pub const fn empty() -> Self {
        Self {
            window_len: 0,
            digest: EMPTY_FRONTIER_GUARD_DIGEST,
        }
    }
}

#[cfg(creusot)]
impl FramingResume {
    #[logic(open(crate))]
    pub(crate) fn well_formed(self, offset: u64) -> bool {
        pearlite! { match self {
            Self::Clean => true,
            Self::Continuation { record_start_offset, record_end_offset, next_fragment_index } =>
                next_fragment_index@ > 0 && record_start_offset@ < offset@ &&
                (record_end_offset@ == 0 || offset@ < record_end_offset@),
        } }
    }
}

#[cfg(creusot)]
impl CommittedFrontierGuard {
    #[logic(open(crate))]
    pub(crate) fn well_formed(self, offset: u64) -> bool {
        pearlite! { self.window_len@ == (if offset@ < 64 { offset@ } else { 64 }) &&
        (offset@ > 0 || self.digest.deep_model() == EMPTY_FRONTIER_GUARD_DIGEST.deep_model()) }
    }
}
