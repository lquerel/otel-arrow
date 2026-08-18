// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded background compression for finalized file-exporter segments.
//!
//! A worker streams one source file through a standard gzip or zstd encoder on Tokio's blocking
//! pool. It synchronizes and renames a temporary output but deliberately retains the source; the
//! owning rotation manifest commits the compressed representation before removing that fallback.

use super::config::FileCompression;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

/// One manifest-authorized segment compression operation.
#[derive(Debug, Clone)]
pub struct CompressionRequest {
    /// Finalized segment sequence number.
    pub sequence: u64,
    /// Standard file-level codec to produce.
    pub codec: FileCompression,
    /// Retained uncompressed source path.
    pub source: PathBuf,
    /// Final compressed destination path.
    pub destination: PathBuf,
    /// Same-directory temporary output path.
    pub temporary: PathBuf,
}

/// At most one in-flight blocking compression job for a signal writer.
pub struct CompressionWorker {
    request: CompressionRequest,
    handle: JoinHandle<io::Result<()>>,
}

impl CompressionWorker {
    /// Starts streaming one finalized segment on the blocking pool.
    #[must_use]
    pub fn start(request: CompressionRequest) -> Self {
        let job = request.clone();
        let handle = tokio::task::spawn_blocking(move || compress(&job));
        Self { request, handle }
    }

    /// Returns whether the blocking job can be joined without waiting.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Joins the worker and returns the request whose output is ready to commit.
    pub async fn finish(self) -> io::Result<CompressionRequest> {
        self.handle
            .await
            .map_err(|error| io::Error::other(format!("compression task failed: {error}")))??;
        Ok(self.request)
    }
}

fn compress(request: &CompressionRequest) -> io::Result<()> {
    if request.destination.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "compressed segment destination already exists",
        ));
    }
    let source = File::open(&request.source)?;
    let output = create_output(&request.temporary)?;
    let mut input = BufReader::new(source);
    let output = match request.codec {
        FileCompression::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(output, flate2::Compression::default());
            _ = io::copy(&mut input, &mut encoder)?;
            encoder.finish()?
        }
        FileCompression::Zstd => {
            let mut encoder = zstd::stream::write::Encoder::new(output, 0)?;
            _ = io::copy(&mut input, &mut encoder)?;
            encoder.finish()?
        }
    };
    output.sync_all()?;
    drop(output);
    std::fs::rename(&request.temporary, &request.destination)
}

fn create_output(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    _ = options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        _ = options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::tempdir;

    /// Scenario: The worker compresses identical input with each configured file-level codec.
    /// Guarantees: Gzip and zstd outputs are standard streams that decode to the exact source.
    #[tokio::test]
    async fn produces_standard_gzip_and_zstd_streams() {
        let content = b"one complete OTLP frame\n".repeat(100);
        for codec in [FileCompression::Gzip, FileCompression::Zstd] {
            let dir = tempdir().unwrap();
            let source = dir.path().join("segment");
            let destination = dir.path().join(format!("segment{}", codec.suffix()));
            let temporary = dir.path().join("segment.tmp");
            std::fs::write(&source, &content).unwrap();
            let request = CompressionRequest {
                sequence: 0,
                codec,
                source: source.clone(),
                destination: destination.clone(),
                temporary,
            };
            _ = CompressionWorker::start(request).finish().await.unwrap();

            let decoded = match codec {
                FileCompression::Gzip => {
                    let mut decoder =
                        flate2::read::GzDecoder::new(File::open(destination).unwrap());
                    let mut decoded = Vec::new();
                    _ = decoder.read_to_end(&mut decoded).unwrap();
                    decoded
                }
                FileCompression::Zstd => {
                    zstd::stream::decode_all(File::open(destination).unwrap()).unwrap()
                }
            };
            assert_eq!(decoded, content);
            assert_eq!(std::fs::read(source).unwrap(), content);
        }
    }
}
