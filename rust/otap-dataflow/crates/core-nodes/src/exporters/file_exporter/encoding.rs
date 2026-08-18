// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded OTLP JSON Lines and length-prefixed protobuf framing for the file exporter.
//!
//! The shared pdata serializers write unframed OTLP JSON to any synchronous writer. This module
//! wraps those serializers and the shared OTAP-to-protobuf encoders with an exporter-owned byte
//! limit. JSON frames end in a newline. Protobuf frames use the Go Collector file exporter's
//! four-byte big-endian unsigned length prefix.

use otap_df_pdata::error::Error as PdataError;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::otlp::json::{
    JsonEncodeError, write_logs_json, write_metrics_json, write_traces_json,
};
use otap_df_pdata::otlp::{ProtoBuffer, ProtoBytesEncoder};
use otap_df_pdata_views::views::logs::LogsDataView;
use otap_df_pdata_views::views::metrics::MetricsView;
use otap_df_pdata_views::views::trace::TracesView;
use std::io::{self, Write};

/// Bytes in the protobuf frame length prefix.
const PROTO_LENGTH_PREFIX_BYTES: usize = size_of::<u32>();

/// Failure while encoding one bounded file frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameEncodeError {
    /// The encoded document and its framing exceed the configured frame limit.
    #[error("OTLP file frame exceeds the configured {max_frame_bytes} byte limit")]
    FrameTooLarge {
        /// Maximum allowed frame size, including its delimiter or length prefix.
        max_frame_bytes: usize,
    },
    /// The pdata view could not be serialized as OTLP JSON.
    #[error(transparent)]
    Json(#[from] JsonEncodeError),
    /// OTAP records could not be serialized as OTLP protobuf.
    #[error("OTLP protobuf encoding failed: {0}")]
    Proto(String),
}

struct BoundedWriter<'a> {
    output: &'a mut Vec<u8>,
    max_document_bytes: usize,
    limit_exceeded: bool,
}

impl Write for BoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.output.len().saturating_add(bytes.len()) > self.max_document_bytes {
            self.limit_exceeded = true;
            return Err(io::Error::other("OTLP JSON frame limit exceeded"));
        }
        self.output.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_frame(
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
    encode: impl FnOnce(&mut BoundedWriter<'_>) -> Result<(), JsonEncodeError>,
) -> Result<(), FrameEncodeError> {
    output.clear();
    let mut writer = BoundedWriter {
        output,
        max_document_bytes: max_frame_bytes.saturating_sub(1),
        limit_exceeded: false,
    };
    let result = encode(&mut writer);
    if writer.limit_exceeded {
        writer.output.clear();
        return Err(FrameEncodeError::FrameTooLarge { max_frame_bytes });
    }
    if let Err(error) = result {
        writer.output.clear();
        return Err(FrameEncodeError::Json(error));
    }
    writer.output.push(b'\n');
    Ok(())
}

/// Encodes one logs view as a bounded OTLP JSON Lines frame.
pub fn encode_logs<L: LogsDataView>(
    logs: &L,
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), FrameEncodeError> {
    encode_frame(output, max_frame_bytes, |writer| {
        write_logs_json(logs, writer)
    })
}

/// Encodes one metrics view as a bounded OTLP JSON Lines frame.
pub fn encode_metrics<M: MetricsView>(
    metrics: &M,
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), FrameEncodeError> {
    encode_frame(output, max_frame_bytes, |writer| {
        write_metrics_json(metrics, writer)
    })
}

/// Encodes one traces view as a bounded OTLP JSON Lines frame.
pub fn encode_traces<T: TracesView>(
    traces: &T,
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), FrameEncodeError> {
    encode_frame(output, max_frame_bytes, |writer| {
        write_traces_json(traces, writer)
    })
}

/// Frames already encoded OTLP protobuf bytes with a four-byte big-endian length prefix.
pub fn frame_proto_bytes(
    proto: &[u8],
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), FrameEncodeError> {
    output.clear();
    let frame_len = PROTO_LENGTH_PREFIX_BYTES.saturating_add(proto.len());
    let Ok(proto_len) = u32::try_from(proto.len()) else {
        return Err(FrameEncodeError::FrameTooLarge { max_frame_bytes });
    };
    if frame_len > max_frame_bytes {
        return Err(FrameEncodeError::FrameTooLarge { max_frame_bytes });
    }
    output.extend_from_slice(&proto_len.to_be_bytes());
    output.extend_from_slice(proto);
    Ok(())
}

/// Encodes OTAP Arrow records as one bounded length-prefixed OTLP protobuf frame.
pub fn encode_proto_records<E: ProtoBytesEncoder>(
    encoder: &mut E,
    records: &mut OtapArrowRecords,
    proto_buffer: &mut ProtoBuffer,
    output: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), FrameEncodeError> {
    output.clear();
    proto_buffer.clear();
    if let Err(error) = encoder.encode(records, proto_buffer) {
        proto_buffer.clear();
        return match error {
            PdataError::Dropped => Err(FrameEncodeError::FrameTooLarge { max_frame_bytes }),
            error => Err(FrameEncodeError::Proto(error.to_string())),
        };
    }
    let result = frame_proto_bytes(proto_buffer.as_ref(), output, max_frame_bytes);
    proto_buffer.clear();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::proto::opentelemetry::logs::v1::LogsData;

    /// Scenario: An encoded document and its newline exactly reach the configured frame limit.
    /// Guarantees: The frame bound includes the delimiter and permits an exact fit.
    #[test]
    fn exact_frame_limit_is_accepted() {
        let mut output = vec![b'x'];
        encode_logs(&LogsData::default(), &mut output, 3).unwrap();
        assert_eq!(output, b"{}\n");
    }

    /// Scenario: The JSON document fits but adding the required newline would exceed the limit.
    /// Guarantees: The encoder reports a typed limit error and removes every partial byte.
    #[test]
    fn frame_limit_counts_newline_and_clears_output() {
        let mut output = vec![b'x'];
        let error = encode_logs(&LogsData::default(), &mut output, 2).unwrap_err();
        assert!(matches!(error, FrameEncodeError::FrameTooLarge { .. }));
        assert!(output.is_empty());
    }

    /// Scenario: A protobuf payload and its four-byte prefix exactly reach the frame limit.
    /// Guarantees: Protobuf framing uses an unsigned big-endian length compatible with Go.
    #[test]
    fn protobuf_frame_uses_big_endian_length_prefix() {
        let mut output = vec![b'x'];
        frame_proto_bytes(b"abc", &mut output, 7).unwrap();
        assert_eq!(output, [0, 0, 0, 3, b'a', b'b', b'c']);
    }

    /// Scenario: A protobuf payload fits but its four-byte prefix exceeds the frame limit.
    /// Guarantees: The complete physical frame is bounded and partial output is discarded.
    #[test]
    fn protobuf_frame_limit_counts_length_prefix() {
        let mut output = vec![b'x'];
        let error = frame_proto_bytes(b"abc", &mut output, 6).unwrap_err();
        assert!(matches!(error, FrameEncodeError::FrameTooLarge { .. }));
        assert!(output.is_empty());
    }
}
