// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reusable behavioral checks for registered pdata codecs.

use bytes::Bytes;
use otel_arrow_dfe_config::SignalType;

use crate::batching::{BatchPlan, PdataFormat};
use crate::codec::{CodecState, EncodingPlan, ResolvedCodec};
use crate::{OtapPayload, OtapPayloadHelpers};

/// Inputs and expected results for one codec's decode conformance checks.
pub struct DecodeConformanceCase {
    /// Resolved codec under test.
    pub codec: ResolvedCodec,
    /// Signal carried by both byte samples.
    pub signal: SignalType,
    /// Complete independently decodable batch.
    pub valid: Bytes,
    /// Malformed batch that the codec must reject, when strict validation is
    /// part of the codec's compatibility contract.
    pub malformed: Option<Bytes>,
    /// Number of signal items in the valid batch.
    pub expected_items: usize,
}

/// Checks lazy admission, signal preservation, views, counting, recoverable
/// failures, repeated failure safety, and matching-format forwarding.
pub fn assert_decode_conformance(case: DecodeConformanceCase) {
    let encoded = case
        .codec
        .admit(case.signal, case.valid.clone())
        .expect("codec must admit its supported signal");
    assert_eq!(encoded.signal_type(), case.signal);
    assert_eq!(encoded.bytes().as_ptr(), case.valid.as_ptr());

    let mut payload = OtapPayload::from(encoded.clone());
    assert_eq!(payload.num_items(), case.expected_items);

    let mut state = CodecState::default();
    let view = payload
        .view(&mut state)
        .expect("valid codec bytes must produce a view");
    assert_eq!(view.signal_type(), case.signal);

    let records = OtapPayload::from(encoded)
        .try_into_otap(&mut state)
        .expect("valid codec bytes must decode");
    assert_eq!(records.signal_type(), case.signal);
    assert_eq!(records.num_items(), case.expected_items);

    if let Some(malformed_bytes) = case.malformed {
        let mut malformed = OtapPayload::from(
            case.codec
                .admit(case.signal, malformed_bytes.clone())
                .expect("admission must remain lazy"),
        );
        for _ in 0..2 {
            let error = malformed
                .try_into_otap(&mut state)
                .expect_err("malformed bytes must fail decoding");
            let (_, recovered) = error.into_parts();
            assert_eq!(recovered.signal_type(), case.signal);
            assert_eq!(recovered.encoded_bytes(), Some(&malformed_bytes));
            assert_eq!(
                recovered
                    .encoded_bytes()
                    .expect("encoded recovery")
                    .as_ptr(),
                malformed_bytes.as_ptr()
            );
            malformed = recovered;
        }
    }

    if case.codec.metadata().batching.is_some() {
        let format = PdataFormat::encoded(case.codec);
        let plan = BatchPlan::new(format, format.default_profile(), true)
            .expect("registered default batching profile must resolve");
        let mut batching_state = CodecState::default();
        let mut inputs = Vec::with_capacity(2);
        for _ in 0..2 {
            let mut input = OtapPayload::from(
                case.codec
                    .admit(case.signal, case.valid.clone())
                    .expect("codec must re-admit an independent batch"),
            );
            plan.prepare(&mut input, &mut batching_state)
                .expect("default batching profile must prepare valid input");
            inputs.push(input);
        }
        let expected_weight = inputs
            .iter()
            .map(|input| {
                plan.profile()
                    .sizer
                    .batch_size(input)
                    .expect("prepared size")
            })
            .sum::<usize>();
        let output = plan
            .batch(case.signal, inputs, &mut batching_state)
            .expect("default batcher must accept independent valid batches");
        assert_eq!(
            output
                .batches
                .iter()
                .map(|(_, weight)| *weight)
                .sum::<usize>(),
            expected_weight
        );

        let mut decoded_items = 0;
        for (output, _) in output.batches {
            let records = output
                .try_into_otap(&mut CodecState::default())
                .expect("each batch output must decode independently");
            assert_eq!(records.signal_type(), case.signal);
            decoded_items += records.num_items();
        }
        assert_eq!(decoded_items, case.expected_items * 2);
    }

    if case.codec.metadata().can_encode {
        let plan = EncodingPlan::new(case.codec, Default::default())
            .expect("conforming output codec must resolve a plan");
        let mut forwarded = OtapPayload::from(
            case.codec
                .admit(case.signal, case.valid.clone())
                .expect("codec must re-admit valid bytes"),
        );
        let mut forwarding_state = CodecState::default();
        let output = forwarded
            .prepare_encoded(&mut forwarding_state, &plan)
            .expect("matching representation must forward");
        assert_eq!(output.as_ref().as_ptr(), case.valid.as_ptr());
        assert_eq!(output.as_ref(), case.valid.as_ref());
    }
}
