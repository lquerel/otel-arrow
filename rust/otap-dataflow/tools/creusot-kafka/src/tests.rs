// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
use crate::*;

/// Scenario: An out-of-order ack precedes replay of the still-pending lower offset.
/// Guarantees: Rewind preserves the lower commit barrier and caps obsolete acknowledgement evidence below it.
#[test]
fn rewind_caps_later_acknowledgement() {
    let mut cursor = PartitionCursor::new(1);
    cursor.tracked(100);
    cursor.tracked(101);
    assert!(!cursor.acknowledged(101, Some(100)));
    cursor.rewound(100);
    assert_eq!(cursor.last_lowest, Some(100));
    assert_eq!(cursor.high_water_mark, Some(99));
}

/// Scenario: The same partition receives deliveries under older, equal, and newer ownership periods.
/// Guarantees: Stale and equal generations preserve the cursor; a newer generation discards its old cursor evidence.
#[test]
fn tracking_generation_boundaries() {
    let mut cursor = PartitionCursor::new(7);
    cursor.tracked(5);
    let _ = cursor.acknowledged(9, Some(5));
    let before = cursor;
    assert_eq!(cursor.prepare_track(6), GenerationChange::Stale);
    assert_eq!(cursor, before);
    assert_eq!(cursor.prepare_track(7), GenerationChange::Same);
    assert_eq!(cursor, before);
    assert_eq!(cursor.prepare_track(u64::MAX), GenerationChange::New);
    assert_eq!(cursor, PartitionCursor::new(u64::MAX));
}

/// Scenario: Rewind reaches the smallest signed offset and retry attempts reach their largest value.
/// Guarantees: Rewind clears unrepresentable predecessor evidence and the implemented counters saturate without arithmetic panic.
#[test]
fn numeric_boundaries_are_explicit() {
    let mut cursor = PartitionCursor {
        generation: 1,
        last_lowest: None,
        high_water_mark: Some(i64::MAX),
    };
    cursor.rewound(i64::MIN);
    assert_eq!(cursor.high_water_mark, None);
    assert_eq!(next_attempt(u32::MAX), u32::MAX);
    assert_eq!(next_delivery_generation(u64::MAX), u64::MAX);
}

/// Scenario: Current and obsolete wakeups attempt to promote backoff and replay phases.
/// Guarantees: Only the matching token and deadline may become due, and stale feedback cannot start or complete replay.
#[test]
fn replay_phase_requires_matching_identity() {
    let mut phase = RetryPhase::Backoff {
        deadline: 10u64,
        paused: true,
    };
    assert_eq!(phase.mark_due(2, 1, 10), None);
    assert_eq!(phase.mark_due(2, 2, 9), None);
    assert_eq!(phase.mark_due(2, 2, 10), Some(true));
    assert!(!phase.mark_replaying(2, 1));
    assert!(phase.mark_replaying(2, 2));
    assert!(!phase.completes(2, 1, 100, 100));
    assert!(!phase.completes(2, 2, 100, 101));
    assert!(phase.completes(2, 2, 100, 100));
}

/// Scenario: Feedback races ownership reassignment before new deliveries have been tracked.
/// Guarantees: The newer assigned generation fences old feedback even while the tracker still holds an older generation.
#[test]
fn assignment_fences_feedback_before_retracking() {
    assert_eq!(
        classify_offset_feedback(3, Some(3), 4, true),
        OffsetFeedbackAction::DropStale
    );
    assert_eq!(
        classify_offset_feedback(4, Some(3), 4, true),
        OffsetFeedbackAction::Commit
    );
    assert_eq!(
        classify_offset_feedback(4, Some(5), 0, false),
        OffsetFeedbackAction::DropStale
    );
    assert_eq!(
        classify_offset_feedback(4, Some(3), 0, false),
        OffsetFeedbackAction::DropLateAck { purge: true }
    );
}
