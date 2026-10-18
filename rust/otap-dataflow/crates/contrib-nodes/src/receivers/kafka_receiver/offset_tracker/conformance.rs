// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Independent Vec-based oracle for the concrete set, map, cache, and TPL adapter.
use super::*;

#[derive(Clone)]
struct ReferencePartition {
    partition: i32,
    generation: u64,
    pending: Vec<i64>,
    high: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Track(i32, i64, u64),
    Ack(i32, i64),
    Replay(i32, i64, u64),
    Revoke(i32, u64),
}

fn step(reference: &mut Vec<ReferencePartition>, tracker: &mut OffsetTracker, action: Action) {
    match action {
        Action::Track(partition, offset, generation) => {
            tracker.track("test", partition, offset, generation);
            let index = reference.iter().position(|p| p.partition == partition);
            let index = match index {
                Some(index) => index,
                None => {
                    reference.push(ReferencePartition {
                        partition,
                        generation,
                        pending: Vec::new(),
                        high: None,
                    });
                    reference.len() - 1
                }
            };
            let state = &mut reference[index];
            if generation >= state.generation {
                if generation > state.generation {
                    state.pending.clear();
                    state.high = None;
                    state.generation = generation;
                }
                if !state.pending.contains(&offset) {
                    state.pending.push(offset);
                }
            }
        }
        Action::Ack(partition, offset) => {
            let mut advanced = false;
            if let Some(state) = reference.iter_mut().find(|p| p.partition == partition)
                && let Some(index) = state.pending.iter().position(|p| *p == offset)
            {
                let old = state.pending.iter().min().copied();
                let _ = state.pending.remove(index);
                state.high = Some(state.high.map_or(offset, |old| old.max(offset)));
                advanced = old != state.pending.iter().min().copied();
            }
            assert_eq!(tracker.acknowledge("test", partition, offset), advanced);
        }
        Action::Replay(partition, failed, generation) => {
            let mut rewind = None;
            if let Some(state) = reference.iter_mut().find(|p| p.partition == partition)
                && state.generation == generation
                && state.pending.contains(&failed)
            {
                rewind = state.pending.iter().min().copied();
                let low = rewind.expect("pending failure");
                state.pending = vec![low];
                state.high = state.high.map(|high| high.min(low - 1));
            }
            assert_eq!(
                tracker.prepare_replay("test", partition, failed, generation),
                rewind
            );
        }
        Action::Revoke(partition, generation) => {
            let removed = reference
                .iter()
                .position(|p| p.partition == partition && p.generation <= generation);
            assert_eq!(
                tracker.revoke_if_older("test", partition, generation),
                removed.is_some()
            );
            if let Some(index) = removed {
                let _ = reference.remove(index);
            }
        }
    }
    let expected_count: usize = reference.iter().map(|p| p.pending.len()).sum();
    assert_eq!(tracker.total_pending(), expected_count);
    let actual_count: usize = tracker
        .partitions
        .values()
        .flat_map(HashMap::values)
        .map(|p| p.pending.len())
        .sum();
    assert_eq!(actual_count, expected_count);
    assert_eq!(
        tracker.partitions.values().map(HashMap::len).sum::<usize>(),
        reference.len()
    );
    assert_eq!(tracker.tpl.count(), reference.len());
    let mut expected = HashMap::new();
    for state in reference {
        let actual = &tracker.partitions["test"][&state.partition];
        assert_eq!(actual.cursor.generation, state.generation);
        assert_eq!(actual.cursor.high_water_mark, state.high);
        assert_eq!(actual.lowest_pending(), state.pending.iter().min().copied());
        let mut pending = state.pending.clone();
        pending.sort_unstable();
        assert_eq!(actual.pending.iter().copied().collect::<Vec<_>>(), pending);
        if let Some(commit) = state
            .pending
            .iter()
            .min()
            .copied()
            .or_else(|| state.high.map(|high| high + 1))
        {
            let _ = expected.insert(("test".to_string(), state.partition), commit);
        }
    }
    assert_eq!(tracker.committable_snapshot(), expected);
    let tpl = tracker.committable_tpl();
    let actual_native: HashMap<_, _> = tpl
        .elements()
        .iter()
        .map(|entry| {
            (
                (entry.topic().to_string(), entry.partition()),
                entry.offset(),
            )
        })
        .collect();
    let expected_native: HashMap<_, _> = expected
        .into_iter()
        .map(|(key, offset)| (key, Offset::Offset(offset)))
        .collect();
    assert_eq!(actual_native, expected_native);
}

/// Scenario: Every trace up to length four interleaves sparse deliveries, stale generations, acknowledgements, replay, and revocation across two partitions.
/// Guarantees: Concrete pending sets, cached minima and counts, generations, commit boundaries, and TPL membership agree with an independent Vec oracle after every event.
#[test]
fn short_offset_traces_match_independent_oracle() {
    let actions = [
        Action::Track(0, 0, 1),
        Action::Track(0, 5, 1),
        Action::Track(0, 2, 2),
        Action::Track(0, 5, 0),
        Action::Track(1, 9, 1),
        Action::Ack(0, 0),
        Action::Ack(0, 5),
        Action::Ack(1, 9),
        Action::Replay(0, 5, 1),
        Action::Replay(0, 2, 2),
        Action::Revoke(0, 1),
        Action::Revoke(1, 1),
    ];
    let mut count = 0;
    for width in 1..=4 {
        for mut encoded in 0..actions.len().pow(width) {
            let mut tracker = OffsetTracker::new();
            let mut reference = Vec::new();
            for _ in 0..width {
                let action = actions[encoded % actions.len()];
                encoded /= actions.len();
                step(&mut reference, &mut tracker, action);
            }
            count += 1;
        }
    }
    assert_eq!(count, 22_620);
}
