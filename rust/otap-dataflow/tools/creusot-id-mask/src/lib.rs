// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared production ID selection kernels and independent membership tests.

#![allow(dead_code)]

extern crate self as otel_arrow_dfe_pdata;

pub mod otap {
    pub mod filter {
        pub use crate::id_bitmap::{IdBitmap, IdBitmapPool};
    }
}

#[path = "../../../crates/pdata/src/otap/filter/id_bitmap.rs"]
mod id_bitmap;

#[path = "../../../crates/query-engine/src/pipeline/id_mask.rs"]
mod id_mask;

#[cfg(test)]
mod tests {
    use super::{
        id_bitmap::{IdBitmap, IdBitmapPool},
        id_mask::IdMask,
    };
    use std::collections::BTreeSet;

    fn mask(kind: u8, ids: &[u32], pool: &mut IdBitmapPool) -> IdMask {
        match kind {
            0 => IdMask::All,
            1 => IdMask::None,
            _ => {
                let mut bitmap = pool.acquire();
                bitmap.populate(ids.iter().copied());
                if kind == 2 {
                    IdMask::Some(bitmap)
                } else {
                    IdMask::NotSome(bitmap)
                }
            }
        }
    }

    /// Scenario: all sixteen mask pairs combine across word/page boundaries and reused bitmaps.
    /// Guarantees: AND and OR match Boolean membership, including complements and empty sets.
    #[test]
    fn mask_composition_and_pool_reuse() {
        let mut pool = IdBitmapPool::new();
        let samples = [0, 1, 63, 64, 65, 65535, 65536, 65537, u32::MAX];
        for left_ids in [&samples[..0], &samples[..4], &samples[3..]] {
            for right_ids in [&samples[..0], &samples[2..6], &samples[..]] {
                for left_kind in 0..4 {
                    for right_kind in 0..4 {
                        for is_and in [false, true] {
                            let left = mask(left_kind, left_ids, &mut pool);
                            let right = mask(right_kind, right_ids, &mut pool);
                            let member = |kind, ids: &[u32], id| match kind {
                                0 => true,
                                1 => false,
                                2 => ids.contains(&id),
                                _ => !ids.contains(&id),
                            };
                            let expected = samples.map(|id| {
                                let a = member(left_kind, left_ids, id);
                                let b = member(right_kind, right_ids, id);
                                if is_and { a && b } else { a || b }
                            });
                            let result = if is_and {
                                left.combine_and(right, &mut pool)
                            } else {
                                left.combine_or(right, &mut pool)
                            };
                            for (id, expected) in samples.into_iter().zip(expected) {
                                assert_eq!(result.contains(id), expected);
                            }
                            result.release_to(&mut pool);
                        }
                    }
                }
            }
        }
        assert!(pool.acquire().is_empty());
    }

    /// Scenario: sparse ID sets include the maximum ID and asymmetric page allocation.
    /// Guarantees: bitmap operations match mathematical sets and clearing preserves reuse.
    #[test]
    fn paged_bitmap_matches_sets() {
        let mut state = 17u32;
        let ids: Vec<_> = (0..160)
            .map(|_| {
                state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                state
            })
            .chain([0, 63, 64, 65535, 65536, u32::MAX])
            .collect();
        let left: BTreeSet<_> = ids.iter().step_by(2).copied().collect();
        let right: BTreeSet<_> = ids.iter().step_by(3).copied().collect();
        let mut rhs = IdBitmap::new();
        rhs.populate(right.iter().copied());
        for op in 0..3 {
            let mut lhs = IdBitmap::new();
            lhs.populate(left.iter().copied());
            let expected: BTreeSet<_> = match op {
                0 => {
                    lhs.union_with(&rhs);
                    left.union(&right).copied().collect()
                }
                1 => {
                    lhs.intersect_with(&rhs);
                    left.intersection(&right).copied().collect()
                }
                _ => {
                    lhs.difference_with(&rhs);
                    left.difference(&right).copied().collect()
                }
            };
            assert_eq!(lhs.iter().collect::<BTreeSet<_>>(), expected);
            assert_eq!(lhs.len(), expected.len() as u64);
            for &id in &ids {
                assert_eq!(lhs.contains(id), expected.contains(&id));
            }
            for _ in 0..18 {
                lhs.clear();
            }
            assert!(lhs.is_empty());
            lhs.insert(u32::MAX);
            assert_eq!(lhs.iter().collect::<Vec<_>>(), vec![u32::MAX]);
            assert_eq!(rhs.iter().collect::<BTreeSet<_>>(), right);
        }
    }

    /// Scenario: a reused page reaches the last generation covered by the clear contract.
    /// Guarantees: the final permitted clear removes every ID and preserves the generation bound.
    #[test]
    fn last_valid_generation_clears_retained_pages() {
        let mut bitmap = IdBitmap::new();
        bitmap.generation = u64::MAX - 1;
        bitmap.insert(65536);
        bitmap.clear();
        assert_eq!(bitmap.generation, u64::MAX);
        assert!(bitmap.is_empty());
        assert!(bitmap.pages[1].is_some());
    }
}
