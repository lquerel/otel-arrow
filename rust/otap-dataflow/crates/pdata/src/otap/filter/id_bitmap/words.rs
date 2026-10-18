// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Word operations used by the production paged bitmap.

#[cfg(creusot)]
use creusot_std::{logic::ops::NthBitLogic, prelude::*};

#[cfg(creusot)]
use creusot_std::prelude::Clone;

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Union,
    Intersection,
    Difference,
}

#[cfg_attr(creusot, bitwise_proof)]
#[cfg_attr(creusot, ensures(forall<n> 0 <= n && n < 64 ==> !result.nth_bit(n)))]
pub(super) const fn zero_word() -> u64 {
    0
}

#[inline]
#[cfg_attr(creusot, bitwise_proof)]
#[cfg_attr(creusot, requires(bit@ < 64))]
#[cfg_attr(creusot, ensures(forall<n> 0 <= n && n < 64 ==> result.nth_bit(n) == (word.nth_bit(n) || n == bit@)))]
pub(super) fn insert_bit(word: u64, bit: usize) -> u64 {
    word | (1 << bit)
}

#[inline]
#[cfg_attr(creusot, bitwise_proof)]
#[cfg_attr(creusot, requires(bit@ < 64))]
#[cfg_attr(creusot, ensures(result == word.nth_bit(bit@)))]
pub(super) fn contains_bit(word: u64, bit: usize) -> bool {
    word & (1 << bit) != 0
}

#[inline]
#[cfg_attr(creusot, bitwise_proof)]
#[cfg_attr(creusot, ensures(forall<n> 0 <= n && n < 64 ==> result.nth_bit(n) == match op {
    Operation::Union => lhs.nth_bit(n) || rhs.nth_bit(n),
    Operation::Intersection => lhs.nth_bit(n) && rhs.nth_bit(n),
    Operation::Difference => lhs.nth_bit(n) && !rhs.nth_bit(n),
}))]
fn combine_word(lhs: u64, rhs: u64, op: Operation) -> u64 {
    match op {
        Operation::Union => lhs | rhs,
        Operation::Intersection => lhs & rhs,
        Operation::Difference => lhs & !rhs,
    }
}

#[cfg_attr(creusot, bitwise_proof)]
#[cfg_attr(creusot, ensures(result == forall<n> 0 <= n && n < 64 ==> !word.nth_bit(n)))]
fn word_empty(word: u64) -> bool {
    // Make finite bit extensionality explicit for the SMT solvers. These
    // assertions are checked obligations, not assumptions or trusted lemmas.
    #[cfg(creusot)]
    proof_assert! {
        (forall<n> 0 <= n && n < 64 ==> !word.nth_bit(n)) ==>
            !word.nth_bit(0) &&
            !word.nth_bit(1) &&
            !word.nth_bit(2) &&
            !word.nth_bit(3) &&
            !word.nth_bit(4) &&
            !word.nth_bit(5) &&
            !word.nth_bit(6) &&
            !word.nth_bit(7) &&
            !word.nth_bit(8) &&
            !word.nth_bit(9) &&
            !word.nth_bit(10) &&
            !word.nth_bit(11) &&
            !word.nth_bit(12) &&
            !word.nth_bit(13) &&
            !word.nth_bit(14) &&
            !word.nth_bit(15) &&
            !word.nth_bit(16) &&
            !word.nth_bit(17) &&
            !word.nth_bit(18) &&
            !word.nth_bit(19) &&
            !word.nth_bit(20) &&
            !word.nth_bit(21) &&
            !word.nth_bit(22) &&
            !word.nth_bit(23) &&
            !word.nth_bit(24) &&
            !word.nth_bit(25) &&
            !word.nth_bit(26) &&
            !word.nth_bit(27) &&
            !word.nth_bit(28) &&
            !word.nth_bit(29) &&
            !word.nth_bit(30) &&
            !word.nth_bit(31) &&
            !word.nth_bit(32) &&
            !word.nth_bit(33) &&
            !word.nth_bit(34) &&
            !word.nth_bit(35) &&
            !word.nth_bit(36) &&
            !word.nth_bit(37) &&
            !word.nth_bit(38) &&
            !word.nth_bit(39) &&
            !word.nth_bit(40) &&
            !word.nth_bit(41) &&
            !word.nth_bit(42) &&
            !word.nth_bit(43) &&
            !word.nth_bit(44) &&
            !word.nth_bit(45) &&
            !word.nth_bit(46) &&
            !word.nth_bit(47) &&
            !word.nth_bit(48) &&
            !word.nth_bit(49) &&
            !word.nth_bit(50) &&
            !word.nth_bit(51) &&
            !word.nth_bit(52) &&
            !word.nth_bit(53) &&
            !word.nth_bit(54) &&
            !word.nth_bit(55) &&
            !word.nth_bit(56) &&
            !word.nth_bit(57) &&
            !word.nth_bit(58) &&
            !word.nth_bit(59) &&
            !word.nth_bit(60) &&
            !word.nth_bit(61) &&
            !word.nth_bit(62) &&
            !word.nth_bit(63)
    }
    #[cfg(creusot)]
    proof_assert! {
        !word.nth_bit(0) &&
            !word.nth_bit(1) &&
            !word.nth_bit(2) &&
            !word.nth_bit(3) &&
            !word.nth_bit(4) &&
            !word.nth_bit(5) &&
            !word.nth_bit(6) &&
            !word.nth_bit(7) &&
            !word.nth_bit(8) &&
            !word.nth_bit(9) &&
            !word.nth_bit(10) &&
            !word.nth_bit(11) &&
            !word.nth_bit(12) &&
            !word.nth_bit(13) &&
            !word.nth_bit(14) &&
            !word.nth_bit(15) &&
            !word.nth_bit(16) &&
            !word.nth_bit(17) &&
            !word.nth_bit(18) &&
            !word.nth_bit(19) &&
            !word.nth_bit(20) &&
            !word.nth_bit(21) &&
            !word.nth_bit(22) &&
            !word.nth_bit(23) &&
            !word.nth_bit(24) &&
            !word.nth_bit(25) &&
            !word.nth_bit(26) &&
            !word.nth_bit(27) &&
            !word.nth_bit(28) &&
            !word.nth_bit(29) &&
            !word.nth_bit(30) &&
            !word.nth_bit(31) &&
            !word.nth_bit(32) &&
            !word.nth_bit(33) &&
            !word.nth_bit(34) &&
            !word.nth_bit(35) &&
            !word.nth_bit(36) &&
            !word.nth_bit(37) &&
            !word.nth_bit(38) &&
            !word.nth_bit(39) &&
            !word.nth_bit(40) &&
            !word.nth_bit(41) &&
            !word.nth_bit(42) &&
            !word.nth_bit(43) &&
            !word.nth_bit(44) &&
            !word.nth_bit(45) &&
            !word.nth_bit(46) &&
            !word.nth_bit(47) &&
            !word.nth_bit(48) &&
            !word.nth_bit(49) &&
            !word.nth_bit(50) &&
            !word.nth_bit(51) &&
            !word.nth_bit(52) &&
            !word.nth_bit(53) &&
            !word.nth_bit(54) &&
            !word.nth_bit(55) &&
            !word.nth_bit(56) &&
            !word.nth_bit(57) &&
            !word.nth_bit(58) &&
            !word.nth_bit(59) &&
            !word.nth_bit(60) &&
            !word.nth_bit(61) &&
            !word.nth_bit(62) &&
            !word.nth_bit(63) ==> word == 0u64
    }
    word == 0
}

#[inline]
#[cfg_attr(creusot, ensures(forall<w, n> 0 <= w && w < words@.len() && 0 <= n && n < 64 ==> !(^words)@[w].nth_bit(n)))]
pub(super) fn clear_words(words: &mut [u64]) {
    #[cfg(creusot)]
    let length = snapshot! { words@.len() };
    let mut i = 0;
    #[cfg_attr(creusot, invariant(words@.len() == *length))]
    #[cfg_attr(creusot, invariant(i@ <= words@.len()))]
    #[cfg_attr(creusot, invariant(forall<w, n> 0 <= w && w < i@ && 0 <= n && n < 64 ==> !words@[w].nth_bit(n)))]
    while i < words.len() {
        words[i] = zero_word();
        i += 1;
    }
}

#[inline]
#[cfg_attr(creusot, requires(lhs@.len() == rhs@.len()))]
#[cfg_attr(creusot, ensures(forall<w, n> 0 <= w && w < lhs@.len() && 0 <= n && n < 64 ==> (^lhs)@[w].nth_bit(n) == match op {
    Operation::Union => lhs@[w].nth_bit(n) || rhs@[w].nth_bit(n),
    Operation::Intersection => lhs@[w].nth_bit(n) && rhs@[w].nth_bit(n),
    Operation::Difference => lhs@[w].nth_bit(n) && !rhs@[w].nth_bit(n),
}))]
pub(super) fn combine_words(lhs: &mut [u64], rhs: &[u64], op: Operation) {
    #[cfg(creusot)]
    let before = snapshot! { lhs@ };
    let mut i = 0;
    #[cfg_attr(creusot, invariant(lhs@.len() == before.len()))]
    #[cfg_attr(creusot, invariant(lhs@.len() == rhs@.len()))]
    #[cfg_attr(creusot, invariant(i@ <= lhs@.len()))]
    #[cfg_attr(creusot, invariant(forall<w, n> 0 <= w && w < lhs@.len() && 0 <= n && n < 64 ==> lhs@[w].nth_bit(n) ==
        if w < i@ { match op {
            Operation::Union => before[w].nth_bit(n) || rhs@[w].nth_bit(n),
            Operation::Intersection => before[w].nth_bit(n) && rhs@[w].nth_bit(n),
            Operation::Difference => before[w].nth_bit(n) && !rhs@[w].nth_bit(n),
        }} else { before[w].nth_bit(n) }))]
    while i < lhs.len() {
        lhs[i] = combine_word(lhs[i], rhs[i], op);
        i += 1;
    }
}

#[inline]
#[cfg_attr(creusot, ensures(result == forall<w, n> 0 <= w && w < words@.len() && 0 <= n && n < 64 ==> !words@[w].nth_bit(n)))]
pub(super) fn words_empty(words: &[u64]) -> bool {
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= words@.len()))]
    #[cfg_attr(creusot, invariant(forall<w, n> 0 <= w && w < i@ && 0 <= n && n < 64 ==> !words@[w].nth_bit(n)))]
    while i < words.len() {
        if !word_empty(words[i]) {
            return false;
        }
        i += 1;
    }
    true
}
