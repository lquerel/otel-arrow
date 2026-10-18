// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Paged ID storage, shared with the standalone Creusot target.

#[cfg(creusot)]
use creusot_std::{logic::ops::NthBitLogic, prelude::*};

/// Number of u64 words per page. Each page covers 65,536 IDs (one full u16 range).
pub(super) const ID_BITMAP_PAGE_WORDS: usize = 1024;

/// Number of [`clear`](IdBitmap::clear) cycles a page can remain unused before being evicted
/// (deallocated). A threshold of 16 means a page that hasn't been touched in 16 consecutive
/// `clear()` calls will be freed, preventing unbounded memory growth from adversarial inputs
/// while avoiding thrashing for pages that are used intermittently.
pub(super) const ID_BITMAP_PAGE_EVICTION_THRESHOLD: u64 = 16;

/// A single page of the [`IdBitmap`], covering 65,536 IDs (8 KiB of bitmap data).
///
/// Each page tracks the generation in which it was last written, enabling the bitmap to evict
/// pages that haven't been touched in several cycles.
pub(super) struct IdBitmapPage {
    pub(super) words: [u64; ID_BITMAP_PAGE_WORDS],
    pub(super) last_used_generation: u64,
}

impl IdBitmapPage {
    /// Creates a new zeroed page stamped with the given generation.
    #[cfg_attr(creusot, ensures(result.last_used_generation == generation))]
    #[cfg_attr(creusot, ensures(forall<w, n> 0 <= w && w < 1024 && 0 <= n && n < 64 ==> !result.words@[w].nth_bit(n)))]
    fn new(generation: u64) -> Self {
        Self {
            words: [words::zero_word(); ID_BITMAP_PAGE_WORDS],
            last_used_generation: generation,
        }
    }
}

/// A paged bitmap for fast membership testing of ID values.
///
/// The underlying bitmap data is heap allocated, and the intention of this type is that it
/// can be reused between batches by calling the `clear` method. This method is also called
/// automatically by the `populate` method, allowing the bitmap to be rewritten from some
/// input ID column.
///
/// The ID space is partitioned into pages of 65,536 IDs each (8 KiB per page). For the common
/// case of dense IDs starting near 0 (typical of OTAP batches), there few pages are allocated.
///
/// The motivation for the paged bitmap is to protect against adversarial situations where we
/// receive batches containing few, sparse IDs.
///
/// ## Page lifecycle
///
/// Each page tracks the generation (batch cycle) in which it was last written. On
/// [`clear`](IdBitmap::clear), the generation counter is incremented and pages are evaluated:
///
/// - Pages used within the last [`ID_BITMAP_PAGE_EVICTION_THRESHOLD`] generations are zeroed and retained.
/// - Pages that haven't been used in more than [`ID_BITMAP_PAGE_EVICTION_THRESHOLD`] generations are
///   deallocated, preventing unbounded memory growth from adversarial or unusual input patterns.
///
/// This means pages that are used regularly (even intermittently) stay allocated, while pages
/// from one-off anomalous batches are eventually freed.
pub struct IdBitmap {
    pub(super) pages: Vec<Option<Box<IdBitmapPage>>>,
    pub(super) generation: u64,
}

#[cfg(creusot)]
impl creusot_std::invariant::Invariant for IdBitmap {
    #[logic(open(super))]
    fn invariant(self) -> bool {
        pearlite! { self.pages@.len() <= 65536 &&
            forall<i> 0 <= i && i < self.pages@.len() ==>
                match self.pages@[i] {
                    Some(page) => page.last_used_generation <= self.generation,
                    None => true,
                }
        }
    }
}

impl IdBitmap {
    /// Returns the number of IDs stored in the bitmap (popcount).
    #[must_use]
    #[cfg(not(creusot))]
    pub fn len(&self) -> u64 {
        self.pages
            .iter()
            .filter_map(|p| p.as_ref())
            .flat_map(|p| p.words.iter())
            .map(|w| w.count_ones() as u64)
            .sum()
    }

    /// Clears the bitmap and repopulates it from the given iterator.
    ///
    /// This reuses existing page allocations when possible. New pages are allocated as needed,
    /// and stale pages are evicted per the generation-based eviction policy.
    #[cfg(not(creusot))]
    pub fn populate(&mut self, iter: impl Iterator<Item = u32>) {
        self.clear();
        for id in iter {
            self.insert(id);
        }
    }

    /// Returns an iterator over IDs present in the bitmap.
    #[must_use]
    #[cfg(not(creusot))]
    pub fn iter(&self) -> IdBitmapIter<'_> {
        IdBitmapIter::new(self)
    }
    #[cfg(creusot)]
    #[logic(open(super))]
    pub(super) fn bit(self, page: Int, word: Int, bit: Int) -> bool {
        pearlite! {
            if 0 <= page && page < self.pages@.len() &&
                0 <= word && word < 1024 && 0 <= bit && bit < 64 {
                match self.pages@[page] {
                    Some(p) => p.words@[word].nth_bit(bit),
                    None => false,
                }
            } else { false }
        }
    }

    #[cfg(creusot)]
    #[logic(open(super))]
    pub(super) fn has(self, id: u32) -> bool {
        pearlite! { self.bit(id@ / 65536, (id@ % 65536) / 64, id@ % 64) }
    }

    /// Creates a new empty bitmap.
    #[must_use]
    #[cfg_attr(creusot, ensures(forall<p, w, n> !result.bit(p, w, n)))]
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            generation: 0,
        }
    }

    /// Clears all bits and evicts stale pages.
    ///
    /// The existing generation policy requires fewer than `u64::MAX` prior clears.
    /// The proof makes this boundary explicit; generation rollover is not verified.
    #[cfg_attr(creusot, requires(self.generation < u64::MAX))]
    #[cfg_attr(creusot, ensures(forall<p, w, n> !(^self).bit(p, w, n)))]
    #[cfg_attr(creusot, ensures((^self).generation@ == self.generation@ + 1))]
    pub fn clear(&mut self) {
        self.generation += 1;
        #[cfg(creusot)]
        let generation = snapshot! { self.generation };
        let mut i = 0;
        #[cfg_attr(creusot, invariant(i@ <= self.pages@.len()))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> p < i@ ==> !self.bit(p, w, n)))]
        #[cfg_attr(creusot, invariant(self.generation == *generation))]
        #[cfg_attr(creusot, invariant(creusot_std::invariant::inv(*self)))]
        while i < self.pages.len() {
            if let Some(page) = &mut self.pages[i] {
                if self.generation - page.last_used_generation > ID_BITMAP_PAGE_EVICTION_THRESHOLD {
                    self.pages[i] = None;
                } else {
                    words::clear_words(&mut page.words);
                }
            }
            i += 1;
        }
        #[cfg_attr(creusot, invariant(forall<p, w, n> !self.bit(p, w, n)))]
        #[cfg_attr(creusot, invariant(self.generation == *generation))]
        #[cfg_attr(creusot, invariant(creusot_std::invariant::inv(*self)))]
        while self.pages.last().is_some_and(|p| p.is_none()) {
            let _ = self.pages.pop();
        }
    }

    /// Returns the page and bit offsets for every u32 ID.
    #[inline]
    #[cfg_attr(creusot, ensures(result.0@ == id@ / 65536))]
    #[cfg_attr(creusot, ensures(result.1@ == id@ % 65536))]
    const fn page_and_bit(id: u32) -> (usize, usize) {
        ((id / 65536) as usize, (id % 65536) as usize)
    }

    /// Ensures a page exists and stamps it with the current generation.
    #[inline]
    #[cfg_attr(creusot, requires(page_idx@ < 65536))]
    #[cfg_attr(creusot, ensures((^self).generation == self.generation))]
    #[cfg_attr(creusot, ensures(forall<w, n> 0 <= w && w < 1024 && 0 <= n && n < 64 ==>
        result@[w].nth_bit(n) == self.bit(page_idx@, w, n)))]
    #[cfg_attr(creusot, ensures(forall<p, w, n> 0 <= w && w < 1024 && 0 <= n && n < 64 ==>
        (^self).bit(p, w, n) == if p == page_idx@ {
            (^result)@[w].nth_bit(n)
        } else { self.bit(p, w, n) }))]
    fn ensure_page(&mut self, page_idx: usize) -> &mut [u64; ID_BITMAP_PAGE_WORDS] {
        #[cfg(creusot)]
        let before = snapshot! { *self };
        #[cfg_attr(creusot, invariant(self.generation == before.generation))]
        #[cfg_attr(creusot, invariant(creusot_std::invariant::inv(*self)))]
        #[cfg_attr(creusot, invariant(self.pages@.len() <= 65536))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> self.bit(p, w, n) == (*before).bit(p, w, n)))]
        while page_idx >= self.pages.len() {
            self.pages.push(None);
        }
        let generation = self.generation;
        let page =
            self.pages[page_idx].get_or_insert_with(|| Box::new(IdBitmapPage::new(generation)));
        page.last_used_generation = generation;
        &mut page.words
    }

    /// Inserts an ID, preserving membership of every other ID.
    #[inline]
    #[cfg_attr(creusot, ensures(forall<p, w, n> 0 <= w && w < 1024 && 0 <= n && n < 64 ==>
        (^self).bit(p, w, n) == (self.bit(p, w, n) ||
            (p == id@ / 65536 && w == (id@ % 65536) / 64 && n == id@ % 64))))]
    pub fn insert(&mut self, id: u32) {
        let (page_idx, bit_idx) = Self::page_and_bit(id);
        let page = self.ensure_page(page_idx);
        page[bit_idx / 64] = words::insert_bit(page[bit_idx / 64], bit_idx % 64);
    }

    /// Tests membership, treating unallocated pages as empty.
    #[inline]
    #[must_use]
    #[cfg_attr(creusot, ensures(result == self.has(id)))]
    pub fn contains(&self, id: u32) -> bool {
        let (page_idx, bit_idx) = Self::page_and_bit(id);
        match self.pages.get(page_idx) {
            Some(Some(page)) => words::contains_bit(page.words[bit_idx / 64], bit_idx % 64),
            _ => false,
        }
    }

    /// Returns true exactly when no ID is selected.
    #[must_use]
    #[cfg_attr(creusot, ensures(result == forall<p, w, n> !self.bit(p, w, n)))]
    pub fn is_empty(&self) -> bool {
        let mut i = 0;
        #[cfg_attr(creusot, invariant(i@ <= self.pages@.len()))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> p < i@ ==> !self.bit(p, w, n)))]
        while i < self.pages.len() {
            if let Some(page) = &self.pages[i]
                && !words::words_empty(&page.words)
            {
                return false;
            }
            i += 1;
        }
        true
    }

    /// In-place union of the selected IDs.
    #[cfg_attr(creusot, ensures(forall<p, w, n> (^self).bit(p, w, n) == (self.bit(p, w, n) || other.bit(p, w, n))))]
    pub fn union_with(&mut self, other: &Self) {
        #[cfg(creusot)]
        let before = snapshot! { *self };
        let mut i = 0;
        #[cfg_attr(creusot, invariant(i@ <= other.pages@.len()))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> self.bit(p, w, n) ==
            if p < i@ { (*before).bit(p, w, n) || other.bit(p, w, n) } else { (*before).bit(p, w, n) }))]
        while i < other.pages.len() {
            if let Some(op) = &other.pages[i] {
                let sp = self.ensure_page(i);
                words::combine_words(sp, &op.words, words::Operation::Union);
            }
            i += 1;
        }
    }

    /// In-place intersection of the selected IDs.
    #[cfg_attr(creusot, ensures(forall<p, w, n> (^self).bit(p, w, n) == (self.bit(p, w, n) && other.bit(p, w, n))))]
    pub fn intersect_with(&mut self, other: &Self) {
        #[cfg(creusot)]
        let before = snapshot! { *self };
        let mut i = 0;
        #[cfg_attr(creusot, invariant(creusot_std::invariant::inv(*self)))]
        #[cfg_attr(creusot, invariant(self.pages@.len() == before.pages@.len()))]
        #[cfg_attr(creusot, invariant(self.generation == before.generation))]
        #[cfg_attr(creusot, invariant(i@ <= self.pages@.len()))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> self.bit(p, w, n) ==
            if p < i@ { (*before).bit(p, w, n) && other.bit(p, w, n) } else { (*before).bit(p, w, n) }))]
        while i < self.pages.len() {
            if let Some(sp) = &mut self.pages[i] {
                match other.pages.get(i) {
                    Some(Some(op)) => {
                        sp.last_used_generation = self.generation;
                        words::combine_words(
                            &mut sp.words,
                            &op.words,
                            words::Operation::Intersection,
                        );
                    }
                    _ => {
                        words::clear_words(&mut sp.words);
                    }
                }
            }
            i += 1;
        }
    }

    /// In-place difference of the selected IDs.
    #[cfg_attr(creusot, ensures(forall<p, w, n> (^self).bit(p, w, n) == (self.bit(p, w, n) && !other.bit(p, w, n))))]
    pub fn difference_with(&mut self, other: &Self) {
        #[cfg(creusot)]
        let before = snapshot! { *self };
        let mut i = 0;
        #[cfg_attr(creusot, invariant(creusot_std::invariant::inv(*self)))]
        #[cfg_attr(creusot, invariant(self.pages@.len() == before.pages@.len()))]
        #[cfg_attr(creusot, invariant(self.generation == before.generation))]
        #[cfg_attr(creusot, invariant(i@ <= self.pages@.len()))]
        #[cfg_attr(creusot, invariant(forall<p, w, n> self.bit(p, w, n) ==
            if p < i@ { (*before).bit(p, w, n) && !other.bit(p, w, n) } else { (*before).bit(p, w, n) }))]
        while i < self.pages.len() {
            if let Some(sp) = &mut self.pages[i]
                && let Some(Some(op)) = other.pages.get(i)
            {
                sp.last_used_generation = self.generation;
                words::combine_words(&mut sp.words, &op.words, words::Operation::Difference);
            }
            i += 1;
        }
    }
}

#[path = "id_bitmap/words.rs"]
mod words;

/// A pool of reusable [`IdBitmap`] instances to avoid repeated heap allocations.
///
/// When acquiring a bitmap, the pool returns a previously released bitmap (already cleared) or
/// creates a new one if the pool is empty. When releasing a bitmap, it is returned to the pool
/// for future reuse. The bitmaps retain their page allocations across reuse cycles.
pub struct IdBitmapPool {
    bitmaps: Vec<IdBitmap>,
}

impl IdBitmapPool {
    /// Creates a new empty pool.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bitmaps: Vec::new(),
        }
    }

    /// Acquires an [`IdBitmap`] from the pool, or creates a new empty one if the pool is empty.
    ///
    /// The returned bitmap is cleared and ready for use.
    #[cfg_attr(creusot, requires(forall<i> 0 <= i && i < self.bitmaps@.len() ==> self.bitmaps@[i].generation < u64::MAX))]
    #[cfg_attr(creusot, ensures(forall<p, w, n> !result.bit(p, w, n)))]
    pub fn acquire(&mut self) -> IdBitmap {
        match self.bitmaps.pop() {
            Some(mut bm) => {
                bm.clear();
                bm
            }
            None => IdBitmap::new(),
        }
    }

    /// Returns an [`IdBitmap`] to the pool for future reuse.
    pub fn release(&mut self, bitmap: IdBitmap) {
        self.bitmaps.push(bitmap);
    }
}

impl Default for IdBitmapPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(creusot))]
impl Default for IdBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(creusot))]
impl std::fmt::Debug for IdBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let allocated_pages = self.pages.iter().filter(|p| p.is_some()).count();
        f.debug_struct("IdBitmap")
            .field("total_pages", &self.pages.len())
            .field("allocated_pages", &allocated_pages)
            .field("generation", &self.generation)
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(not(creusot))]
impl PartialEq for IdBitmap {
    fn eq(&self, other: &Self) -> bool {
        // Compare the logical content only (not generation counters): for each page index,
        // both bitmaps must have the same bits set. Missing or None pages are treated as
        // all-zeros.
        let max_pages = self.pages.len().max(other.pages.len());
        for i in 0..max_pages {
            let self_words = self
                .pages
                .get(i)
                .and_then(|p| p.as_ref())
                .map(|p| &p.words[..]);
            let other_words = other
                .pages
                .get(i)
                .and_then(|p| p.as_ref())
                .map(|p| &p.words[..]);
            match (self_words, other_words) {
                (Some(sw), Some(ow)) => {
                    if sw != ow {
                        return false;
                    }
                }
                (Some(w), None) | (None, Some(w)) => {
                    if w.iter().any(|&v| v != 0) {
                        return false;
                    }
                }
                (None, None) => {}
            }
        }
        true
    }
}

/// An iterator over the IDs present in an [`IdBitmap`].
///
/// This performs work proportional to the number of set bits, not the total bitmap capacity.
#[cfg(not(creusot))]
pub struct IdBitmapIter<'a> {
    bitmap: &'a IdBitmap,
    /// Current page index into `bitmap.pages`.
    page_idx: usize,
    /// Index of the *next* word to examine in the current page. When `current_word` is non-zero,
    /// it was loaded from `words[word_idx - 1]`.
    word_idx: usize,
    /// Remaining bits from the word at `words[word_idx - 1]`. Set bits are consumed by clearing
    /// the lowest set bit on each `next()` call.
    current_word: u64,
}

#[cfg(not(creusot))]
impl<'a> IdBitmapIter<'a> {
    fn new(bitmap: &'a IdBitmap) -> Self {
        Self {
            bitmap,
            page_idx: 0,
            word_idx: 0,
            current_word: 0,
        }
    }
}

#[cfg(not(creusot))]
impl Iterator for IdBitmapIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            // If the current word has bits remaining, extract the lowest set bit.
            // word_idx points one past the word we loaded, so the actual word index is
            // word_idx - 1.
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros();
                self.current_word &= self.current_word - 1;
                let id = ((self.page_idx as u32) << 16) | (((self.word_idx as u32) - 1) * 64) | bit;
                return Some(id);
            }

            // Find the next non-zero word, advancing through words and pages as needed.
            loop {
                // Try the next word in the current page
                if let Some(Some(page)) = self.bitmap.pages.get(self.page_idx)
                    && self.word_idx < ID_BITMAP_PAGE_WORDS
                {
                    let word = page.words[self.word_idx];
                    self.word_idx += 1;
                    if word != 0 {
                        self.current_word = word;
                        break; // Break inner loop, outer loop will extract bits
                    }
                    continue; // Try next word in this page
                }

                // No more words in this page (or page was None) -- advance to the next page
                self.page_idx += 1;
                self.word_idx = 0;

                if self.page_idx > self.bitmap.pages.len() {
                    return None;
                }
            }
        }
    }
}
