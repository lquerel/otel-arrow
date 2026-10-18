# IdMask and bitmap Creusot verification

This standalone target verifies the production ID-selection implementation:

- [`id_mask.rs`](../../crates/query-engine/src/pipeline/id_mask.rs): Boolean
  composition of explicit and complemented ID sets.
- [`id_bitmap.rs`](../../crates/pdata/src/otap/filter/id_bitmap.rs): paged storage,
  membership, set operations, clearing, and pool acquisition.
- [`words.rs`](../../crates/pdata/src/otap/filter/id_bitmap/words.rs): the actual
  word operations used by that storage.

The normal crates and proof target compile the same executable function bodies.
Contracts, logical definitions, and checked proof assertions use `cfg(creusot)`.
There are no trusted substitutes for bitmap operations. Production dependencies
and the production compiler pin are unchanged.

This follow-up is based on `chore/creusot-rate-bucket-verification`, which owns
shared tool provisioning. The extraction, word helpers, and mask contracts form
one verification boundary: proving the mask algebra requires proving the bitmap
operations on which it depends.

## Run

Use the Linux x86_64 prerequisites and
[pinned toolchain recipe](../creusot-rate-bucket/README.md#tooling-pins) from the
rate-bucket infrastructure. From the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-id-mask/verify.py
```

The runner uses the existing `creusot-rate-bucket/build.sh` and Dockerfile; there
is one maintained toolchain image, not a second installer. After building it:

```sh
python3 rust/otap-dataflow/tools/creusot-id-mask/verify.py --skip-build
```

`--image NAME --skip-build` supports experiments with an explicitly selected
image. Reports retain the actual image ID. CI builds the pinned recipe once and
runs both targets with it. This target selects Alt-Ergo, CVC5, and Z3 from the
same checksum-verified bundle; its solver selection is in `why3find.json`.
Creusot and `creusot-std` are pinned to 0.14.0, with the matching nightly in
`rust-toolchain.toml` and a separate checked-in Cargo.lock.

The ordinary tests require no Creusot installation:

```sh
cd rust/otap-dataflow
cargo +1.98.1 test --manifest-path tools/creusot-id-mask/Cargo.toml --locked
```

These tests compare all sixteen pairs of mask variants against independent
Boolean membership, including empty sets, complements, word/page boundaries,
and `u32::MAX`. Bitmap operations are also compared against `BTreeSet` with
sparse IDs, asymmetric pages, eviction, and reuse. A boundary test checks the
last permitted generation increment. Tests complement the universal contracts;
they do not replace verification.

## Contracts

For every `u32` ID, `IdMask` composition establishes:

```text
contains(and(a, b), id) = contains(a, id) && contains(b, id)
contains(or(a, b), id)  = contains(a, id) || contains(b, id)
```

The logical membership definition interprets `All`, `None`, an explicit set,
and a complemented set independently of the composition branches. The bitmap
proofs connect that definition to the actual page, word, and bit storage:

- New bitmaps are empty, and ID decomposition chooses the correct offsets.
- Insertion sets exactly the requested ID and preserves every other ID.
- Runtime membership and emptiness agree with the logical set.
- Union, intersection, and difference match set membership, including absent
  pages and unequal page allocation.
- Clearing removes every bit, including retained pages; pool acquisition
  returns an empty bitmap.
- Word helpers satisfy their bit-level contracts and preserve slice bounds.

The structural invariant bounds the page vector to 65,536 entries and requires
page generations to be no newer than the bitmap generation. Invalid internal
states cannot be constructed through the public API. `ensure_page` and word
helpers have index/length preconditions that their verified callers discharge.

## Preconditions and exclusions

`clear` requires `generation < u64::MAX`. Pool acquisition conservatively
requires this for every cached bitmap. These expose the existing runtime
counter boundary: rollover behavior is unchanged, and callers outside the
proof target are not formally proved to satisfy this lifetime limit. The proof
therefore does not establish arbitrary-length reuse. A rollover policy would
need a separately reviewed runtime change and updated contracts.

`populate`, Arrow column decoding/population, iteration, popcount, equality,
and formatting remain ordinary production code and are excluded from the
Creusot target. In particular, this is a proof of ID-set composition, not of
Arrow null semantics, expression evaluation, or complete telemetry filtering.
Exact eviction timing, allocation/capacity behavior, pool size, allocation
failure, performance, and termination are outside the formal guarantee.

The index loops and word helpers replace iterator-based operations so their
state transitions can be specified. They execute in production too. No
verification-only algorithm replaces a production body. Existing bitmap and
query-engine tests and performance checks remain necessary for this hot path.

No application implementation or lemma is marked `trusted`, and this target
adds no external specifications. It relies on Creusot's standard-library
specifications, compiler translation, Why3, and the selected solvers. The
finite-bit assertions in `word_empty` are checked obligations, not assumptions.
A proof establishes conformance to these requirements; completeness of the
requirements still needs semantic review.

## Evidence and negative controls

`expected-proofs.json` records all 29 translated files and their named
obligations, including mask composition, bitmap storage, word operations, and
helper obligations. The runner requires an exact match and successful fresh
verification. Inventory changes require review; the runner never regenerates
this manifest automatically.

A complete run tests the shared code, proves the original source, and rejects
all six deliberate production mutations at their intended goals:

| Mutation | Required rejection |
| --- | --- |
| Explicit-set AND uses union | `vc_combine_and` |
| Complemented-set OR uses union | `vc_combine_or` |
| Difference keeps overlapping bits | `vc_combine_word` |
| Clearing retains old bits | `vc_clear_words` |
| Pool acquisition skips clearing | `vc_acquire` |
| ID decomposition uses the wrong page width | `vc_page_and_bit` |

Each mutation runs in a disposable copy, with contracts intact and fresh
translation and solver results. The checkout is never mounted into verification
containers. The original source is restored between mutations and must prove
again at the end. Compilation errors, setup failures, and failures at unrelated
goals do not count as successful negative controls.

`target/reports/` contains logs, baseline translations and certificates, source
hashes, Git revision/dirty status, and image ID. `result.json` becomes `proved`
only after every check passes. CI retains these revision-associated artifacts
for 14 days, including incomplete diagnostics on failure. Generated output and
caches are ignored; contracts, proof inventory, tests, scripts, and pins are
versioned. Solver caches and replayed proofs are not verification evidence.

Evidence-handling tests can run independently:

```sh
python3 -m unittest discover -s rust/otap-dataflow/tools/creusot-id-mask -p 'test_*.py'
```

See the [verification policy](../../docs/creusot-verification.md) for advisory
CI status, ownership, maintenance, and tool-upgrade requirements.
