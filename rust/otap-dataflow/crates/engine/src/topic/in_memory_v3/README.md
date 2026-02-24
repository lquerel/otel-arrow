# InMemory Topic Runtime V3 (Experimental)

## Goal

Provide an experimental in-process topic backend based on a disruptor-style
ring/cursor model, with one implementation that supports both subscription
semantics:

- `balanced` (competing consumers in a group)
- `broadcast` (independent per-subscriber cursor)

## Design

- `ring.rs`: fixed-size sequence-indexed ring.
- `balanced.rs`: one ring per balanced group, with backpressure policies.
- `broadcast.rs`: one shared ring for all broadcast subscribers.
- `runtime.rs`: topic registry + publisher/subscriber construction.

## Current Guarantees

- Supports `balanced_on_full: block | drop_newest`.
- Supports `broadcast_on_lag: drop_oldest | disconnect`.
- Supports dynamic balanced-group and broadcast subscriber creation.

## Current Limitations

- Publisher outcome tracking (`Ack/Nack`) is not implemented in v3.
- Publish with `outcome_interest != none` fails fast with
  `UnsupportedTopicPolicy`.
- This backend is experimental and currently benchmarked for no-outcome
  scenarios.
