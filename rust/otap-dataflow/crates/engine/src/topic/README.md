# Topic Runtime Abstraction

## Goals

This module defines the backend-agnostic runtime API used by topic-aware nodes.

It exists to:
- decouple publisher and subscriber nodes through named topics,
- support both balanced and broadcast subscriptions behind one contract,
- keep engine-facing code stable while allowing multiple backends (in-memory now, persistent/distributed later).

## Architecture

`mod.rs` is the public entrypoint and re-export surface.

Core modules:
- `contract.rs`: public runtime contract (`TopicRuntime`, `TopicPublisher`, `TopicSubscriber`, publish/outcome types).
- `delivery.rs`: delivery object and ack/nack handle API.
- `error.rs`: runtime error model.
- `capabilities.rs`: backend capability contract and policy support checks.
- `in_memory/`: first concrete backend implementation.

## Public Contracts

### Runtime lifecycle

- `TopicRuntime::create_topic(...)` creates a topic instance from a config backend selector + policies.
- `TopicRuntime::publisher(...)` returns a publisher handle, with optional local overrides (`TopicPublisherOptions`).
- `TopicRuntime::subscribe(...)` returns a subscriber handle for either:
  - `TopicSubscription::Balanced { group }`, or
  - `TopicSubscription::Broadcast`.

### Publish contract

`TopicPublisher::publish(payload)` returns `TopicPublishResult`:
- `report`: enqueue-time snapshot (`attempted_subscribers`, `delivered_subscribers`, `dropped_subscribers`),
- `outcome`: optional future resolved by downstream ack/nack path when publisher asked for it.

`TopicOutcomeInterest` lets a publisher opt in to outcome tracking (`None`, `Ack`, `Nack`, `AckOrNack`).

### Delivery + ack/nack contract

`TopicSubscriber::recv()` returns `TopicDelivery<T>`, which provides:
- payload access (`payload()`),
- settlement methods (`ack()`, `nack()`, `nack_with_reason(...)`),
- split mode (`into_parts()`) to move payload and settlement handle separately.

This contract intentionally keeps the backend-specific implementation hidden.

## Capability Contract

Backends advertise support through `TopicRuntimeCapabilities` and fail fast on unsupported policies at topic creation.

Current explicit capability gate:
- `broadcast_on_lag` support is checked by backend before topic creation succeeds.

## Guarantees

- Stable backend-agnostic API for publishers/subscribers.
- Explicit failure for unsupported policies/backends (`UnsupportedTopicPolicy`, `UnsupportedTopicBackend`).
- Ack/nack integration point exists in the shared API and does not leak backend internals.

## Non-Goals (Current Scope)

- No guarantee that all backends provide identical internal behavior for every policy.
- No persistence/distributed semantics in this abstraction layer itself.
- No exactly-once semantics promised by this module.

