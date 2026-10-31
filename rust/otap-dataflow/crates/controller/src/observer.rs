// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../../../docs/engine-observer-extensions.md")]

use crate::{
    ControllerExtensionContext, ControllerExtensionError, ControllerExtensionTaskFactory,
    ControllerExtensionValidateFn, distributed_slice,
};
use futures_util::FutureExt;
use otel_arrow_dfe_config::ExtensionId;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_state::store::ObservedStateHandle;
use otel_arrow_dfe_telemetry::observer::{EngineObservationSubscription, ObserverEventEmitter};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

/// An observer extension receives observation capabilities, never an engine control-plane handle.
pub struct EngineObserverContext {
    /// Configured observer instance identity.
    pub extension_id: ExtensionId,
    /// The instance's type and opaque configuration.
    pub extension: Arc<ExtensionUserConfig>,
    /// Independent cursor over the engine's bounded shared input stream.
    pub observations: EngineObservationSubscription,
    /// Current observed state, for initial reads and recovery after lag.
    pub observed_state: ObservedStateHandle,
    /// Output capability used by `otel_arrow_dfe_telemetry::observer_event!`.
    pub events: ObserverEventEmitter,
}

/// Registration for a generic engine observer, using the controller extension config namespace.
#[derive(Clone, Copy)]
pub struct EngineObserverFactory {
    /// Type URN used under `engine.controller.extensions.<id>.type`.
    pub name: &'static str,
    /// Short description of the observer type.
    pub description: &'static str,
    /// Documentation URL, if any.
    pub documentation_url: &'static str,
    /// Static config validation before any engine thread starts.
    pub validate_config: ControllerExtensionValidateFn,
    /// Prepare the observer task. Runtime errors/panics are isolated from engine control.
    pub start: fn(
        EngineObserverContext,
    ) -> Result<ControllerExtensionTaskFactory, ControllerExtensionError>,
}

/// Linked observer factories, discovered by the default controller extension registry.
#[distributed_slice]
pub static ENGINE_OBSERVER_FACTORIES: [EngineObserverFactory] = [..];

pub(crate) fn prepare(
    factory: EngineObserverFactory,
    context: ControllerExtensionContext,
) -> Result<ControllerExtensionTaskFactory, ControllerExtensionError> {
    let observations = context
        .telemetry_registry
        .observations()
        .subscribe(context.extension_id.as_ref())?;
    let events = observations.emitter();
    let task_factory = (factory.start)(EngineObserverContext {
        extension_id: context.extension_id,
        extension: context.extension,
        observations,
        observed_state: context.observed_state,
        events: events.clone(),
    })?;
    Ok(isolate(task_factory, events))
}

fn isolate(
    task_factory: ControllerExtensionTaskFactory,
    events: ObserverEventEmitter,
) -> ControllerExtensionTaskFactory {
    Box::new(move |cancel| {
        Box::pin(async move {
            // Preserve !Send futures and the existing thread-local extension model.
            let result = match catch_unwind(AssertUnwindSafe(|| task_factory(cancel))) {
                Ok(task) => AssertUnwindSafe(task).catch_unwind().await,
                Err(panic) => Err(panic),
            };
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    otel_arrow_dfe_telemetry::observer_event!(
                        events, ERROR, "observer.runtime_failed", error = %error
                    )
                }
                Err(_) => otel_arrow_dfe_telemetry::observer_event!(
                    events,
                    ERROR,
                    "observer.runtime_panicked",
                    message = "Observer stopped after a panic"
                ),
            }
            // An observer has no authority to shut down pipelines on failure.
            Ok(())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ControllerExtensionRegistry;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use tokio_util::sync::CancellationToken;

    // A linked fixture proves that a downstream crate can register an observer without
    // adding a factory to a collector's explicit registry construction.
    #[allow(unsafe_code)]
    #[distributed_slice(ENGINE_OBSERVER_FACTORIES)]
    static FIXTURE: EngineObserverFactory = EngineObserverFactory {
        name: "urn:test:extension:engine_observer",
        description: "Generic observer fixture",
        documentation_url: "",
        validate_config: otel_arrow_dfe_config::validation::no_config,
        start: |context| {
            Ok(Box::new(move |cancel| {
                Box::pin(async move {
                    // Thread-local state is allowed inside the future.
                    let identity = std::rc::Rc::new(context.extension_id.clone());
                    otel_arrow_dfe_telemetry::observer_event!(context.events, INFO, "observer.started", instance = %identity);
                    cancel.cancelled().await;
                    drop(context);
                    Ok(())
                })
            }))
        },
    };

    /// Scenario: a linked observer factory is loaded by the collector's default extension registry.
    /// Guarantees: observer registration is automatic and preserves its distinct runtime policy.
    #[test]
    fn linked_factory_is_discovered() {
        let registry = ControllerExtensionRegistry::default();
        assert!(registry.get(&FIXTURE.name.into()).unwrap().is_observer);
    }

    /// Scenario: observer tasks return an error, panic on construction or panic while polling.
    /// Guarantees: each failure ends only that task, releases its subscription and leaves peers active.
    #[tokio::test]
    async fn runtime_errors_and_panics_are_isolated() {
        for mode in 0..3 {
            let registry = TelemetryRegistryHandle::new();
            let source = registry.observations();
            let peer = source.subscribe("peer").unwrap();
            let subscriber = source.subscribe("failing").unwrap();
            let emitter = subscriber.emitter();
            let task: ControllerExtensionTaskFactory = Box::new(move |_| {
                if mode == 1 {
                    panic!("construction failure");
                }
                Box::pin(async move {
                    let _subscription = subscriber;
                    if mode == 2 {
                        panic!("polling failure");
                    }
                    Err(std::io::Error::other("task failure").into())
                })
            });
            isolate(task, emitter)(CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(source.subscriber_count(), 1);
            assert_eq!(peer.observer_id(), "peer");
            assert!(source.subscribe("failing").is_ok());
        }
    }
}
