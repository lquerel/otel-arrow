// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent support for Weaver-generated semantic events.

use otap_df_telemetry::attributes::{AttributeSetHandler, AttributeValue};
use otap_df_telemetry::descriptor::AttributeValueType;

/// Concrete severity selected for one emitted event occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLevel {
    /// Debug-level occurrence.
    Debug,
    /// Informational occurrence.
    Info,
    /// Warning occurrence.
    Warn,
    /// Error occurrence.
    Error,
}

/// Severity behavior observed at one or more source call sites for an event.
///
/// Unlike [`EventLevel`], `Dynamic` describes a call site rather than a level
/// attached to an emitted event occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    /// Debug-level event.
    Debug,
    /// Informational event.
    Info,
    /// Warning event.
    Warn,
    /// Error event.
    Error,
    /// Severity is selected dynamically by the caller.
    Dynamic,
}

/// Requirement level attached to an event or event attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventRequirementLevel {
    /// The signal or attribute is required.
    Required,
    /// The signal or attribute is recommended.
    Recommended,
    /// The attribute is required only under a documented condition.
    ConditionallyRequired,
    /// The signal or attribute is opt-in.
    OptIn,
}

/// Static metadata for one event attribute.
#[derive(Debug, Clone, Copy)]
pub struct EventAttributeDescriptor {
    /// Canonical semantic-convention key.
    pub key: &'static str,
    /// Attribute key emitted by the current instrumentation.
    pub wire_key: &'static str,
    /// Human-readable description.
    pub brief: &'static str,
    /// Value kind declared by the semantic-convention registry.
    pub value_type: AttributeValueType,
    /// Attribute requirement level for this event.
    pub requirement_level: EventRequirementLevel,
}

/// Static metadata for one semantic event.
#[derive(Debug, Clone, Copy)]
pub struct EventDescriptor {
    /// Canonical semantic-convention event name.
    pub name: &'static str,
    /// Event name emitted by the current instrumentation.
    pub wire_name: &'static str,
    /// Human-readable description.
    pub brief: &'static str,
    /// Stability value from the semantic-convention definition.
    pub stability: &'static str,
    /// Event requirement level.
    pub requirement_level: EventRequirementLevel,
    /// Instrumentation scopes that currently emit this event.
    pub scope_names: &'static [&'static str],
    /// Severity levels observed at current source call sites.
    pub severity_levels: &'static [EventSeverity],
    /// Current source files containing event call sites.
    pub sources: &'static [&'static str],
    /// Conditional compilation expressions for current call sites.
    pub availability: &'static [&'static str],
    /// Entity types allowed by the semantic-convention association.
    pub entity_associations: &'static [&'static str],
    /// Ordered event attribute descriptors.
    pub attributes: &'static [EventAttributeDescriptor],
}

/// Borrowed event attribute value passed to an [`EventSink`].
#[derive(Debug, Clone, Copy)]
pub enum EventAttributeValueRef<'a> {
    /// Borrowed string value.
    String(&'a str),
    /// Signed integer value.
    Int(i64),
    /// Double-precision floating-point value.
    Double(f64),
    /// Boolean value.
    Boolean(bool),
    /// Dynamically typed value used for a registry `any` attribute.
    Any(&'a AttributeValue),
}

impl EventAttributeValueRef<'_> {
    /// Converts the borrowed representation into the telemetry runtime's owned value.
    #[must_use]
    pub fn into_owned(self) -> AttributeValue {
        match self {
            Self::String(value) => AttributeValue::String(value.to_owned()),
            Self::Int(value) => AttributeValue::Int(value),
            Self::Double(value) => AttributeValue::Double(value),
            Self::Boolean(value) => AttributeValue::Boolean(value),
            Self::Any(value) => value.clone(),
        }
    }
}

/// Object-safe attribute visitor implemented by every generated event payload.
pub trait EventAttributes {
    /// Visits present attributes in descriptor order without allocating.
    fn visit_attributes(
        &self,
        visitor: &mut dyn FnMut(&'static EventAttributeDescriptor, EventAttributeValueRef<'_>),
    );
}

/// Metadata contract implemented by every generated event payload.
pub trait SemanticEvent: EventAttributes {
    /// Static descriptor generated for this event.
    const DESCRIPTOR: &'static EventDescriptor;
}

/// Backend contract consumed by the generated event client.
///
/// Implementations may collect events for tests or adapt them to a logging
/// backend. This prototype intentionally provides no adapter to existing
/// instrumentation.
pub trait EventSink {
    /// Receives one typed event associated with a semantic entity.
    fn emit(
        &mut self,
        entity: &dyn AttributeSetHandler,
        descriptor: &'static EventDescriptor,
        level: EventLevel,
        attributes: &dyn EventAttributes,
    );
}

/// Thin client that dispatches generated event payloads to a caller-provided sink.
#[derive(Debug, Clone)]
pub struct EventClient<S> {
    sink: S,
}

impl<S> EventClient<S> {
    /// Creates a client backed by `sink`.
    #[must_use]
    pub const fn new(sink: S) -> Self {
        Self { sink }
    }

    /// Returns a shared reference to the backend.
    #[must_use]
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Returns a mutable reference to the backend.
    #[must_use]
    pub const fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// Consumes the client and returns its backend.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.sink
    }
}

impl<S: EventSink> EventClient<S> {
    pub(crate) fn emit<E, A>(&mut self, entity: &A, event: &E, level: EventLevel)
    where
        E: SemanticEvent,
        A: AttributeSetHandler,
    {
        self.sink.emit(entity, E::DESCRIPTOR, level, event);
    }
}
