// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounds sparse edits before they can construct recursively processed JSON.

use otel_arrow_dfe_admin::{ControlPlaneError, PipelineConfigChange, PipelineUpdateRequest};
use otel_arrow_dfe_config::secret::{OMITTED_VALUE, REDACTED_VALUE};
use serde_json::Value;

/// Pipeline root is depth zero; every object field or array element adds one.
pub(super) const MAX_PIPELINE_CONFIG_DEPTH: usize = 128;

fn invalid_depth() -> ControlPlaneError {
    ControlPlaneError::InvalidRequest {
        message: format!(
            "partial update exceeds the maximum pipeline configuration depth of {MAX_PIPELINE_CONFIG_DEPTH}"
        ),
    }
}

pub(super) fn validate_value(
    value: &Value,
    depth: usize,
    reject_markers: bool,
) -> Result<(), ControlPlaneError> {
    let mut pending = vec![(value, depth)];
    while let Some((value, depth)) = pending.pop() {
        if depth > MAX_PIPELINE_CONFIG_DEPTH {
            return Err(invalid_depth());
        }
        match value {
            Value::Object(fields) => pending.extend(fields.values().map(|v| (v, depth + 1))),
            Value::Array(values) => pending.extend(values.iter().map(|v| (v, depth + 1))),
            Value::String(value)
                if reject_markers && (value == OMITTED_VALUE || value == REDACTED_VALUE) =>
            {
                return Err(ControlPlaneError::InvalidRequest {
                    message: "snapshot display markers are not valid partial update values"
                        .to_owned(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) fn validate_request(request: &PipelineUpdateRequest) -> Result<(), ControlPlaneError> {
    for change in &request.changes {
        let path = match change {
            PipelineConfigChange::Set { path, .. } | PipelineConfigChange::Remove { path } => path,
        };
        if path.is_empty() || path.len() > MAX_PIPELINE_CONFIG_DEPTH {
            return Err(invalid_depth());
        }
        if let PipelineConfigChange::Set { value, .. } = change {
            validate_value(value, path.len(), true)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(changes: Vec<PipelineConfigChange>) -> PipelineUpdateRequest {
        PipelineUpdateRequest {
            expected_revision: 0,
            changes,
            step_timeout_secs: 1,
            drain_timeout_secs: 1,
        }
    }

    /// Scenario: Sparse paths and array values reach or exceed the pipeline depth budget.
    /// Guarantees: The boundary is accepted and combined path/value depth is checked before mutation.
    #[test]
    fn depth_checks_include_paths_and_array_elements() {
        let path = vec!["field".to_owned(); MAX_PIPELINE_CONFIG_DEPTH];
        assert!(
            validate_request(&request(vec![PipelineConfigChange::Set {
                path: path.clone(),
                value: Value::Null
            }]))
            .is_ok()
        );
        assert!(
            validate_request(&request(vec![PipelineConfigChange::Set {
                path: path.clone(),
                value: serde_json::json!([null])
            }]))
            .is_err()
        );
        assert!(validate_request(&request(vec![PipelineConfigChange::Remove { path }])).is_ok());
        for length in [0, MAX_PIPELINE_CONFIG_DEPTH + 1, 20_000] {
            let path = vec!["private-key".to_owned(); length];
            for change in [
                PipelineConfigChange::Remove { path: path.clone() },
                PipelineConfigChange::Set {
                    path,
                    value: Value::Null,
                },
            ] {
                let error = validate_request(&request(vec![change])).unwrap_err();
                assert!(!format!("{error:?}").contains("private-key"));
            }
        }
    }

    /// Scenario: A later edit exceeds depth or contains a nested reporting marker.
    /// Guarantees: Preflight rejects the complete batch, and value validation uses no recursive walk.
    #[test]
    fn preflight_checks_every_change_and_nested_markers() {
        let valid = PipelineConfigChange::Set {
            path: vec!["nodes".into()],
            value: serde_json::json!({}),
        };
        let invalid = PipelineConfigChange::Set {
            path: vec!["nested".into(); 128],
            value: serde_json::json!({"more": 1}),
        };
        assert!(validate_request(&request(vec![valid, invalid])).is_err());
        for marker in [REDACTED_VALUE, OMITTED_VALUE] {
            assert!(validate_value(&serde_json::json!({"array": [marker]}), 0, true).is_err());
        }
        let mut nested = Value::Null;
        for _ in 0..128 {
            nested = serde_json::json!([nested]);
        }
        assert!(validate_value(&nested, 0, false).is_ok());
        assert!(validate_value(&nested, 1, false).is_err());
    }
}
