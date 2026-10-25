// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Real-process regressions for sparse edits and sensitive management reporting.

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Engine(Child);
impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &Value,
) -> std::io::Result<(u16, Value)> {
    let body = serde_json::to_vec(body)?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(20)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| std::io::Error::other("missing HTTP response"))?;
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| std::io::Error::other("invalid status"))?;
    Ok((status, serde_json::from_str(body)?))
}

/// Scenario: A real engine receives the stack-overflow reproducer and a canonical edit of legacy HTTP compression.
/// Guarantees: Excessive edits preserve process/state; all Admin scopes and read-modify-PUT preserve credentials.
#[test]
fn sparse_edits_are_bounded_and_management_reads_are_replayable() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let config = json!({
        "version": "otel_dataflow/v1",
        "engine": {"http_admin": {"bind_address": address.to_string()}},
        "groups": {"default": {"pipelines": {"main": {
            "policies": {"resources": {"core_allocation": {"type": "core_count", "count": 1}}},
            "nodes": {
                "receiver": {"type": "receiver:otlp", "config": {"protocols": {"grpc": {"listening_addr": "127.0.0.1:0"}}}},
                "batch": {"type": "processor:batch"},
                "grpc": {"type": "exporter:otlp_grpc", "config": {"grpc_endpoint": "http://user:private-password@127.0.0.1:9"}},
                "otap": {"type": "exporter:otap", "config": {"grpc_endpoint": "http://127.0.0.1:9/private-path?token=private-query"}},
                "http": {"type": "exporter:otlp_http", "config": {"endpoint": "http://127.0.0.1:9", "client_pool_size": 1, "http": {"compression_method": "gzip", "headers": {"authorization": "private-header"}}}}
            },
            "connections": [{"from": "receiver", "to": "batch"}, {"from": "batch", "to": ["grpc", "otap", "http"]}]
        }}}}
    });
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut engine = Engine(
        Command::new(env!("CARGO_BIN_EXE_df_engine"))
            .arg("--config")
            .arg(&path)
            .args(["--num-cores", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let resource = "/api/v1/groups/default/pipelines/main";
    let before = loop {
        if let Ok((200, details)) = http(address, "GET", resource, &Value::Null) {
            break details;
        }
        assert!(
            engine.0.try_wait().unwrap().is_none(),
            "engine exited during startup"
        );
        assert!(Instant::now() < deadline, "admin startup timed out");
        std::thread::sleep(Duration::from_millis(20));
    };
    for route in ["/api/v1/config", "/api/v1/groups/default", resource] {
        let (status, body) = http(address, "GET", route, &Value::Null).unwrap();
        assert_eq!(status, 200);
        for secret in ["private-password", "private-query", "private-header"] {
            assert!(body.to_string().contains(secret));
        }
        assert!(!body.to_string().contains("[REDACTED]"));
        assert!(!body.to_string().contains("[OMITTED]"));
    }
    assert_eq!(
        before["pipeline"]["nodes"]["grpc"]["config"]["grpc_endpoint"],
        "http://user:private-password@127.0.0.1:9"
    );
    assert_eq!(
        before["pipeline"]["nodes"]["otap"]["config"]["grpc_endpoint"],
        "http://127.0.0.1:9/private-path?token=private-query"
    );
    let mut deep_path = vec!["nodes", "batch", "config"];
    deep_path.extend(std::iter::repeat_n("nested", 20_000));
    let request = json!({"expectedRevision": before["configRevision"], "changes": [{"operation": "set", "path": deep_path, "value": 1}]});
    assert_eq!(http(address, "PATCH", resource, &request).unwrap().0, 422);
    assert!(engine.0.try_wait().unwrap().is_none());
    assert_eq!(
        http(address, "GET", resource, &Value::Null).unwrap().1,
        before
    );
    let request = json!({"expectedRevision": before["configRevision"], "stepTimeoutSecs": 5, "drainTimeoutSecs": 5,
        "changes": [{"operation": "set", "path": ["nodes", "http", "config", "http", "compression_method"], "value": "gzip"}]});
    assert_eq!(http(address, "PATCH", resource, &request).unwrap().0, 200);
    let authored = http(address, "GET", resource, &Value::Null).unwrap().1;
    assert_eq!(
        authored["pipeline"]["nodes"]["http"]["config"]["http"]["compression_method"],
        "gzip"
    );
    assert!(
        authored["pipeline"]["nodes"]["http"]["config"]["http"]
            .get("compression")
            .is_none()
    );
    let request = json!({"expectedRevision": authored["configRevision"], "stepTimeoutSecs": 5, "drainTimeoutSecs": 5,
        "changes": [{"operation": "set", "path": ["nodes", "http", "config", "http", "compression"], "value": "deflate"}]});
    let (status, rollout) = http(
        address,
        "PATCH",
        &format!("{resource}?wait=true&timeout_secs=15"),
        &request,
    )
    .unwrap();
    assert_eq!(status, 200, "{rollout}");
    assert_eq!(rollout["state"], "succeeded");
    let after = http(address, "GET", resource, &Value::Null).unwrap().1;
    assert_eq!(
        after["pipeline"]["nodes"]["http"]["config"]["http"]["compression"],
        "deflate"
    );
    let mut replacement = after["pipeline"].clone();
    replacement["nodes"]["http"]["config"]["http"]["compression"] = "gzip".into();
    let request = json!({"pipeline": replacement, "stepTimeoutSecs": 5, "drainTimeoutSecs": 5});
    let (status, rollout) = http(
        address,
        "PUT",
        &format!("{resource}?wait=true&timeout_secs=15"),
        &request,
    )
    .unwrap();
    assert_eq!(status, 200, "{rollout}");
    assert_eq!(rollout["state"], "succeeded");
    assert_eq!(
        http(address, "GET", resource, &Value::Null).unwrap().1["pipeline"],
        request["pipeline"]
    );
}
