// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! In-memory topic registry for inter-pipeline communication.

use otap_df_config::PipelineGroupId;
use otap_df_config::PipelineId;
use otap_df_config::TopicName;
use otap_df_config::engine::EngineConfig;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::topic::{TopicConfig, TopicOverflowPolicy};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Type-erased handle for sharing a topic registry in non-generic contexts.
#[derive(Clone)]
pub struct TopicRegistryHandle {
    inner: Arc<dyn Any + Send + Sync>,
}

impl TopicRegistryHandle {
    /// Wrap a concrete registry into a type-erased handle.
    pub fn new<T: Any + Send + Sync>(registry: Arc<T>) -> Self {
        Self { inner: registry }
    }

    /// Downcast to the concrete registry type.
    #[must_use]
    pub fn downcast<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::downcast(self.inner.clone()).ok()
    }
}

impl fmt::Debug for TopicRegistryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicRegistryHandle").finish()
    }
}

/// Topic scope used for resolution and isolation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TopicScope {
    /// Global topic visible to all pipeline groups.
    Global,
    /// Group-scoped topic visible within a pipeline group.
    Group {
        /// Pipeline group identifier for this topic scope.
        pipeline_group_id: PipelineGroupId,
    },
    /// Pipeline-scoped topic visible within a single pipeline.
    Pipeline {
        /// Pipeline group identifier for this topic scope.
        pipeline_group_id: PipelineGroupId,
        /// Pipeline identifier for this topic scope.
        pipeline_id: PipelineId,
    },
}

/// Fully-qualified topic key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicKey {
    /// Scope of the topic.
    pub scope: TopicScope,
    /// Name of the topic.
    pub name: TopicName,
}

impl TopicKey {
    /// Create a global topic key.
    #[must_use]
    pub fn global(name: TopicName) -> Self {
        Self {
            scope: TopicScope::Global,
            name,
        }
    }

    /// Create a group-scoped topic key.
    #[must_use]
    pub fn group(pipeline_group_id: PipelineGroupId, name: TopicName) -> Self {
        Self {
            scope: TopicScope::Group { pipeline_group_id },
            name,
        }
    }

    /// Create a pipeline-scoped topic key.
    #[must_use]
    pub fn pipeline(
        pipeline_group_id: PipelineGroupId,
        pipeline_id: PipelineId,
        name: TopicName,
    ) -> Self {
        Self {
            scope: TopicScope::Pipeline {
                pipeline_group_id,
                pipeline_id,
            },
            name,
        }
    }
}

impl fmt::Display for TopicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.scope {
            TopicScope::Global => write!(f, "global:{}", self.name),
            TopicScope::Group { pipeline_group_id } => {
                write!(f, "group:{}:{}", pipeline_group_id, self.name)
            }
            TopicScope::Pipeline {
                pipeline_group_id,
                pipeline_id,
            } => write!(
                f,
                "pipeline:{}:{}:{}",
                pipeline_group_id, pipeline_id, self.name
            ),
        }
    }
}

struct TopicEntry<PData> {
    config: TopicConfig,
    groups: RwLock<HashMap<String, Arc<TopicGroup<PData>>>>,
}

impl<PData> TopicEntry<PData> {
    fn new(config: TopicConfig) -> Self {
        Self {
            config,
            groups: RwLock::new(HashMap::new()),
        }
    }
}

struct TopicGroup<PData> {
    sender: flume::Sender<PData>,
    receiver: flume::Receiver<PData>,
    subscribers: AtomicUsize,
}

impl<PData> TopicGroup<PData> {
    fn new(capacity: usize) -> Result<Self, ConfigError> {
        if capacity == 0 {
            return Err(ConfigError::InvalidUserConfig {
                error: "topic buffer capacity must be greater than zero".to_string(),
            });
        }

        let (sender, receiver) = flume::bounded(capacity);
        Ok(Self {
            sender,
            receiver,
            subscribers: AtomicUsize::new(0),
        })
    }

    fn has_subscribers(&self) -> bool {
        self.subscribers.load(Ordering::Acquire) > 0
    }
}

/// A balanced subscription to a topic group.
pub struct TopicSubscription<PData> {
    receiver: flume::Receiver<PData>,
    group: Arc<TopicGroup<PData>>,
}

impl<PData> TopicSubscription<PData> {
    /// Receive the next message from the topic group.
    pub async fn recv(&self) -> Result<PData, flume::RecvError> {
        self.receiver.recv_async().await
    }

    /// Try to receive the next message without awaiting.
    pub fn try_recv(&self) -> Result<PData, flume::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl<PData> Drop for TopicSubscription<PData> {
    fn drop(&mut self) {
        let _ = self.group.subscribers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Errors returned when publishing to a topic.
#[derive(Debug, thiserror::Error)]
pub enum TopicPublishError {
    /// Topic buffer is full for an error overflow policy.
    #[error("topic buffer full for {topic} (group '{group}')")]
    BufferFull {
        /// Topic identifier.
        topic: TopicKey,
        /// Balanced group identifier.
        group: String,
    },
    /// Topic channel is disconnected.
    #[error("topic channel disconnected for {topic} (group '{group}')")]
    Disconnected {
        /// Topic identifier.
        topic: TopicKey,
        /// Balanced group identifier.
        group: String,
    },
}

/// Registry of topic definitions and runtime groups.
pub struct TopicRegistry<PData> {
    topics: RwLock<HashMap<TopicKey, Arc<TopicEntry<PData>>>>,
}

impl<PData: Clone + Send + Sync + 'static> TopicRegistry<PData> {
    /// Build a registry from an engine configuration.
    pub fn from_engine_config(config: &EngineConfig) -> Result<Self, ConfigError> {
        let mut topics = HashMap::new();

        for (name, topic) in &config.topics {
            validate_topic_config(topic)?;
            let key = TopicKey::global(name.clone());
            let _ = topics.insert(key, Arc::new(TopicEntry::new(topic.clone())));
        }

        for (group_id, group) in &config.pipeline_groups {
            for (name, topic) in &group.topics {
                validate_topic_config(topic)?;
                let key = TopicKey::group(group_id.clone(), name.clone());
                let _ = topics.insert(key, Arc::new(TopicEntry::new(topic.clone())));
            }

            for (pipeline_id, pipeline) in &group.pipelines {
                for (name, topic) in pipeline.topic_iter() {
                    validate_topic_config(topic)?;
                    let key =
                        TopicKey::pipeline(group_id.clone(), pipeline_id.clone(), name.clone());
                    let _ = topics.insert(key, Arc::new(TopicEntry::new(topic.clone())));
                }
            }
        }

        Ok(Self {
            topics: RwLock::new(topics),
        })
    }

    /// Resolve a topic name using pipeline, group, and global scopes.
    pub fn resolve(
        &self,
        pipeline_group_id: &PipelineGroupId,
        pipeline_id: &PipelineId,
        name: &str,
    ) -> Result<TopicKey, ConfigError> {
        let name: TopicName = name.to_string().into();
        let pipeline_key =
            TopicKey::pipeline(pipeline_group_id.clone(), pipeline_id.clone(), name.clone());
        let group_key = TopicKey::group(pipeline_group_id.clone(), name.clone());
        let global_key = TopicKey::global(name.clone());

        let topics = self.topics.read().expect("topic registry lock poisoned");
        if topics.contains_key(&pipeline_key) {
            return Ok(pipeline_key);
        }
        if topics.contains_key(&group_key) {
            return Ok(group_key);
        }
        if topics.contains_key(&global_key) {
            return Ok(global_key);
        }

        Err(ConfigError::InvalidUserConfig {
            error: format!(
                "unknown topic '{name}' for pipeline '{pipeline_group_id}:{pipeline_id}'"
            ),
        })
    }

    /// Subscribe to a topic group using balanced semantics.
    pub fn subscribe_balanced(
        &self,
        key: &TopicKey,
        group_id: &str,
    ) -> Result<TopicSubscription<PData>, ConfigError> {
        let entry = self.entry(key)?;
        let capacity = entry.config.policy.buffer.capacity;

        let mut groups = entry.groups.write().expect("topic group lock poisoned");
        let group = match groups.get(group_id) {
            Some(existing) => existing.clone(),
            None => {
                let group = Arc::new(TopicGroup::new(capacity)?);
                let _ = groups.insert(group_id.to_string(), group.clone());
                group
            }
        };

        let _ = group.subscribers.fetch_add(1, Ordering::AcqRel);
        Ok(TopicSubscription {
            receiver: group.receiver.clone(),
            group,
        })
    }

    /// Publish data to all active balanced groups for this topic.
    pub async fn publish(&self, key: &TopicKey, data: PData) -> Result<(), TopicPublishError> {
        let entry = self
            .entry(key)
            .map_err(|_| TopicPublishError::Disconnected {
                topic: key.clone(),
                group: "<unknown>".to_string(),
            })?;
        let overflow = entry.config.policy.buffer.overflow.clone();

        let groups = {
            let groups = entry.groups.read().expect("topic group lock poisoned");
            groups
                .iter()
                .filter(|(_, group)| group.has_subscribers())
                .map(|(name, group)| (name.clone(), group.clone()))
                .collect::<Vec<_>>()
        };

        if groups.is_empty() {
            return Ok(());
        }

        let mut payload = Some(data);
        let mut iter = groups.into_iter().peekable();
        while let Some((group_name, group)) = iter.next() {
            let data = if iter.peek().is_some() {
                payload.as_ref().expect("payload missing").clone()
            } else {
                payload.take().expect("payload missing")
            };

            match &overflow {
                TopicOverflowPolicy::Block => {
                    group.sender.send_async(data).await.map_err(|_| {
                        TopicPublishError::Disconnected {
                            topic: key.clone(),
                            group: group_name.clone(),
                        }
                    })?;
                }
                TopicOverflowPolicy::DropNewest => {
                    if let Err(err) = group.sender.try_send(data) {
                        if matches!(err, flume::TrySendError::Disconnected(_)) {
                            return Err(TopicPublishError::Disconnected {
                                topic: key.clone(),
                                group: group_name.clone(),
                            });
                        }
                    }
                }
                TopicOverflowPolicy::DropOldest => match group.sender.try_send(data) {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(data)) => {
                        let _ = group.receiver.try_recv();
                        if let Err(err) = group.sender.try_send(data) {
                            if matches!(err, flume::TrySendError::Disconnected(_)) {
                                return Err(TopicPublishError::Disconnected {
                                    topic: key.clone(),
                                    group: group_name.clone(),
                                });
                            }
                        }
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        return Err(TopicPublishError::Disconnected {
                            topic: key.clone(),
                            group: group_name.clone(),
                        });
                    }
                },
                TopicOverflowPolicy::Error => {
                    if let Err(err) = group.sender.try_send(data) {
                        return match err {
                            flume::TrySendError::Full(_) => Err(TopicPublishError::BufferFull {
                                topic: key.clone(),
                                group: group_name.clone(),
                            }),
                            flume::TrySendError::Disconnected(_) => {
                                Err(TopicPublishError::Disconnected {
                                    topic: key.clone(),
                                    group: group_name.clone(),
                                })
                            }
                        };
                    }
                }
            }
        }

        Ok(())
    }

    fn entry(&self, key: &TopicKey) -> Result<Arc<TopicEntry<PData>>, ConfigError> {
        let topics = self.topics.read().expect("topic registry lock poisoned");
        topics
            .get(key)
            .cloned()
            .ok_or_else(|| ConfigError::InvalidUserConfig {
                error: format!("topic '{}' is not declared", key),
            })
    }
}

fn validate_topic_config(topic: &TopicConfig) -> Result<(), ConfigError> {
    if topic.policy.buffer.capacity == 0 {
        return Err(ConfigError::InvalidUserConfig {
            error: "topic buffer capacity must be greater than zero".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_config::engine::{EngineConfig, EngineSettings};
    use otap_df_config::pipeline::{PipelineConfig, PipelineConfigBuilder, PipelineType};
    use otap_df_config::pipeline_group::PipelineGroupConfig;
    use otap_df_config::topic::{TopicBufferPolicy, TopicConfig, TopicOverflowPolicy, TopicPolicy};
    use otap_df_config::{PipelineGroupId, PipelineId};
    use std::collections::HashSet;

    #[tokio::test]
    async fn balanced_topic_delivers_once() {
        let pipeline_group_id: PipelineGroupId = "test-group".into();
        let pipeline_id: PipelineId = "test-pipeline".into();
        let pipeline = PipelineConfigBuilder::new()
            .build(
                PipelineType::Otap,
                pipeline_group_id.clone(),
                pipeline_id.clone(),
            )
            .expect("pipeline config");

        let mut group = PipelineGroupConfig::new();
        group
            .add_pipeline(pipeline_id.clone(), pipeline)
            .expect("pipeline insert");

        let policy = TopicPolicy {
            buffer: TopicBufferPolicy {
                capacity: 4,
                overflow: TopicOverflowPolicy::Block,
            },
            ..TopicPolicy::default()
        };

        let mut engine_config = EngineConfig {
            settings: EngineSettings::default(),
            topics: HashMap::new(),
            pipeline_groups: HashMap::new(),
        };
        let _ = engine_config.topics.insert(
            "shared".into(),
            TopicConfig {
                description: None,
                policy,
            },
        );
        let _ = engine_config
            .pipeline_groups
            .insert(pipeline_group_id.clone(), group);

        let registry = TopicRegistry::<u32>::from_engine_config(&engine_config).expect("registry");
        let key = registry
            .resolve(&pipeline_group_id, &pipeline_id, "shared")
            .expect("topic resolve");
        let sub_a = registry
            .subscribe_balanced(&key, "workers")
            .expect("subscription a");
        let sub_b = registry
            .subscribe_balanced(&key, "workers")
            .expect("subscription b");

        registry.publish(&key, 10).await.expect("publish 1");
        registry.publish(&key, 20).await.expect("publish 2");

        let first = sub_a.recv().await.expect("recv 1");
        let second = sub_a.recv().await.expect("recv 2");

        let mut values = vec![first, second];
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
        assert!(matches!(sub_b.try_recv(), Err(flume::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn balanced_topic_delivers_each_message_once() {
        // Two balanced subscribers should collectively see each message exactly once.
        let pipeline_group_id: PipelineGroupId = "test-group".into();
        let pipeline_id: PipelineId = "test-pipeline".into();
        let pipeline = PipelineConfigBuilder::new()
            .build(
                PipelineType::Otap,
                pipeline_group_id.clone(),
                pipeline_id.clone(),
            )
            .expect("pipeline config");

        let mut group = PipelineGroupConfig::new();
        group
            .add_pipeline(pipeline_id.clone(), pipeline)
            .expect("pipeline insert");

        let policy = TopicPolicy {
            buffer: TopicBufferPolicy {
                capacity: 128,
                overflow: TopicOverflowPolicy::Block,
            },
            ..TopicPolicy::default()
        };

        let mut engine_config = EngineConfig {
            settings: EngineSettings::default(),
            topics: HashMap::new(),
            pipeline_groups: HashMap::new(),
        };
        let _ = engine_config.topics.insert(
            "shared".into(),
            TopicConfig {
                description: None,
                policy,
            },
        );
        let _ = engine_config
            .pipeline_groups
            .insert(pipeline_group_id.clone(), group);

        let registry = TopicRegistry::<u32>::from_engine_config(&engine_config).expect("registry");
        let key = registry
            .resolve(&pipeline_group_id, &pipeline_id, "shared")
            .expect("topic resolve");
        let sub_a = registry
            .subscribe_balanced(&key, "workers")
            .expect("subscription a");
        let sub_b = registry
            .subscribe_balanced(&key, "workers")
            .expect("subscription b");

        let message_count = 100u32;
        for value in 0..message_count {
            registry.publish(&key, value).await.expect("publish");
        }

        let mut seen = HashSet::new();
        for _ in 0..message_count {
            tokio::select! {
                msg = sub_a.recv() => {
                    let value = msg.expect("recv a");
                    assert!(seen.insert(value), "duplicate message {value}");
                }
                msg = sub_b.recv() => {
                    let value = msg.expect("recv b");
                    assert!(seen.insert(value), "duplicate message {value}");
                }
            }
        }
        assert_eq!(seen.len(), message_count as usize);
        assert!(matches!(sub_a.try_recv(), Err(flume::TryRecvError::Empty)));
        assert!(matches!(sub_b.try_recv(), Err(flume::TryRecvError::Empty)));
    }

    #[test]
    fn topic_resolution_prefers_pipeline_over_group_over_global() {
        // Resolution order: pipeline-scoped overrides group overrides global.
        let pipeline_group_id: PipelineGroupId = "test-group".into();
        let pipeline_id: PipelineId = "test-pipeline".into();

        let pipeline_yaml = r#"
topics:
  shared:
    policy:
      buffer:
        capacity: 3
"#;
        let pipeline = PipelineConfig::from_yaml(
            pipeline_group_id.clone(),
            pipeline_id.clone(),
            pipeline_yaml,
        )
        .expect("pipeline config");

        let mut group = PipelineGroupConfig::new();
        let _ = group.topics.insert(
            "shared".into(),
            TopicConfig {
                description: None,
                policy: TopicPolicy {
                    buffer: TopicBufferPolicy {
                        capacity: 2,
                        overflow: TopicOverflowPolicy::Block,
                    },
                    ..TopicPolicy::default()
                },
            },
        );
        let _ = group.topics.insert(
            "group_only".into(),
            TopicConfig {
                description: None,
                policy: TopicPolicy {
                    buffer: TopicBufferPolicy {
                        capacity: 2,
                        overflow: TopicOverflowPolicy::Block,
                    },
                    ..TopicPolicy::default()
                },
            },
        );
        group
            .add_pipeline(pipeline_id.clone(), pipeline)
            .expect("pipeline insert");

        let mut engine_config = EngineConfig {
            settings: EngineSettings::default(),
            topics: HashMap::new(),
            pipeline_groups: HashMap::new(),
        };
        let _ = engine_config.topics.insert(
            "shared".into(),
            TopicConfig {
                description: None,
                policy: TopicPolicy {
                    buffer: TopicBufferPolicy {
                        capacity: 1,
                        overflow: TopicOverflowPolicy::Block,
                    },
                    ..TopicPolicy::default()
                },
            },
        );
        let _ = engine_config.topics.insert(
            "global_only".into(),
            TopicConfig {
                description: None,
                policy: TopicPolicy {
                    buffer: TopicBufferPolicy {
                        capacity: 1,
                        overflow: TopicOverflowPolicy::Block,
                    },
                    ..TopicPolicy::default()
                },
            },
        );
        let _ = engine_config
            .pipeline_groups
            .insert(pipeline_group_id.clone(), group);

        let registry = TopicRegistry::<u32>::from_engine_config(&engine_config).expect("registry");

        let shared_key = registry
            .resolve(&pipeline_group_id, &pipeline_id, "shared")
            .expect("resolve shared");
        assert!(matches!(
            shared_key.scope,
            TopicScope::Pipeline {
                pipeline_group_id: ref group_id,
                pipeline_id: ref pipe_id
            } if group_id == &pipeline_group_id && pipe_id == &pipeline_id
        ));

        let group_key = registry
            .resolve(&pipeline_group_id, &pipeline_id, "group_only")
            .expect("resolve group");
        assert!(matches!(
            group_key.scope,
            TopicScope::Group { pipeline_group_id: ref group_id } if group_id == &pipeline_group_id
        ));

        let global_key = registry
            .resolve(&pipeline_group_id, &pipeline_id, "global_only")
            .expect("resolve global");
        assert!(matches!(global_key.scope, TopicScope::Global));
    }

    #[test]
    fn topic_registry_rejects_zero_capacity() {
        // Buffer capacity must be > 0 at any scope.
        let pipeline_group_id: PipelineGroupId = "test-group".into();
        let pipeline_id: PipelineId = "test-pipeline".into();
        let pipeline = PipelineConfigBuilder::new()
            .build(
                PipelineType::Otap,
                pipeline_group_id.clone(),
                pipeline_id.clone(),
            )
            .expect("pipeline config");

        let mut group = PipelineGroupConfig::new();
        group
            .add_pipeline(pipeline_id.clone(), pipeline)
            .expect("pipeline insert");

        let policy = TopicPolicy {
            buffer: TopicBufferPolicy {
                capacity: 0,
                overflow: TopicOverflowPolicy::Block,
            },
            ..TopicPolicy::default()
        };

        let mut engine_config = EngineConfig {
            settings: EngineSettings::default(),
            topics: HashMap::new(),
            pipeline_groups: HashMap::new(),
        };
        let _ = engine_config.topics.insert(
            "invalid".into(),
            TopicConfig {
                description: None,
                policy,
            },
        );
        let _ = engine_config
            .pipeline_groups
            .insert(pipeline_group_id.clone(), group);

        let err = TopicRegistry::<u32>::from_engine_config(&engine_config)
            .err()
            .expect("expected invalid capacity");
        assert!(
            err.to_string()
                .contains("capacity must be greater than zero")
        );
    }
}
