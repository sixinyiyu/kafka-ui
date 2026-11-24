//! Kafka 业务层。
//!
//! 与 UI 完全解耦：所有操作都是普通 `async fn`，接收 `&Client` 或配置结构体，
//! 返回可序列化的 DTO。UI 层负责在后台线程调用并回传结果。
//!
//! 客户端用法参考 kafka-proxy（`kafka_client` crate）：
//! - `Client::builder(bootstrap).with_sasl(..)/.with_kerberos(..).build()`
//! - `client.admin()` 管理 topic / group / offset
//! - `client.consumer(..).assign(topic, partitions) + seek(..) + poll_timeout(..)` 拉取消息
//! - `client.producer(..).send(ProducerRecord)` 发送消息

pub mod client;
pub mod consumer;
pub mod messages;
pub mod producer;
pub mod topics;

pub use client::{build_client, ClusterConfig, ConnectionTest, SecurityProtocolKind};
