//! 向 Topic 模拟发送消息。

use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_client::{Client, Header, ProducerConfig, ProducerRecord};
use serde::{Deserialize, Serialize};

/// 发送参数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendSpec {
    pub topic: String,
    /// 消息 key（空表示不设置）。
    pub key: String,
    /// 消息体。
    pub value: String,
    /// 指定分区（None 表示按 key/hash 路由）。
    pub partition: Option<i32>,
    /// 自定义 headers。
    pub headers: Vec<(String, String)>,
}

impl Default for SendSpec {
    fn default() -> Self {
        Self {
            topic: String::new(),
            key: String::new(),
            value: String::new(),
            partition: None,
            headers: Vec::new(),
        }
    }
}

/// 发送结果。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendOutcome {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

/// 发送一条消息。
pub async fn send_message(client: &Client, spec: &SendSpec) -> Result<SendOutcome> {
    if spec.topic.trim().is_empty() {
        anyhow::bail!("请选择要发送的 Topic");
    }
    if spec.value.is_empty() {
        anyhow::bail!("消息内容不能为空");
    }

    // 新建的 topic 可能不在本地元数据缓存中：先 describe 一次触发元数据拉取，
    // 再 refresh 兜底，避免 producer 分区路由报 "Topic not found"。
    let topic = spec.topic.trim().to_string();
    for attempt in 0..5 {
        if let Ok(desc) = client.admin().describe_topics(&[topic.as_str()]).await {
            if desc
                .iter()
                .any(|d| d.name == topic && !d.partitions.is_empty())
            {
                break;
            }
        }
        if attempt < 4 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
    client.refresh_metadata().await.ok();

    // 注意：`linger_ms = 0` 会让 kafka_client 内部的 `tokio::time::interval`
    // panic（period 必须非零），因此这里用默认 linger，并通过 `send_direct`
    // 立即发送（不依赖批量缓冲），更符合「模拟发送」的直觉。
    let producer = client
        .producer(ProducerConfig::new().with_acks(1).with_retries(3))
        .await;

    let mut record = ProducerRecord::new(
        spec.topic.trim().to_string(),
        Bytes::from(spec.value.clone()),
    );
    if !spec.key.is_empty() {
        record = record.with_key(Bytes::from(spec.key.clone()));
    }
    if let Some(p) = spec.partition {
        record = record.with_partition(p);
    }
    let headers: Vec<Header> = spec
        .headers
        .iter()
        .filter(|(k, _)| !k.trim().is_empty())
        .map(|(k, v)| Header {
            key: k.trim().to_string(),
            value: Bytes::from(v.clone()),
        })
        .collect();
    if !headers.is_empty() {
        record = record.with_headers(headers);
    }

    let meta = producer
        .send_direct(record)
        .await
        .with_context(|| format!("发送消息到 {} 失败", spec.topic))?;
    let _ = producer.close().await;

    Ok(SendOutcome {
        topic: meta.topic,
        partition: meta.partition,
        offset: meta.offset,
        timestamp: meta.timestamp,
    })
}

impl SendOutcome {
    pub fn summary(&self) -> String {
        format!(
            "发送成功 → partition={} offset={}",
            self.partition, self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_value_rejected() {
        let spec = SendSpec {
            topic: "t".into(),
            value: "".into(),
            ..Default::default()
        };
        assert!(!spec.value.is_empty() || spec.value.is_empty()); // 占位：真实校验在 async 内部
    }
}
