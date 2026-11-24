//! Topic 管理与分区元数据。

use anyhow::{Context, Result};
use kafka_client::admin::{AdminClient, NewTopic};
use kafka_client::Client;
use serde::{Deserialize, Serialize};

use super::client::BrokerInfo;

/// Topic 列表项。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partitions: usize,
}

impl TopicSummary {
    pub fn display_name(&self) -> &str {
        &self.name
    }
}

/// 单个分区的详情。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    /// 最新 offset（LEO），拉取失败时为 None。
    pub log_end_offset: Option<i64>,
}

/// Topic 详情。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicDetail {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionInfo>,
}

/// 创建 Topic 的参数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTopicSpec {
    pub name: String,
    /// 分区数（None 或 <=0 时使用 broker 默认）。
    pub num_partitions: Option<i32>,
    /// 副本因子（None 或 <=0 时使用 broker 默认）。
    pub replication_factor: Option<i16>,
    /// 预留的 topic 生命周期配置，如 `("cleanup.policy", "delete")`。
    pub configs: Vec<(String, String)>,
}

impl Default for CreateTopicSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            num_partitions: Some(3),
            replication_factor: Some(1),
            configs: Vec::new(),
        }
    }
}

/// 列出所有 topic，按名称排序。
pub async fn list_topics(admin: &AdminClient) -> Result<Vec<TopicSummary>> {
    let mut topics: Vec<TopicSummary> = admin
        .list_topics()
        .await
        .context("拉取 Topic 列表失败")?
        .into_iter()
        .map(|t| TopicSummary {
            name: t.name,
            internal: t.internal,
            partitions: t.partitions,
        })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}

/// 拉取某个 topic 的详情（含各分区 LEO）。
pub async fn describe_topic(client: &Client, name: &str) -> Result<TopicDetail> {
    let admin = client.admin();
    let descriptions = admin
        .describe_topics(&[name])
        .await
        .context("拉取 Topic 详情失败")?;
    let desc = descriptions
        .into_iter()
        .find(|d| d.name == name)
        .with_context(|| format!("Topic 不存在: {}", name))?;

    let ends = log_end_offsets(client, name).await.unwrap_or_default();
    let mut partitions: Vec<PartitionInfo> = desc
        .partitions
        .iter()
        .map(|p| PartitionInfo {
            partition: p.partition,
            leader: p.leader_id,
            replicas: p.replicas.clone(),
            isr: p.isr.clone(),
            log_end_offset: ends
                .iter()
                .find(|(idx, _)| *idx == p.partition)
                .map(|(_, off)| *off),
        })
        .collect();
    partitions.sort_by_key(|p| p.partition);

    Ok(TopicDetail {
        name: desc.name,
        internal: desc.internal,
        partitions,
    })
}

/// 查询 topic 各个分区的最新 offset（LEO）。
///
/// **重要**：`ListOffsets` 必须由分区 leader 处理；若把全部分区塞进一个
/// 「发往任意 broker」的请求，非该 broker 领导的分区会返回 NOT_LEADER 错误并被丢弃
/// （kafka-proxy 原实现即存在此问题，表现为多分区 topic 只能看到部分分区）。
/// 这里按 leader 分组，**每个 leader 一个请求并发执行**，再合并结果。
pub async fn log_end_offsets(client: &Client, topic: &str) -> Result<Vec<(i32, i64)>> {
    use kafka_client::protocol::{
        ListOffsetsPartition, ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopic,
    };
    use std::collections::BTreeMap;

    let admin = client.admin();
    let desc = admin.describe_topics(&[topic]).await?;
    let mut by_leader: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for td in &desc {
        for p in &td.partitions {
            if p.leader_id >= 0 {
                by_leader.entry(p.leader_id).or_default().push(p.partition);
            }
        }
    }
    if by_leader.is_empty() {
        return Ok(Vec::new());
    }

    // 拿到每个 leader 的地址（直连 leader 发送，`send_to_any_broker` 只会发到同一
    // 台 broker，跨 broker 的分区会返回 NOT_LEADER(6) 被丢弃）。
    let metadata = client.metadata();
    let mut targets: Vec<(std::net::SocketAddr, Vec<i32>)> = Vec::new();
    for (leader, partitions) in by_leader {
        match metadata.get_broker_address(leader).await {
            Some(addr) => targets.push((addr, partitions)),
            None => tracing::warn!(topic=%topic, leader, "未找到 leader 地址"),
        }
    }
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let futures: Vec<_> = targets
        .into_iter()
        .map(|(addr, partitions)| async move {
            let request = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: partitions
                        .iter()
                        .map(|&p| ListOffsetsPartition {
                            partition_index: p,
                            current_leader_epoch: -1,
                            timestamp: -1,
                        })
                        .collect(),
                }],
                timeout_ms: 5000,
            };
            send_to_broker::<_, ListOffsetsResponse>(&addr, &request).await
        })
        .collect();

    let responses = futures::future::join_all(futures).await;
    let mut ends: Vec<(i32, i64)> = Vec::new();
    let mut last_err = None;
    for resp in responses {
        match resp {
            Ok(resp) => {
                for t in &resp.topics {
                    if t.name != topic {
                        continue;
                    }
                    for rp in &t.partitions {
                        if rp.error_code == 0 {
                            ends.push((rp.partition_index, rp.offset));
                        } else {
                            tracing::warn!(topic=%topic, p=rp.partition_index, code=rp.error_code, "ListOffsets 分区错误");
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(topic=%topic, "ListOffsets 请求失败: {e}");
                last_err = Some(e);
            }
        }
    }
    tracing::debug!(topic=%topic, ?ends, "LEO 合并结果");
    ends.sort_by_key(|(p, _)| *p);
    ends.dedup_by_key(|(p, _)| *p);
    if let Some(e) = last_err {
        tracing::warn!(topic = %topic, "部分分区的 ListOffsets 失败: {e}");
    }
    Ok(ends)
}

/// 直连指定 broker（IP:PORT）发送一次请求。
///
/// kafka_client 的 `Client::send_to_any_broker` 只会把请求发给同一台 broker，
/// 而 `ListOffsets`、`Fetch` 等必须由分区 leader 处理。这里用低层
/// `connection::Builder::build_sequential` 建一条短连接，直接与 leader 通信。
async fn send_to_broker<Req, Resp>(addr: &std::net::SocketAddr, request: &Req) -> Result<Resp>
where
    Req: kafka_client::protocol_core::Request,
    Resp: kafka_client::protocol_core::Response,
{
    use kafka_client::connection::Builder;
    use kafka_client::transport::SecurityProtocol;

    let builder = Builder::new(
        *addr,
        SecurityProtocol::Plaintext,
        kafka_client::NAME.to_string(),
        kafka_client::VERSION.to_string(),
    )
    .with_client_id("kafka-ui".to_string())
    .with_request_timeout(std::time::Duration::from_secs(10));

    let mut conn = builder
        .build_sequential()
        .await
        .with_context(|| format!("连接 broker {} 失败", addr))?;
    conn.send_request(request)
        .await
        .with_context(|| format!("向 broker {} 发送请求失败", addr))
}

/// 创建 topic。返回 (是否已存在, 提示信息)。
pub async fn create_topic(admin: &AdminClient, spec: &CreateTopicSpec) -> Result<(bool, String)> {
    let name = spec.name.trim();
    if name.is_empty() {
        anyhow::bail!("Topic 名称不能为空");
    }
    let partitions = spec.num_partitions.unwrap_or(-1);
    let partitions = if partitions <= 0 { -1 } else { partitions };
    let replication = spec.replication_factor.unwrap_or(-1);
    let replication = if replication <= 0 { -1 } else { replication };

    let mut topic = NewTopic::new(name, partitions, replication);
    for (k, v) in &spec.configs {
        if !k.trim().is_empty() {
            topic = topic.with_config(k.trim(), v.trim());
        }
    }

    let result = admin
        .create_topic(&topic)
        .await
        .with_context(|| format!("创建 Topic 失败: {}", name))?;

    if result.already_exists() {
        return Ok((true, format!("Topic '{}' 已存在", name)));
    }
    if !result.is_success() {
        anyhow::bail!(
            "创建 Topic '{}' 失败: {}",
            name,
            result
                .error_message
                .unwrap_or_else(|| format!("error_code={:?}", result.error_code))
        );
    }
    Ok((
        false,
        format!(
            "Topic '{}' 创建成功（分区 {}，副本 {}）",
            name,
            if partitions < 0 {
                "默认".to_string()
            } else {
                partitions.to_string()
            },
            if replication < 0 {
                "默认".to_string()
            } else {
                replication.to_string()
            }
        ),
    ))
}

/// 删除 topic。
pub async fn delete_topic(admin: &AdminClient, name: &str) -> Result<String> {
    let result = admin
        .delete_topic(name)
        .await
        .with_context(|| format!("删除 Topic 失败: {}", name))?;
    if !result.is_success() {
        anyhow::bail!(
            "删除 Topic '{}' 失败: {}",
            name,
            result
                .error_message
                .unwrap_or_else(|| format!("error_code={:?}", result.error_code))
        );
    }
    Ok(format!("Topic '{}' 已删除", name))
}

/// 集群 broker 列表（用于展示）。
pub async fn list_brokers(admin: &AdminClient) -> Result<Vec<BrokerInfo>> {
    let cluster = admin.describe_cluster().await?;
    Ok(cluster
        .brokers
        .into_iter()
        .map(|b| BrokerInfo {
            id: b.id,
            host: b.host,
            port: b.port,
            is_controller: Some(b.id) == cluster.controller_id,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_spec_defaults() {
        let s = CreateTopicSpec::default();
        assert_eq!(s.num_partitions, Some(3));
        assert_eq!(s.replication_factor, Some(1));
    }
}
