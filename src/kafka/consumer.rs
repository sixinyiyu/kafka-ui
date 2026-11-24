//! 消费组 / 消费情况查询。
//!
//! 参考 kafka-proxy 的 `list_consumers`：
//! `list_groups` → 过滤 consumer 协议 → `describe_groups` 拿成员 →
//! `fetch_group_offsets` 拿提交位点与 lag。

use std::time::Duration;

use anyhow::{Context, Result};
use kafka_client::admin::AdminClient;
use serde::{Deserialize, Serialize};

/// 消费组成员。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupMemberView {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
}

/// 消费组在单个分区上的位点。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupPartitionView {
    pub partition: i32,
    pub committed_offset: i64,
    pub log_end_offset: i64,
    pub lag: i64,
}

/// 某个消费组对某个 topic 的消费情况。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupView {
    pub group: String,
    pub state: String,
    pub protocol_type: String,
    pub members: Vec<GroupMemberView>,
    pub partitions: Vec<GroupPartitionView>,
    pub total_lag: i64,
}

impl GroupView {
    pub fn total_committed(&self) -> i64 {
        self.partitions
            .iter()
            .filter(|p| p.committed_offset >= 0)
            .map(|p| p.committed_offset)
            .sum()
    }

    pub fn status_label(&self) -> &'static str {
        if self.total_lag <= 0 {
            "已消费完"
        } else if self.members.is_empty() || self.state.eq_ignore_ascii_case("Empty") {
            "无活跃消费者"
        } else {
            "消费落后"
        }
    }
}

/// 列出所有消费组（不带 topic 过滤）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupSummaryView {
    pub group: String,
    pub state: String,
    pub protocol_type: String,
}

/// 列出集群中的消费组。
pub async fn list_groups(admin: &AdminClient) -> Result<Vec<GroupSummaryView>> {
    let mut groups: Vec<GroupSummaryView> = admin
        .list_groups()
        .await
        .context("拉取消费组列表失败")?
        .into_iter()
        .filter(|g| g.protocol_type == "consumer" || g.protocol_type.is_empty())
        .map(|g| GroupSummaryView {
            group: g.group_id,
            state: g.state,
            protocol_type: g.protocol_type,
        })
        .collect();
    groups.sort_by(|a, b| a.group.cmp(&b.group));
    Ok(groups)
}

/// 查询某个 topic 的消费情况（哪些消费组在消费、进度如何）。
///
/// `topic_filter` 为 Some 时仅返回涉及该 topic 的分区。
pub async fn topic_consumers(
    admin: &AdminClient,
    topic_filter: Option<&str>,
) -> Result<Vec<GroupView>> {
    let groups = admin.list_groups().await.context("拉取消费组列表失败")?;
    let consumer_groups: Vec<String> = groups
        .into_iter()
        .filter(|g| g.protocol_type == "consumer" || g.protocol_type.is_empty())
        .map(|g| g.group_id)
        .collect();

    if consumer_groups.is_empty() {
        return Ok(Vec::new());
    }

    // 成员信息
    let ids: Vec<&str> = consumer_groups.iter().map(|s| s.as_str()).collect();
    let descriptions = admin
        .describe_groups(&ids)
        .await
        .context("拉取消费组成员失败")?;
    let desc_by_id: std::collections::HashMap<&str, &kafka_client::admin::AdminGroupDescription> =
        descriptions.iter().map(|d| (d.group_id.as_str(), d)).collect();

    // 并发拉取各组的已提交位点（5s 超时保护），与 kafka-proxy 保持一致
    let fetches = consumer_groups.iter().map(|gid| async move {
        match tokio::time::timeout(Duration::from_secs(5), admin.fetch_group_offsets(gid)).await {
            Ok(Ok(offsets)) => Some((gid.clone(), offsets)),
            Ok(Err(e)) => {
                tracing::warn!(group = %gid, "fetch_group_offsets 失败: {e}");
                None
            }
            Err(_) => {
                tracing::warn!(group = %gid, "fetch_group_offsets 超时(5s)");
                None
            }
        }
    });
    let results = tokio::time::timeout(
        Duration::from_secs(15),
        futures::future::join_all(fetches),
    )
    .await
    .unwrap_or_default();

    let mut views: Vec<GroupView> = Vec::new();
    for item in results.into_iter().flatten() {
        let (gid, offsets) = item;
        let topic_offsets: Vec<_> = offsets
            .into_iter()
            .filter(|o| topic_filter.map(|t| o.topic == t).unwrap_or(true))
            .collect();
        if topic_offsets.is_empty() {
            continue;
        }
        let desc = desc_by_id.get(gid.as_str());
        let members = desc
            .map(|d| {
                d.members
                    .iter()
                    .map(|m| GroupMemberView {
                        member_id: m.member_id.clone(),
                        client_id: m.client_id.clone(),
                        client_host: m.client_host.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut partitions: Vec<GroupPartitionView> = topic_offsets
            .iter()
            .map(|o| GroupPartitionView {
                partition: o.partition,
                committed_offset: o.committed_offset,
                log_end_offset: o.log_end_offset,
                lag: o.lag,
            })
            .collect();
        partitions.sort_by_key(|p| p.partition);
        let total_lag: i64 = partitions.iter().filter(|p| p.lag > 0).map(|p| p.lag).sum();
        views.push(GroupView {
            group: gid,
            state: desc.map(|d| d.state.clone()).unwrap_or_default(),
            protocol_type: desc.map(|d| d.protocol_type.clone()).unwrap_or_default(),
            members,
            partitions,
            total_lag,
        });
    }

    views.sort_by(|a, b| b.total_lag.cmp(&a.total_lag).then(a.group.cmp(&b.group)));
    Ok(views)
}

/// 删除消费组（用于清理残留）。
pub async fn delete_group(admin: &AdminClient, group: &str) -> Result<String> {
    admin
        .delete_group(group)
        .await
        .with_context(|| format!("删除消费组失败: {}", group))?;
    Ok(format!("消费组 '{}' 已删除", group))
}
