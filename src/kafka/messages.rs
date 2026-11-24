//! 拉取 Topic 最近的 N 条消息。
//!
//! 思路与 kafka-proxy 的 `latest_messages` 一致：
//! 1. `ListOffsets` 拿到各分区最新 offset（LEO）；
//! 2. 计算每个分区的起点 `start = max(0, LEO - N)`；
//! 3. 以「直连模式消费者」`assign` 全部分区 + `seek` 到起点；
//! 4. `poll_timeout` 收集，按分区裁剪到最近 N 条后再合并排序。

use anyhow::{Context, Result};
use bytes::Bytes;
use kafka_client::{AutoOffsetReset, Client, ConsumerConfig, ConsumerRecord};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use super::topics::log_end_offsets;

/// 单条消息（可直接序列化展示）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageView {
    pub partition: i32,
    pub offset: i64,
    /// 毫秒时间戳：`timestamp_type` 为 CreateTime 时为消息创建时间，
    /// 否则为 broker 写入时间。0 表示未知。
    pub timestamp: i64,
    pub key: Option<String>,
    pub key_is_binary: bool,
    pub value: String,
    pub value_is_binary: bool,
    /// 值的字节长度。
    pub value_len: usize,
    pub headers: Vec<(String, String)>,
}

impl MessageView {
    /// 时间戳格式化（本地时区 `yyyy-mm-dd HH:MM:SS`）。
    pub fn timestamp_text(&self) -> String {
        if self.timestamp <= 0 {
            return "—".to_string();
        }
        match chrono::DateTime::from_timestamp_millis(self.timestamp) {
            Some(dt) => dt
                .with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            None => "—".to_string(),
        }
    }

    /// 用于表格一行展示的单行摘要。
    pub fn one_line_value(&self) -> String {
        let s = self.value.replace(['\r', '\n', '\t'], " ");
        if s.chars().count() > 200 {
            let head: String = s.chars().take(200).collect();
            format!("{head}…")
        } else {
            s
        }
    }
}

/// 拉取结果。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FetchResult {
    pub topic: String,
    pub messages: Vec<MessageView>,
    /// 每个分区的最新 offset。
    pub ends: Vec<(i32, i64)>,
    pub elapsed_ms: u128,
    /// 是否因超时提前结束（消息可能不全）。
    pub truncated: bool,
}

/// 拉取 `topic` 各分区最近 `count` 条消息，合并后按 offset 升序返回。
pub async fn fetch_latest(
    client: &Client,
    topic: &str,
    count: usize,
    timeout: Duration,
) -> Result<FetchResult> {
    let start = Instant::now();
    let count = count.clamp(1, 2000);
    let admin = client.admin();
    admin.refresh_metadata().await.ok();

    let ends = log_end_offsets(client, topic)
        .await
        .with_context(|| format!("查询分区位点失败: {}", topic))?;

    // 过滤空分区，计算每个分区的读取起点
    let mut windows: Vec<(i32, i64, i64)> = ends
        .iter()
        .filter(|(_, end)| *end > 0)
        .map(|(p, end)| (*p, (*end - count as i64).max(0), *end))
        .collect();
    windows.sort_by_key(|(p, _, _)| *p);

    if windows.is_empty() {
        return Ok(FetchResult {
            topic: topic.to_string(),
            messages: Vec::new(),
            ends,
            elapsed_ms: start.elapsed().as_millis(),
            truncated: false,
        });
    }

    let cfg = ConsumerConfig::new()
        .with_auto_commit(false)
        .with_auto_offset_reset(AutoOffsetReset::Earliest)
        .with_max_poll_records(count * windows.len() + 64)
        .with_partition_max_bytes(8 * 1024 * 1024)
        .with_max_wait(Duration::from_millis(800));

    let mut consumer = client.consumer(cfg);
    let partitions: Vec<i32> = windows.iter().map(|(p, _, _)| *p).collect();
    consumer
        .assign(topic.to_string(), partitions)
        .await
        .context("分配分区失败")?;
    for (p, start_off, _) in &windows {
        consumer
            .seek(topic.to_string(), *p, *start_off)
            .await
            .with_context(|| format!("定位分区 {} 起点失败", p))?;
    }
    // `seek` 是投递到消费者线程的异步命令，不等待回执；给消费者一点时间落地，
    // 否则紧随其后的 poll 可能仍从 assign 默认位点（latest）开始读，导致漏消息。
    tokio::time::sleep(Duration::from_millis(150)).await;

    let want_total: usize = windows
        .iter()
        .map(|(_, s, e)| (e - s) as usize)
        .sum::<usize>()
        .min(count * windows.len());
    let mut collected: Vec<ConsumerRecord> = Vec::with_capacity(want_total);
    let deadline = Instant::now() + timeout;
    let mut truncated = false;

    while collected.len() < want_total && Instant::now() < deadline {
        let remain = deadline.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            break;
        }
        let batch = consumer
            .poll_timeout(remain.min(Duration::from_secs(2)))
            .await
            .context("拉取消息失败")?;
        if batch.is_empty() {
            if windows_done(&collected, &windows) {
                break;
            }
            continue;
        }
        for r in batch {
            if let Some((_, s, e)) = windows.iter().find(|(p, _, _)| *p == r.partition) {
                if r.offset >= *s && r.offset < *e {
                    collected.push(r);
                }
            }
        }
        if windows_done(&collected, &windows) {
            break;
        }
    }
    if collected.len() < want_total && Instant::now() >= deadline {
        truncated = true;
    }
    let _ = consumer.close().await;

    let messages = select_recent(collected, &windows, count);
    Ok(FetchResult {
        topic: topic.to_string(),
        messages,
        ends,
        elapsed_ms: start.elapsed().as_millis(),
        truncated,
    })
}

/// 每个分区是否已读满（已达到该分区可用的总条数）。
fn windows_done(collected: &[ConsumerRecord], windows: &[(i32, i64, i64)]) -> bool {
    windows.iter().all(|(p, s, e)| {
        let have = collected.iter().filter(|r| r.partition == *p).count() as i64;
        let avail = e - s;
        have >= avail
    })
}

/// 按分区取最近 `count` 条，再整体按 (partition, offset) 排序。
fn select_recent(
    collected: Vec<ConsumerRecord>,
    windows: &[(i32, i64, i64)],
    count: usize,
) -> Vec<MessageView> {
    use std::collections::HashMap;

    let mut by_part: HashMap<i32, Vec<ConsumerRecord>> = HashMap::new();
    for r in collected {
        by_part.entry(r.partition).or_default().push(r);
    }

    let mut out: Vec<MessageView> = Vec::new();
    for (p, s, e) in windows {
        let mut recs = by_part.remove(p).unwrap_or_default();
        recs.retain(|r| r.offset >= *s && r.offset < *e);
        recs.sort_by_key(|r| r.offset);
        let take = recs.len().min(count);
        for r in recs.split_off(recs.len() - take) {
            out.push(to_view(r));
        }
    }

    // 按时间戳（若有效）再按分区/offset 排序，让「最近」语义更直观
    out.sort_by(|a, b| {
        let ta = if a.timestamp > 0 {
            a.timestamp
        } else {
            i64::MAX
        };
        let tb = if b.timestamp > 0 {
            b.timestamp
        } else {
            i64::MAX
        };
        ta.cmp(&tb)
            .then(a.partition.cmp(&b.partition))
            .then(a.offset.cmp(&b.offset))
    });
    out
}

fn to_view(r: ConsumerRecord) -> MessageView {
    let (key, key_is_binary) = match &r.key {
        Some(k) => {
            let (s, bin) = bytes_to_text(k);
            (Some(s), bin)
        }
        None => (None, false),
    };
    let (value, value_is_binary) = bytes_to_text(&r.value);
    let headers = r
        .headers
        .iter()
        .map(|h| {
            let (s, _) = bytes_to_text(&h.value);
            (h.key.clone(), s)
        })
        .collect();
    MessageView {
        partition: r.partition,
        offset: r.offset,
        timestamp: r.timestamp,
        key,
        key_is_binary,
        value_len: r.value.len(),
        value,
        value_is_binary,
        headers,
    }
}

/// UTF-8 可解码则原样返回，否则返回 `0x` 十六进制。第二项表示是否为二进制。
pub fn bytes_to_text(b: &Bytes) -> (String, bool) {
    match std::str::from_utf8(b) {
        Ok(s) => (s.to_string(), false),
        Err(_) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            (format!("0x{hex}"), true)
        }
    }
}

/// 字节长度的人类可读格式。
pub fn human_bytes(n: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.1} {}", v, UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_value_is_hexed() {
        let (s, bin) = bytes_to_text(&Bytes::from_static(&[0xff, 0x00]));
        assert!(bin);
        assert_eq!(s, "0xff00");
    }

    #[test]
    fn utf8_value_is_text() {
        let (s, bin) = bytes_to_text(&Bytes::from_static(b"hello"));
        assert!(!bin);
        assert_eq!(s, "hello");
    }

    #[test]
    fn human_bytes_scales() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KB");
    }
}
