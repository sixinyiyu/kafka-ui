//! 配置持久化：把 Kafka 集群连接信息保存到用户目录下的 YAML 文件。
//!
//! 路径优先级：
//! 1. 环境变量 `KAFKA_UI_CONFIG`
//! 2. `$XDG_CONFIG_HOME/kafka-ui/clusters.yaml`
//! 3. `~/.config/kafka-ui/clusters.yaml`

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::kafka::ClusterConfig;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Store {
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

impl Store {
    /// 配置文件路径。
    pub fn config_path() -> PathBuf {
        if let Ok(p) = std::env::var("KAFKA_UI_CONFIG") {
            if !p.trim().is_empty() {
                return PathBuf::from(p);
            }
        }
        if let Some(base) = dirs::config_dir() {
            return base.join("kafka-ui").join("clusters.yaml");
        }
        PathBuf::from("clusters.yaml")
    }

    /// 从文件加载；文件不存在时返回空配置。
    pub fn load(path: &Path) -> Result<Store> {
        if !path.exists() {
            return Ok(Store::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置失败: {}", path.display()))?;
        if text.trim().is_empty() {
            return Ok(Store::default());
        }
        serde_yaml::from_str(&text)
            .with_context(|| format!("解析配置失败（YAML 格式错误）: {}", path.display()))
    }

    /// 写回文件（先写临时文件再原子替换，避免半截文件）。
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("创建配置目录失败: {}", parent.display()))?;
        }
        let text = serde_yaml::to_string(self).context("序列化配置失败")?;
        let tmp = path.with_extension("yaml.tmp");
        std::fs::write(&tmp, text.as_bytes())
            .with_context(|| format!("写入临时配置失败: {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("替换配置文件失败: {}", path.display()))?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<&ClusterConfig> {
        self.clusters.iter().find(|c| c.id == id)
    }

    /// 是否存在同名集群（用于新增/重命名去重）。
    pub fn name_taken(&self, name: &str, except_id: Option<&str>) -> bool {
        let n = name.trim();
        self.clusters
            .iter()
            .any(|c| c.name.trim() == n && Some(c.id.as_str()) != except_id)
    }

    /// 新增或更新。返回 (是否为新增)。
    pub fn upsert(&mut self, cfg: ClusterConfig) -> bool {
        if let Some(existing) = self.clusters.iter_mut().find(|c| c.id == cfg.id) {
            *existing = cfg;
            false
        } else {
            self.clusters.push(cfg);
            true
        }
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.clusters.len();
        self.clusters.retain(|c| c.id != id);
        self.clusters.len() != before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_yaml() {
        let mut store = Store::default();
        let mut cfg = ClusterConfig::default();
        cfg.id = "abc".into();
        cfg.name = "prod".into();
        cfg.bootstrap_servers = "a:9092,b:9092".into();
        assert!(store.upsert(cfg.clone()));

        let dir = std::env::temp_dir().join(format!("kafka-ui-test-{}", uuid::Uuid::new_v4()));
        let path = dir.join("clusters.yaml");
        store.save(&path).unwrap();
        let loaded = Store::load(&path).unwrap();
        assert_eq!(loaded.clusters.len(), 1);
        assert_eq!(loaded.clusters[0].name, "prod");
        assert_eq!(loaded.clusters[0].bootstrap_list().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn upsert_updates_existing() {
        let mut store = Store::default();
        let mut cfg = ClusterConfig::default();
        cfg.id = "x".into();
        cfg.name = "a".into();
        store.upsert(cfg.clone());
        cfg.name = "b".into();
        assert!(!store.upsert(cfg));
        assert_eq!(store.clusters.len(), 1);
        assert_eq!(store.clusters[0].name, "b");
    }

    #[test]
    fn name_uniqueness() {
        let mut store = Store::default();
        let mut cfg = ClusterConfig::default();
        cfg.id = "1".into();
        cfg.name = "dev".into();
        store.upsert(cfg);
        assert!(store.name_taken("dev", None));
        assert!(!store.name_taken("dev", Some("1")));
        assert!(!store.name_taken("prod", None));
    }
}
