//! Kafka 连接配置与客户端构建。
//!
//! 参考 kafka-proxy 的 `UpstreamAuth::apply_to_client_builder`：
//! 把持久化的连接配置（bootstrap / 安全协议 / SASL 机制 / Kerberos）翻译成
//! `kafka_client::ClientBuilder` 的链式调用。

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use kafka_client::{Client, KerberosCredentials, SaslMechanismType, TlsConfig};
use serde::{Deserialize, Serialize};

/// 安全协议。字符串取值对齐 Kafka 官方命名（前端/`server.properties` 通用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum SecurityProtocolKind {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocolKind {
    pub const ALL: [SecurityProtocolKind; 4] = [
        SecurityProtocolKind::Plaintext,
        SecurityProtocolKind::Ssl,
        SecurityProtocolKind::SaslPlaintext,
        SecurityProtocolKind::SaslSsl,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            SecurityProtocolKind::Plaintext => "PLAINTEXT",
            SecurityProtocolKind::Ssl => "SSL",
            SecurityProtocolKind::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocolKind::SaslSsl => "SASL_SSL",
        }
    }

    pub fn parse(s: &str) -> SecurityProtocolKind {
        match s.trim().to_ascii_uppercase().as_str() {
            "SSL" => SecurityProtocolKind::Ssl,
            "SASL_PLAINTEXT" => SecurityProtocolKind::SaslPlaintext,
            "SASL_SSL" | "SASL_SSL_TLS" => SecurityProtocolKind::SaslSsl,
            _ => SecurityProtocolKind::Plaintext,
        }
    }

    pub fn needs_sasl(&self) -> bool {
        matches!(
            self,
            SecurityProtocolKind::SaslPlaintext | SecurityProtocolKind::SaslSsl
        )
    }

    pub fn needs_tls(&self) -> bool {
        matches!(
            self,
            SecurityProtocolKind::Ssl | SecurityProtocolKind::SaslSsl
        )
    }
}

/// SASL 机制。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanismKind {
    #[default]
    None,
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
}

impl SaslMechanismKind {
    pub const ALL: [SaslMechanismKind; 5] = [
        SaslMechanismKind::None,
        SaslMechanismKind::Plain,
        SaslMechanismKind::ScramSha256,
        SaslMechanismKind::ScramSha512,
        SaslMechanismKind::Gssapi,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            SaslMechanismKind::None => "NONE",
            SaslMechanismKind::Plain => "PLAIN",
            SaslMechanismKind::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanismKind::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanismKind::Gssapi => "GSSAPI",
        }
    }

    pub fn parse(s: &str) -> SaslMechanismKind {
        match s.trim().to_ascii_uppercase().replace('_', "-").as_str() {
            "PLAIN" => SaslMechanismKind::Plain,
            "SCRAM-SHA-256" => SaslMechanismKind::ScramSha256,
            "SCRAM-SHA-512" => SaslMechanismKind::ScramSha512,
            "GSSAPI" => SaslMechanismKind::Gssapi,
            _ => SaslMechanismKind::None,
        }
    }

    fn to_client_mech(self) -> Option<SaslMechanismType> {
        match self {
            SaslMechanismKind::None | SaslMechanismKind::Gssapi => None,
            SaslMechanismKind::Plain => Some(SaslMechanismType::Plain),
            SaslMechanismKind::ScramSha256 => Some(SaslMechanismType::ScramSha256),
            SaslMechanismKind::ScramSha512 => Some(SaslMechanismType::ScramSha512),
        }
    }
}

/// 一条 Kafka 集群连接配置（持久化到配置文件）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    /// 逗号分隔的 `host:port` 列表。
    pub bootstrap_servers: String,
    #[serde(default)]
    pub security_protocol: SecurityProtocolKind,
    #[serde(default)]
    pub sasl_mechanism: SaslMechanismKind,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// Kerberos principal（仅 GSSAPI 使用）。
    #[serde(default)]
    pub kerberos_principal: String,
    #[serde(default)]
    pub kerberos_keytab: String,
    #[serde(default)]
    pub kdc_host: String,
    #[serde(default = "default_kdc_port")]
    pub kdc_port: u16,
    /// TLS 域名（SNI / 证书校验）。
    #[serde(default)]
    pub tls_domain: String,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default)]
    pub description: String,
    /// 是否已在界面上验证过连通性（仅用于展示）。
    #[serde(default)]
    pub verified: bool,
}

fn default_kdc_port() -> u16 {
    88
}

fn default_client_id() -> String {
    "kafka-ui".to_string()
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            bootstrap_servers: "127.0.0.1:9092".to_string(),
            security_protocol: SecurityProtocolKind::Plaintext,
            sasl_mechanism: SaslMechanismKind::None,
            username: String::new(),
            password: String::new(),
            kerberos_principal: String::new(),
            kerberos_keytab: String::new(),
            kdc_host: String::new(),
            kdc_port: 88,
            tls_domain: String::new(),
            client_id: default_client_id(),
            description: String::new(),
            verified: false,
        }
    }
}

impl ClusterConfig {
    /// 解析 bootstrap 列表：支持逗号 / 分号 / 空白分隔。
    pub fn bootstrap_list(&self) -> Vec<String> {
        self.bootstrap_servers
            .split([',', ';', '\n'])
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// 表单校验。返回第一处错误信息。
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(anyhow!("集群名称不能为空"));
        }
        let servers = self.bootstrap_list();
        if servers.is_empty() {
            return Err(anyhow!("至少配置一个 bootstrap 地址"));
        }
        for s in &servers {
            let (host, port) = s
                .rsplit_once(':')
                .ok_or_else(|| anyhow!("bootstrap 地址缺少端口: {}", s))?;
            if host.trim().is_empty() {
                return Err(anyhow!("bootstrap 地址缺少主机: {}", s));
            }
            port.parse::<u16>()
                .map_err(|_| anyhow!("bootstrap 端口非法: {}", s))?;
        }
        if self.security_protocol.needs_sasl() {
            match self.sasl_mechanism {
                SaslMechanismKind::None => {
                    return Err(anyhow!("选择了 SASL 协议，请指定 SASL 机制"));
                }
                SaslMechanismKind::Gssapi => {
                    if self.kerberos_principal.trim().is_empty() {
                        return Err(anyhow!("Kerberos 认证需要填写 principal"));
                    }
                    if self.kerberos_keytab.trim().is_empty() {
                        return Err(anyhow!("Kerberos 认证需要填写 keytab 路径"));
                    }
                }
                _ => {
                    if self.username.trim().is_empty() {
                        return Err(anyhow!("SASL 认证需要填写用户名"));
                    }
                }
            }
        }
        if self.security_protocol.needs_tls() && self.tls_domain.trim().is_empty() {
            return Err(anyhow!("TLS 连接需要填写 TLS 域名"));
        }
        Ok(())
    }

    /// 用于日志展示的脱敏描述。
    pub fn masked_summary(&self) -> String {
        format!(
            "{} [{}] {}",
            self.name,
            self.security_protocol.as_str(),
            self.bootstrap_servers
        )
    }
}

/// 连通性测试结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTest {
    pub ok: bool,
    pub message: String,
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub broker_count: usize,
    pub brokers: Vec<BrokerInfo>,
    pub elapsed_ms: u128,
}

/// broker 信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub is_controller: bool,
}

/// 根据配置构建已连接的 `kafka_client::Client`。
///
/// 字符串认证逻辑对齐 kafka-proxy 的 `apply_to_client_builder`。
pub async fn build_client(cfg: &ClusterConfig) -> Result<Client> {
    cfg.validate()?;
    let servers = cfg.bootstrap_list();

    let mut builder = Client::builder(servers)
        .with_client_id(cfg.client_id.clone())
        .with_request_timeout(Duration::from_secs(30))
        .with_metadata_ttl(Duration::from_secs(60));

    // 安全协议 + TLS
    if cfg.security_protocol.needs_tls() {
        let tls = TlsConfig {
            domain: cfg.tls_domain.clone(),
            ..Default::default()
        };
        builder = builder.with_tls_config(tls);
    } else {
        builder = builder.with_plaintext();
    }

    // SASL / Kerberos
    if cfg.security_protocol.needs_sasl() {
        match cfg.sasl_mechanism {
            SaslMechanismKind::Gssapi => {
                let creds = KerberosCredentials::new(cfg.kerberos_principal.clone())
                    .with_keytab(cfg.kerberos_keytab.clone());
                builder = builder.with_kerberos(creds);
                if !cfg.kdc_host.trim().is_empty() {
                    builder = builder.with_kdc(cfg.kdc_host.clone(), cfg.kdc_port.max(1));
                }
            }
            other => {
                let mech = other
                    .to_client_mech()
                    .ok_or_else(|| anyhow!("不支持的 SASL 机制: {}", other.as_str()))?;
                builder =
                    builder.with_sasl_credentials(mech, cfg.username.clone(), cfg.password.clone());
            }
        }
    }

    builder
        .build()
        .await
        .with_context(|| format!("连接 Kafka 失败（{}）", cfg.bootstrap_servers))
}

/// 测试连接：连接成功后拉取集群拓扑。
pub async fn test_connection(cfg: &ClusterConfig) -> ConnectionTest {
    let start = std::time::Instant::now();
    let result = async {
        let client = build_client(cfg).await?;
        let admin = client.admin();
        let cluster = admin.describe_cluster().await?;
        let result = ConnectionTest {
            ok: true,
            message: format!(
                "连接成功，共 {} 个 broker，controller={:?}",
                cluster.brokers.len(),
                cluster.controller_id
            ),
            cluster_id: cluster.cluster_id.clone(),
            controller_id: cluster.controller_id,
            broker_count: cluster.brokers.len(),
            brokers: cluster
                .brokers
                .iter()
                .map(|b| BrokerInfo {
                    id: b.id,
                    host: b.host.clone(),
                    port: b.port,
                    is_controller: Some(b.id) == cluster.controller_id,
                })
                .collect(),
            elapsed_ms: start.elapsed().as_millis(),
        };
        let _ = client.close().await;
        Ok::<_, anyhow::Error>(result)
    }
    .await;

    match result {
        Ok(mut r) => {
            r.elapsed_ms = start.elapsed().as_millis();
            r
        }
        Err(e) => ConnectionTest {
            ok: false,
            message: format!("{:#}", e),
            cluster_id: None,
            controller_id: None,
            broker_count: 0,
            brokers: Vec::new(),
            elapsed_ms: start.elapsed().as_millis(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bootstrap_list() {
        let cfg = ClusterConfig {
            bootstrap_servers: "a:9092, b:9093 ;c:9094".into(),
            ..ClusterConfig::default()
        };
        assert_eq!(cfg.bootstrap_list(), vec!["a:9092", "b:9093", "c:9094"]);
    }

    #[test]
    fn validates_missing_port() {
        let cfg = ClusterConfig {
            name: "dev".into(),
            bootstrap_servers: "localhost".into(),
            ..ClusterConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn sasl_requires_credentials() {
        let mut cfg = ClusterConfig {
            name: "dev".into(),
            security_protocol: SecurityProtocolKind::SaslPlaintext,
            sasl_mechanism: SaslMechanismKind::Plain,
            username: String::new(),
            ..ClusterConfig::default()
        };
        assert!(cfg.validate().is_err());
        cfg.username = "alice".into();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn security_protocol_roundtrip() {
        for p in SecurityProtocolKind::ALL {
            assert_eq!(SecurityProtocolKind::parse(p.as_str()), p);
        }
        for m in SaslMechanismKind::ALL {
            assert_eq!(SaslMechanismKind::parse(m.as_str()), m);
        }
    }
}
