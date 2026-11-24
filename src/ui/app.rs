//! 应用主界面（对齐 kafka-console 参考设计）。
//!
//! 布局：左侧 218 分组导航（连接 / 集群 / 数据）+ 顶部 56 顶栏 + 内容区。
//! 六个页面：集群连接 · Topics · Brokers · 拉取消息 · 模拟发送 · 消费组。
//! 所有 Kafka 操作都在后台 tokio runtime 执行，结果回到 UI 线程更新实体。

use std::time::Duration;

use gpui::prelude::*;
use gpui::{
    div, px, AnyElement, App, AppContext, Context, Entity, FocusHandle, Focusable, FontWeight,
    IntoElement, MouseButton, Render, SharedString, Styled, Window,
};

use crate::kafka::client::{
    build_client, test_connection, BrokerInfo, ClusterConfig, ConnectionTest, SaslMechanismKind,
    SecurityProtocolKind,
};
use crate::kafka::topics::TopicSummary;
use crate::kafka::{consumer, messages, producer, topics};
use crate::store::Store;
use crate::ui::runtime::run_task;
use crate::ui::text_input::TextInput;
use crate::ui::{theme as th, widgets as w};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum View {
    Clusters,
    Topics,
    Brokers,
    Consume,
    Produce,
    Groups,
}

impl View {
    pub fn label(&self) -> &'static str {
        match self {
            View::Clusters => "集群连接",
            View::Topics => "Topics",
            View::Brokers => "Brokers",
            View::Consume => "拉取消息",
            View::Produce => "模拟发送",
            View::Groups => "消费组",
        }
    }
    fn icon(&self) -> &'static str {
        match self {
            View::Clusters => "◉",
            View::Topics => "▤",
            View::Brokers => "▦",
            View::Consume => "↓",
            View::Produce => "↑",
            View::Groups => "◍",
        }
    }
}

/// 集群编辑表单状态。
pub struct ClusterForm {
    pub id: String,
    pub name: Entity<TextInput>,
    pub bootstrap: Entity<TextInput>,
    pub protocol_idx: usize,
    pub mech_idx: usize,
    pub username: Entity<TextInput>,
    pub password: Entity<TextInput>,
    pub tls_domain: Entity<TextInput>,
    pub kerberos_principal: Entity<TextInput>,
    pub kerberos_keytab: Entity<TextInput>,
    pub kdc_host: Entity<TextInput>,
    pub description: Entity<TextInput>,
    pub error: Option<String>,
    pub test: Option<Result<String, String>>,
}

pub struct KafkaUi {
    pub view: View,
    pub focus_handle: FocusHandle,
    // 集群
    pub store: Store,
    pub config_path: String,
    pub selected: Option<String>,
    pub test: Option<ConnectionTest>,
    pub testing: bool,
    pub form: Option<ClusterForm>,
    // Broker
    pub brokers: Vec<BrokerInfo>,
    pub brokers_cluster_id: Option<String>,
    pub loading_brokers: bool,
    // Topic 详情
    pub topic_detail: Option<topics::TopicDetail>,
    pub loading_detail: bool,
    pub expanded_topic: Option<String>,
    // Topic 列表
    pub topic_filter: Entity<TextInput>,
    pub new_topic: Entity<TextInput>,
    pub topics: Vec<TopicSummary>,
    pub loading_topics: bool,
    pub partition_count: i32,
    pub replica_count: i32,
    // 拉取消息
    pub current_topic: Option<String>,
    pub msg_count: usize,
    pub msgs: messages::FetchResult,
    pub loading_msgs: bool,
    pub selected_msg: Option<usize>,
    pub consume_key: Entity<TextInput>,
    // 模拟发送
    pub send_key: Entity<TextInput>,
    pub send_value: Entity<TextInput>,
    pub send_result: Option<producer::SendOutcome>,
    pub send_history: Vec<(String, String, i32, i64)>,
    // 消费组
    pub groups: Vec<consumer::GroupView>,
    pub loading_groups: bool,
    pub selected_group: Option<String>,
    // 状态
    pub status: String,
    pub status_kind: StatusKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Info,
    Success,
    Error,
}

impl Focusable for KafkaUi {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl KafkaUi {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let path = Store::config_path();
        let store = Store::load(&path).unwrap_or_default();
        let selected = store.clusters.first().map(|c| c.id.clone());

        fn mk(
            placeholder: &'static str,
            window: &mut Window,
            cx: &mut Context<KafkaUi>,
        ) -> Entity<TextInput> {
            cx.new(|cx| TextInput::new("", placeholder, window, cx))
        }

        Self {
            view: View::Clusters,
            focus_handle: cx.focus_handle(),
            store,
            config_path: path.display().to_string(),
            selected,
            test: None,
            testing: false,
            form: None,
            brokers: Vec::new(),
            brokers_cluster_id: None,
            loading_brokers: false,
            topic_detail: None,
            loading_detail: false,
            expanded_topic: None,
            topic_filter: mk("过滤：名称 / 正则，如 ^order", window, cx),
            new_topic: mk("新 Topic 名称", window, cx),
            topics: Vec::new(),
            loading_topics: false,
            partition_count: 3,
            replica_count: 1,
            current_topic: None,
            msg_count: 20,
            msgs: messages::FetchResult::default(),
            loading_msgs: false,
            selected_msg: None,
            consume_key: mk("精确匹配 message key", window, cx),
            send_key: mk("例如 order-10086", window, cx),
            send_value: mk("JSON / 文本", window, cx),
            send_result: None,
            send_history: Vec::new(),
            groups: Vec::new(),
            loading_groups: false,
            selected_group: None,
            status: String::new(),
            status_kind: StatusKind::Info,
        }
    }

    fn set_status(&mut self, msg: impl Into<String>, kind: StatusKind) {
        self.status = msg.into();
        self.status_kind = kind;
    }

    pub fn selected_config(&self) -> Option<ClusterConfig> {
        self.selected
            .as_deref()
            .and_then(|id| self.store.get(id))
            .cloned()
    }

    fn run<T, F, D>(&mut self, cx: &mut Context<Self>, work: F, done: D)
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, String>> + Send + 'static,
        D: FnOnce(&mut Self, &mut Context<Self>, Result<T, String>) + 'static,
    {
        run_task(work, cx, done);
    }

    fn select_cluster(&mut self, id: String, cx: &mut Context<Self>) {
        self.selected = Some(id);
        self.test = None;
        self.topics.clear();
        self.current_topic = None;
        self.brokers.clear();
        self.groups.clear();
        self.selected_group = None;
        self.topic_detail = None;
        self.expanded_topic = None;
        cx.notify();
    }

    // ---------- 集群 ----------

    pub fn new_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let cfg = ClusterConfig {
            id: uuid::Uuid::new_v4().to_string(),
            name: String::new(),
            bootstrap_servers: String::new(),
            security_protocol: SecurityProtocolKind::Plaintext,
            sasl_mechanism: SaslMechanismKind::Plain,
            client_id: "kafka-ui".into(),
            ..ClusterConfig::default()
        };
        self.open_form(cfg, window, cx);
    }

    pub fn edit_form(&mut self, id: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(cfg) = self.store.get(id).cloned() {
            self.open_form(cfg, window, cx);
        }
    }

    fn open_form(&mut self, cfg: ClusterConfig, window: &mut Window, cx: &mut Context<Self>) {
        let name = cx.new(|cx| TextInput::new(cfg.name.clone(), "例如 上海生产集群", window, cx));
        let bootstrap = cx.new(|cx| {
            TextInput::new(
                cfg.bootstrap_servers.clone(),
                "host1:9092,host2:9092",
                window,
                cx,
            )
        });
        let username = cx.new(|cx| TextInput::new(cfg.username.clone(), "用户名", window, cx));
        let password =
            cx.new(|cx| TextInput::new(cfg.password.clone(), "密码", window, cx).masked(true));
        let tls = cx.new(|cx| TextInput::new(cfg.tls_domain.clone(), "TLS 域名", window, cx));
        let principal = cx.new(|cx| {
            TextInput::new(
                cfg.kerberos_principal.clone(),
                "principal@REALM.COM",
                window,
                cx,
            )
        });
        let keytab = cx.new(|cx| {
            TextInput::new(
                cfg.kerberos_keytab.clone(),
                "/etc/security/kafka.keytab",
                window,
                cx,
            )
        });
        let kdc = cx.new(|cx| TextInput::new(cfg.kdc_host.clone(), "KDC 主机（可选）", window, cx));
        let description = cx.new(|cx| {
            TextInput::new(
                cfg.description.clone(),
                "例如：订单业务专用，联系人 xxx",
                window,
                cx,
            )
        });
        self.form = Some(ClusterForm {
            id: cfg.id,
            name,
            bootstrap,
            protocol_idx: SecurityProtocolKind::ALL
                .iter()
                .position(|p| *p == cfg.security_protocol)
                .unwrap_or(0),
            mech_idx: SaslMechanismKind::ALL
                .iter()
                .position(|m| *m == cfg.sasl_mechanism)
                .unwrap_or(1),
            username,
            password,
            tls_domain: tls,
            kerberos_principal: principal,
            kerberos_keytab: keytab,
            kdc_host: kdc,
            description,
            error: None,
            test: None,
        });
        self.set_status("", StatusKind::Info);
        cx.notify();
    }

    fn form_to_config(&self, cx: &App) -> ClusterConfig {
        let f = self.form.as_ref().expect("form");
        let base = self.store.get(&f.id).cloned().unwrap_or_default();
        ClusterConfig {
            id: f.id.clone(),
            name: f.name.read(cx).text(),
            bootstrap_servers: f.bootstrap.read(cx).text(),
            security_protocol: SecurityProtocolKind::ALL[f.protocol_idx],
            sasl_mechanism: SaslMechanismKind::ALL[f.mech_idx],
            username: f.username.read(cx).text(),
            password: f.password.read(cx).text(),
            tls_domain: f.tls_domain.read(cx).text(),
            kerberos_principal: f.kerberos_principal.read(cx).text(),
            kerberos_keytab: f.kerberos_keytab.read(cx).text(),
            kdc_host: f.kdc_host.read(cx).text(),
            description: f.description.read(cx).text(),
            client_id: if base.client_id.is_empty() {
                "kafka-ui".into()
            } else {
                base.client_id
            },
            ..base
        }
    }

    pub fn test_form(&mut self, cx: &mut Context<Self>) {
        let cfg = self.form_to_config(cx);
        let Some(form) = self.form.as_mut() else {
            return;
        };
        form.error = None;
        form.test = None;
        self.testing = true;
        self.run::<ConnectionTest, _, _>(
            cx,
            async move { Ok::<_, String>(test_connection(&cfg).await) },
            |this, cx, res| {
                this.testing = false;
                if let Ok(r) = res {
                    if let Some(f) = this.form.as_mut() {
                        f.test = Some(if r.ok {
                            Ok(r.message.clone())
                        } else {
                            Err(r.message.clone())
                        });
                    }
                    this.test = Some(r);
                }
                cx.notify();
            },
        );
    }

    pub fn save_form(&mut self, cx: &mut Context<Self>) {
        let cfg = self.form_to_config(cx);
        if let Err(e) = cfg.validate() {
            if let Some(f) = self.form.as_mut() {
                f.error = Some(format!("{:#}", e));
            }
            cx.notify();
            return;
        }
        if self.store.name_taken(&cfg.name, Some(&cfg.id)) {
            if let Some(f) = self.form.as_mut() {
                f.error = Some(format!("集群名称 '{}' 已存在", cfg.name.trim()));
            }
            cx.notify();
            return;
        }
        let path = Store::config_path();
        let mut store = self.store.clone();
        let verified = self.test.as_ref().map(|t| t.ok).unwrap_or(false);
        let mut cfg = cfg;
        cfg.verified = verified || store.get(&cfg.id).map(|c| c.verified).unwrap_or(false);
        let created = store.upsert(cfg.clone());
        match store.save(&path) {
            Ok(()) => {
                self.store = store;
                if created {
                    self.selected = Some(cfg.id.clone());
                }
                self.form = None;
                self.set_status(
                    if created {
                        "已新增连接"
                    } else {
                        "已保存连接修改"
                    },
                    StatusKind::Success,
                );
            }
            Err(e) => {
                if let Some(f) = self.form.as_mut() {
                    f.error = Some(format!("保存失败: {:#}", e));
                }
            }
        }
        cx.notify();
    }

    pub fn delete_cluster(&mut self, id: String, cx: &mut Context<Self>) {
        let path = Store::config_path();
        let mut store = self.store.clone();
        if store.remove(&id) {
            match store.save(&path) {
                Ok(()) => {
                    self.store = store;
                    if self.selected.as_deref() == Some(id.as_str()) {
                        self.selected = self.store.clusters.first().map(|c| c.id.clone());
                    }
                    self.form = None;
                    self.set_status("已删除连接", StatusKind::Success);
                }
                Err(e) => self.set_status(format!("删除失败: {:#}", e), StatusKind::Error),
            }
        }
        cx.notify();
    }

    // ---------- Broker ----------

    pub fn load_brokers(&mut self, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            return;
        };
        self.loading_brokers = true;
        self.run::<(Vec<BrokerInfo>, Option<String>), _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let brokers = topics::list_brokers(&client.admin())
                    .await
                    .map_err(err_text)?;
                let controller = brokers
                    .iter()
                    .find(|b| b.is_controller)
                    .map(|b| format!("{}", b.id));
                let _ = client.close().await;
                Ok((brokers, controller))
            },
            |this, cx, res| {
                this.loading_brokers = false;
                match res {
                    Ok((brokers, ctrl)) => {
                        this.brokers = brokers;
                        this.brokers_cluster_id = ctrl;
                    }
                    Err(e) => {
                        this.set_status(format!("加载 Broker 失败: {}", e), StatusKind::Error)
                    }
                }
                cx.notify();
            },
        );
    }

    // ---------- Topic 详情 / 列表 ----------

    pub fn load_detail(&mut self, topic: &str, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            return;
        };
        let topic = topic.to_string();
        self.loading_detail = true;
        self.run::<topics::TopicDetail, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let d = topics::describe_topic(&client, &topic)
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(d)
            },
            |this, cx, res| {
                this.loading_detail = false;
                match res {
                    Ok(d) => this.topic_detail = Some(d),
                    Err(e) => this.set_status(format!("详情加载失败: {}", e), StatusKind::Error),
                }
                cx.notify();
            },
        );
    }

    pub fn toggle_topic(&mut self, name: &str, cx: &mut Context<Self>) {
        if self.expanded_topic.as_deref() == Some(name) {
            self.expanded_topic = None;
            self.topic_detail = None;
        } else {
            self.expanded_topic = Some(name.to_string());
            self.topic_detail = None;
            self.load_detail(name, cx);
        }
        cx.notify();
    }

    pub fn load_topics(&mut self, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            self.set_status("请先在「集群连接」选择集群", StatusKind::Error);
            cx.notify();
            return;
        };
        self.loading_topics = true;
        self.run::<Vec<TopicSummary>, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let list = topics::list_topics(&client.admin())
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(list)
            },
            |this, cx, res| {
                this.loading_topics = false;
                match res {
                    Ok(list) => {
                        this.set_status(
                            format!("已加载 {} 个 Topic", list.len()),
                            StatusKind::Success,
                        );
                        this.topics = list;
                    }
                    Err(e) => this.set_status(format!("加载失败: {}", e), StatusKind::Error),
                }
                cx.notify();
            },
        );
    }

    pub fn create_topic(&mut self, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            return;
        };
        let name = self.new_topic.read(cx).text().trim().to_string();
        if name.is_empty() {
            self.set_status("Topic 名称不能为空", StatusKind::Error);
            cx.notify();
            return;
        }
        let spec = topics::CreateTopicSpec {
            name,
            num_partitions: Some(self.partition_count),
            replication_factor: Some(self.replica_count as i16),
            configs: Vec::new(),
        };
        self.loading_topics = true;
        self.run::<(bool, String), _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let r = topics::create_topic(&client.admin(), &spec)
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(r)
            },
            |this, cx, res| match res {
                Ok((existed, msg)) => {
                    this.new_topic.update(cx, |t, cx| t.set_text("", cx));
                    this.set_status(
                        msg,
                        if existed {
                            StatusKind::Info
                        } else {
                            StatusKind::Success
                        },
                    );
                    this.load_topics(cx);
                }
                Err(e) => {
                    this.loading_topics = false;
                    this.set_status(format!("创建失败: {}", e), StatusKind::Error);
                    cx.notify();
                }
            },
        );
    }

    pub fn delete_topic(&mut self, name: String, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            return;
        };
        self.loading_topics = true;
        let n = name.clone();
        self.run::<String, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let msg = topics::delete_topic(&client.admin(), &n)
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(msg)
            },
            move |this, cx, res| match res {
                Ok(msg) => {
                    this.set_status(msg, StatusKind::Success);
                    if this.expanded_topic.as_deref() == Some(name.as_str()) {
                        this.expanded_topic = None;
                        this.topic_detail = None;
                    }
                    this.load_topics(cx);
                }
                Err(e) => {
                    this.loading_topics = false;
                    this.set_status(format!("删除失败: {}", e), StatusKind::Error);
                    cx.notify();
                }
            },
        );
    }

    // ---------- 拉取消息 ----------

    pub fn open_topic_pull(&mut self, topic: &str, cx: &mut Context<Self>) {
        self.current_topic = Some(topic.to_string());
        self.view = View::Consume;
        self.selected_msg = None;
        self.load_messages(cx);
        cx.notify();
    }

    pub fn load_messages(&mut self, cx: &mut Context<Self>) {
        let (Some(cfg), Some(topic)) = (self.selected_config(), self.current_topic.clone()) else {
            return;
        };
        let count = self.msg_count.clamp(1, 2000);
        self.loading_msgs = true;
        self.selected_msg = None;
        self.run::<messages::FetchResult, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let r = messages::fetch_latest(&client, &topic, count, Duration::from_secs(8))
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(r)
            },
            |this, cx, res| {
                this.loading_msgs = false;
                match res {
                    Ok(r) => {
                        this.set_status(
                            format!("已拉取 {} 条消息（{} ms）", r.messages.len(), r.elapsed_ms),
                            StatusKind::Success,
                        );
                        this.msgs = r;
                    }
                    Err(e) => this.set_status(format!("拉取失败: {}", e), StatusKind::Error),
                }
                cx.notify();
            },
        );
    }

    // ---------- 模拟发送 ----------

    pub fn send(&mut self, cx: &mut Context<Self>) {
        let (Some(cfg), Some(topic)) = (self.selected_config(), self.current_topic.clone()) else {
            self.set_status("请先选择目标 Topic", StatusKind::Error);
            cx.notify();
            return;
        };
        let key = self.send_key.read(cx).text();
        let value = self.send_value.read(cx).text();
        if value.is_empty() {
            self.set_status("消息内容不能为空", StatusKind::Error);
            cx.notify();
            return;
        }
        let spec = producer::SendSpec {
            topic: topic.clone(),
            key: key.clone(),
            value: value.clone(),
            partition: None,
            headers: Vec::new(),
        };
        self.set_status("发送中…", StatusKind::Info);
        self.run::<producer::SendOutcome, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let out = producer::send_message(&client, &spec)
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(out)
            },
            |this, cx, res| match res {
                Ok(out) => {
                    let summary = out.summary();
                    let (p, off) = (out.partition, out.offset);
                    let t = chrono::Local::now().format("%H:%M:%S").to_string();
                    this.send_history.insert(0, (t, topic, p, off));
                    this.send_result = Some(out);
                    this.set_status(summary, StatusKind::Success);
                    cx.notify();
                }
                Err(e) => {
                    this.set_status(format!("发送失败: {}", e), StatusKind::Error);
                    cx.notify();
                }
            },
        );
    }

    // ---------- 消费组 ----------

    pub fn load_groups(&mut self, cx: &mut Context<Self>) {
        let Some(cfg) = self.selected_config() else {
            return;
        };
        let topic = self.current_topic.clone();
        self.loading_groups = true;
        self.run::<Vec<consumer::GroupView>, _, _>(
            cx,
            async move {
                let client = build_client(&cfg).await.map_err(err_text)?;
                let g = consumer::topic_consumers(&client.admin(), topic.as_deref())
                    .await
                    .map_err(err_text)?;
                let _ = client.close().await;
                Ok(g)
            },
            |this, cx, res| {
                this.loading_groups = false;
                match res {
                    Ok(g) => {
                        this.selected_group = g.first().map(|x| x.group.clone());
                        this.set_status(format!("{} 个消费组", g.len()), StatusKind::Success);
                        this.groups = g;
                    }
                    Err(e) => this.set_status(format!("查询失败: {}", e), StatusKind::Error),
                }
                cx.notify();
            },
        );
    }
}

pub fn err_text(e: anyhow::Error) -> String {
    format!("{:#}", e)
}

fn pretty_json(s: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|_| s.to_string()),
        Err(_) => s.to_string(),
    }
}

fn fmt_ids(ids: &[i32]) -> String {
    if ids.is_empty() {
        return "—".into();
    }
    ids.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------
// 渲染
// ---------------------------------------------------------------------------

impl Render for KafkaUi {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity().clone();

        // ── 侧栏 ──
        let mut nav = div()
            .flex()
            .flex_col()
            .w(px(218.0))
            .h_full()
            .flex_shrink_0()
            .bg(th::sidebar_bg())
            .border_r_1()
            .border_color(th::border());

        // brand
        nav = nav.child(
            div()
                .flex()
                .items_center()
                .gap(px(10.0))
                .px(px(16.0))
                .pt(px(16.0))
                .pb(px(14.0))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .justify_center()
                        .w(px(32.0))
                        .h(px(32.0))
                        .rounded(px(10.0))
                        .bg(th::text())
                        .text_color(th::bg_panel())
                        .text_size(px(14.0))
                        .font_weight(FontWeight::BOLD)
                        .child("K"),
                )
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .child(
                            div()
                                .text_size(th::text_md())
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(th::text())
                                .child("Kafka Console"),
                        )
                        .child(
                            div()
                                .text_size(px(9.5))
                                .text_color(th::text_muted())
                                .child("OPS · 只读为主"),
                        ),
                ),
        );

        nav = nav.child(self.render_nav_group("连接", vec![View::Clusters], &entity));
        nav = nav.child(self.render_nav_group("集群", vec![View::Topics, View::Brokers], &entity));
        nav = nav.child(self.render_nav_group(
            "数据",
            vec![View::Consume, View::Produce, View::Groups],
            &entity,
        ));

        // 侧栏底部集群小卡
        let cluster_mini = self
            .selected_config()
            .map(|c| (c.name.clone(), c.bootstrap_servers.clone()))
            .unwrap_or_else(|| ("未选择集群".into(), "—".into()));
        nav = nav.child(div().flex_1()).child(
            div().p(px(12.0)).child(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(5.0))
                    .p(px(10.0))
                    .rounded(px(10.0))
                    .border_1()
                    .border_color(th::border())
                    .bg(th::bg_panel())
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(6.0))
                            .child(w::dot(if self.selected.is_some() {
                                th::success_fg()
                            } else {
                                th::text_faint()
                            }))
                            .child(
                                div()
                                    .text_size(th::text_sm())
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(th::text())
                                    .child(cluster_mini.0),
                            ),
                    )
                    .child(
                        div()
                            .font_family("monospace")
                            .text_size(px(9.5))
                            .text_color(th::text_faint())
                            .child(cluster_mini.1),
                    ),
            ),
        );

        // ── 主区 ──
        let content = match self.view {
            View::Clusters => self.view_clusters(cx),
            View::Topics => self.view_topics(cx),
            View::Brokers => self.view_brokers(cx),
            View::Consume => self.view_consume(cx),
            View::Produce => self.view_produce(cx),
            View::Groups => self.view_groups(cx),
        };

        let topbar = self.render_topbar(&entity, cx);

        let mut main = div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w_0()
            .h_full()
            .child(topbar)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .p(px(20.0))
                    .child(content),
            );

        if !self.status.is_empty() {
            let kind = match self.status_kind {
                StatusKind::Success => w::ChipKind::Ok,
                StatusKind::Error => w::ChipKind::Bad,
                StatusKind::Info => w::ChipKind::Neutral,
            };
            let e = entity.clone();
            main = main.child(
                div()
                    .id("status-bar")
                    .flex()
                    .items_center()
                    .gap(px(10.0))
                    .px(px(20.0))
                    .py(px(7.0))
                    .border_t_1()
                    .border_color(th::border())
                    .bg(th::bg_panel())
                    .child(w::chip(self.status.clone(), kind))
                    .child(div().flex_1())
                    .child(
                        div()
                            .cursor_pointer()
                            .text_size(th::text_sm())
                            .text_color(th::text_faint())
                            .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                                e.update(cx, |ui, cx| {
                                    ui.status.clear();
                                    cx.notify();
                                });
                            })
                            .child("✕"),
                    ),
            );
        }

        div()
            .flex()
            .size_full()
            .bg(th::bg_app())
            .text_color(th::text())
            .font_family("sans-serif")
            .child(nav)
            .child(main)
    }
}

impl KafkaUi {
    fn render_nav_group(
        &self,
        sep: &'static str,
        items: Vec<View>,
        entity: &Entity<KafkaUi>,
    ) -> AnyElement {
        let mut col = div().flex().flex_col().px(px(10.0)).child(
            div()
                .flex()
                .items_center()
                .gap(px(8.0))
                .mx(px(8.0))
                .mt(px(12.0))
                .mb(px(6.0))
                .text_size(px(9.5))
                .text_color(th::text_faint())
                .child(sep),
        );
        for v in items {
            let active = self.view == v;
            let e = entity.clone();
            let badge = match v {
                View::Topics if !self.topics.is_empty() => Some(format!("{}", self.topics.len())),
                View::Brokers if !self.brokers.is_empty() => {
                    Some(format!("{}", self.brokers.len()))
                }
                View::Groups if !self.groups.is_empty() => Some(format!("{}", self.groups.len())),
                _ => None,
            };
            let mut btn = div()
                .id(("nav", v as u8 as usize))
                .flex()
                .items_center()
                .gap(px(9.0))
                .h(px(32.0))
                .px(px(10.0))
                .mb(px(2.0))
                .rounded(px(9.0))
                .cursor_pointer()
                .text_size(th::text_md())
                .when(active, |s| {
                    s.bg(th::bg_panel())
                        .border_1()
                        .border_color(th::accent_border())
                        .text_color(th::accent())
                        .font_weight(FontWeight::SEMIBOLD)
                })
                .when(!active, |s| {
                    s.border_1()
                        .border_color(gpui::transparent_black())
                        .text_color(th::text_body())
                })
                .hover(|s| s.bg(th::bg_hover()))
                .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                    e.update(cx, |ui, cx| {
                        if ui.view != v {
                            ui.view = v;
                            ui.selected_msg = None;
                        }
                        cx.notify();
                    });
                })
                .child(
                    div()
                        .w(px(16.0))
                        .flex()
                        .justify_center()
                        .text_size(px(12.0))
                        .text_color(if active {
                            th::accent()
                        } else {
                            th::text_faint()
                        })
                        .child(v.icon()),
                )
                .child(v.label());
            if let Some(b) = badge {
                btn = btn.child(div().flex_1()).child(
                    div()
                        .px(px(6.0))
                        .rounded(px(999.0))
                        .bg(th::bg_app())
                        .text_size(px(9.5))
                        .text_color(th::text_muted())
                        .child(b),
                );
            }
            col = col.child(btn);
        }
        col.into_any_element()
    }

    fn render_topbar(&self, entity: &Entity<KafkaUi>, cx: &mut Context<Self>) -> AnyElement {
        let cfg = self.selected_config();
        let (nm, boot) = cfg
            .as_ref()
            .map(|c| (c.name.clone(), c.bootstrap_servers.clone()))
            .unwrap_or_else(|| ("未选择集群".into(), "—".into()));
        let connected = self.test.as_ref().map(|t| t.ok).unwrap_or(false);
        let broker_n = self.brokers.len();
        let e_refresh = entity.clone();

        let mut bar = div()
            .flex()
            .items_center()
            .gap(px(12.0))
            .h(px(56.0))
            .flex_shrink_0()
            .px(px(20.0))
            .bg(th::bg_panel())
            .border_b_1()
            .border_color(th::border())
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(9.0))
                    .px(px(10.0))
                    .py(px(4.0))
                    .rounded(px(10.0))
                    .border_1()
                    .border_color(th::border())
                    .bg(th::bg_panel())
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(24.0))
                            .h(px(24.0))
                            .rounded(px(7.0))
                            .bg(th::accent_soft())
                            .text_color(th::accent())
                            .text_size(px(11.0))
                            .child("◆"),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .child(
                                div()
                                    .text_size(th::text_sm())
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(th::text())
                                    .child(nm),
                            )
                            .child(
                                div()
                                    .font_family("monospace")
                                    .text_size(px(9.5))
                                    .text_color(th::text_muted())
                                    .child(boot),
                            ),
                    ),
            );

        bar = bar.child(if connected {
            w::status_chip("连接正常", w::ChipKind::Ok).into_any_element()
        } else {
            w::status_chip("未连接", w::ChipKind::Neutral).into_any_element()
        });
        if broker_n > 0 {
            bar = bar.child(
                w::chip(format!("{} broker", broker_n), w::ChipKind::Neutral).into_any_element(),
            );
        }
        bar = bar.child(div().flex_1());
        if self.testing {
            bar = bar.child(w::chip("测试中…", w::ChipKind::Info).into_any_element());
        }
        bar = bar.child(w::button(
            "top-refresh",
            "刷新",
            w::ButtonKind::Secondary,
            self.selected.is_some(),
            move |_, cx| {
                e_refresh.update(cx, |ui, cx| {
                    ui.load_topics(cx);
                    ui.load_brokers(cx);
                });
            },
        ));
        let _ = cx;
        bar.into_any_element()
    }

    // ═══════════ 1. 集群连接 ═══════════
    fn view_clusters(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let e_new = entity.clone();

        let head = w::page_head(
            "集群连接",
            Some("CONNECTIONS"),
            "选择已保存的连接，或新建一个。认证类型与 Security 协议必须匹配。",
            vec![w::button(
                "cl-new",
                "+ 新建连接",
                w::ButtonKind::Primary,
                true,
                move |window, cx| {
                    e_new.update(cx, |ui, cx| ui.new_form(window, cx));
                },
            )
            .into_any_element()],
        );

        // 左侧连接列表
        let mut list_body = div().flex().flex_col();
        if self.store.clusters.is_empty() {
            list_body = list_body.child(w::empty_state(
                "◉",
                "还没有 Kafka 连接",
                "点击「新建连接」录入 bootstrap 与认证信息",
            ));
        }
        for c in self.store.clusters.clone() {
            let is_on = self.selected.as_deref() == Some(c.id.as_str());
            let e_pick = entity.clone();
            let e_edit = entity.clone();
            let cid = c.id.clone();
            let cid2 = c.id.clone();
            let cname = c.name.clone();
            let initial = cname
                .chars()
                .next()
                .map(|ch| ch.to_uppercase().to_string())
                .unwrap_or_else(|| "K".into());
            let boot_first = c.bootstrap_list().first().cloned().unwrap_or_default();
            let extra = c.bootstrap_list().len().saturating_sub(1);
            let conn_state = if c.verified {
                w::ChipKind::Ok
            } else {
                w::ChipKind::Neutral
            };
            list_body = list_body.child(
                div()
                    .id(SharedString::from(format!("conn-{}", c.id)))
                    .flex()
                    .items_center()
                    .gap(px(11.0))
                    .px(px(14.0))
                    .py(px(11.0))
                    .border_b_1()
                    .border_color(th::line_2())
                    .cursor_pointer()
                    .when(is_on, |s| s.bg(th::accent_bg()))
                    .hover(|s| s.bg(th::bg_hover()))
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        let cid = cid.clone();
                        e_pick.update(cx, |ui, cx| ui.select_cluster(cid, cx));
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(30.0))
                            .h(px(30.0))
                            .flex_shrink_0()
                            .rounded(px(9.0))
                            .bg(th::accent_soft())
                            .border_1()
                            .border_color(th::accent_border())
                            .text_color(th::accent())
                            .font_weight(FontWeight::BOLD)
                            .text_size(th::text_sm())
                            .child(initial),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_w_0()
                            .child(
                                div()
                                    .text_size(th::text_md())
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(th::text())
                                    .child(c.name.clone()),
                            )
                            .child(
                                div()
                                    .font_family("monospace")
                                    .text_size(px(10.0))
                                    .text_color(th::text_muted())
                                    .child(if extra > 0 {
                                        format!("{} +{}", boot_first, extra)
                                    } else {
                                        boot_first
                                    }),
                            )
                            .when(!c.description.trim().is_empty(), |s| {
                                s.child(
                                    div()
                                        .text_size(px(10.0))
                                        .text_color(th::text_faint())
                                        .child(c.description.clone()),
                                )
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .items_end()
                            .gap(px(4.0))
                            .child(w::chip(
                                if c.verified { "已连接" } else { "未连接" },
                                conn_state,
                            ))
                            .child(
                                div()
                                    .flex()
                                    .gap(px(4.0))
                                    .child(w::button_sm(
                                        "conn-edit",
                                        "编辑",
                                        w::ButtonKind::Ghost,
                                        true,
                                        move |window, cx| {
                                            let cid = cid2.clone();
                                            e_edit.update(cx, |ui, cx| {
                                                ui.edit_form(&cid, window, cx)
                                            });
                                        },
                                    ))
                                    .child(w::button_sm(
                                        "conn-del",
                                        "删除",
                                        w::ButtonKind::Danger,
                                        true,
                                        {
                                            let e = entity.clone();
                                            let cid = c.id.clone();
                                            move |_, cx| {
                                                let cid = cid.clone();
                                                e.update(cx, |ui, cx| ui.delete_cluster(cid, cx));
                                            }
                                        },
                                    )),
                            ),
                    ),
            );
        }

        let list_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "◉",
                    "已保存的连接",
                    Some("点击切换，右侧可编辑".into()),
                    None,
                ))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .max_h(px(560.0))
                        .overflow_hidden()
                        .child(list_body),
                ),
        );

        // 右侧表单
        let form_card: AnyElement = if let Some(form) = &self.form {
            self.render_form(form, cx)
        } else {
            w::card(
                div()
                    .flex()
                    .flex_col()
                    .child(w::card_header(
                        "✎",
                        "连接详情",
                        Some("选择左侧连接编辑，或新建".into()),
                        None,
                    ))
                    .child(div().p(px(16.0)).child(w::empty_state(
                        "✎",
                        "未编辑任何连接",
                        "点击左侧连接卡片右侧的「编辑」，或点「+ 新建连接」",
                    ))),
            )
            .into_any_element()
        };

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(px(16.0))
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .w(px(360.0))
                            .flex_shrink_0()
                            .max_h_full()
                            .overflow_hidden()
                            .child(list_card),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .max_h_full()
                            .overflow_hidden()
                            .child(form_card),
                    ),
            )
            .into_any_element()
    }

    fn render_form(&self, form: &ClusterForm, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let proto = SecurityProtocolKind::ALL[form.protocol_idx];
        let mech = SaslMechanismKind::ALL[form.mech_idx];

        let e_proto = entity.clone();
        let e_mech = entity.clone();
        let e_test = entity.clone();
        let e_save = entity.clone();
        let e_cancel = entity.clone();
        let testing = self.testing;

        let editing = !form.id.is_empty() && self.store.get(&form.id).is_some();
        let title = if editing {
            "编辑连接"
        } else {
            "新建连接"
        };

        // 冲突提示
        let conflict = if proto.needs_sasl() && mech == SaslMechanismKind::None {
            Some(("error", "配置冲突：Security 选了 SASL_*，但 SASL 机制是「无」。握手会因缺少凭据失败，请选择 PLAIN / SCRAM / GSSAPI。"))
        } else if proto.needs_sasl()
            && mech == SaslMechanismKind::Gssapi
            && proto == SecurityProtocolKind::Ssl
        {
            Some((
                "warn",
                "配置冲突：Kerberos 必须走 SASL_SSL 或 SASL_PLAINTEXT。",
            ))
        } else {
            None
        };

        let mut body = div().flex().flex_col().gap(px(12.0)).p(px(16.0));
        body = body.child(w::field_with_help("连接名称  *", form.name.clone(), None));
        body = body.child(w::field_with_help(
            "Bootstrap Servers  *",
            form.bootstrap.clone(),
            Some("逗号分隔，建议至少填 2 个地址；只需能连上其中一个 broker 即可"),
        ));
        body = body.child(w::choice_row(
            "安全协议",
            SecurityProtocolKind::ALL
                .iter()
                .map(|p| p.as_str())
                .collect(),
            form.protocol_idx,
            move |idx, cx| {
                e_proto.update(cx, |ui, cx| {
                    if let Some(f) = ui.form.as_mut() {
                        f.protocol_idx = idx;
                        // SASL 协议默认选中 PLAIN（若当前是 NONE）
                        if SecurityProtocolKind::ALL[idx].needs_sasl() && f.mech_idx == 0 {
                            f.mech_idx = 1;
                        }
                    }
                    cx.notify();
                });
            },
        ));
        if proto.needs_sasl() {
            body = body.child(w::choice_row(
                "SASL 机制",
                SaslMechanismKind::ALL
                    .iter()
                    .map(|m| m.as_str())
                    .collect::<Vec<_>>(),
                form.mech_idx,
                move |idx, cx| {
                    e_mech.update(cx, |ui, cx| {
                        if let Some(f) = ui.form.as_mut() {
                            f.mech_idx = idx;
                        }
                        cx.notify();
                    });
                },
            ));
            match mech {
                SaslMechanismKind::Gssapi => {
                    body = body.child(w::field(
                        "Kerberos Principal",
                        form.kerberos_principal.clone(),
                    ));
                    body = body.child(w::field("Keytab 路径", form.kerberos_keytab.clone()));
                    body = body.child(w::field("KDC 主机（可选）", form.kdc_host.clone()));
                }
                SaslMechanismKind::None => {}
                _ => {
                    body = body.child(w::field("用户名", form.username.clone()));
                    body = body.child(w::field("密码", form.password.clone()));
                }
            }
        }
        if proto.needs_tls() {
            body = body.child(w::field(
                "TLS 域名（SNI / 证书校验）",
                form.tls_domain.clone(),
            ));
        }
        body = body.child(w::field("备注信息", form.description.clone()));

        if let Some((kind, msg)) = conflict {
            body = body.child(w::banner(kind, msg));
        }
        if let Some(err) = &form.error {
            body = body.child(w::banner("error", err.clone()));
        }
        if let Some(t) = &form.test {
            body = body.child(match t {
                Ok(m) => w::banner("success", m.clone()).into_any_element(),
                Err(m) => w::banner("error", m.clone()).into_any_element(),
            });
        }

        body = body.child(
            div()
                .flex()
                .items_center()
                .gap(px(8.0))
                .pt(px(4.0))
                .child(w::button(
                    "f-test",
                    if testing {
                        "测试中…"
                    } else {
                        "测试连接"
                    },
                    w::ButtonKind::Secondary,
                    !testing,
                    move |_, cx| {
                        e_test.update(cx, |ui, cx| ui.test_form(cx));
                    },
                ))
                .child(w::button(
                    "f-save",
                    "保存并连接",
                    w::ButtonKind::Primary,
                    true,
                    move |_, cx| {
                        e_save.update(cx, |ui, cx| ui.save_form(cx));
                    },
                ))
                .child(w::button(
                    "f-cancel",
                    "取消",
                    w::ButtonKind::Ghost,
                    true,
                    move |_, cx| {
                        e_cancel.update(cx, |ui, cx| {
                            ui.form = None;
                            cx.notify();
                        });
                    },
                )),
        );
        body = body.child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_faint())
                .child("认证类型与 Security 必须匹配：Kerberos → SASL_SSL / SASL_PLAINTEXT；BASIC(PLAIN/SCRAM) → SASL_*；SSL 只做 TLS。"),
        );

        w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "✎",
                    title,
                    None,
                    Some(w::AnyElementAlias(
                        w::chip(proto.as_str(), w::ChipKind::Info).into_any_element(),
                    )),
                ))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .max_h(px(620.0))
                        .overflow_hidden()
                        .child(body),
                ),
        )
        .into_any_element()
    }

    // ═══════════ 2. Topics ═══════════
    fn view_topics(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let e_refresh = entity.clone();
        let loading = self.loading_topics;

        let total_parts: usize = self.topics.iter().map(|t| t.partitions).sum();
        let internal = self.topics.iter().filter(|t| t.internal).count();
        let biz = self.topics.len().saturating_sub(internal);

        let head = w::page_head(
            "Topics",
            Some("CLUSTER"),
            "先看列表定位 topic，再展开查看分区树（Leader / 副本 / ISR / LEO）。",
            vec![w::button(
                "tp-refresh",
                if loading { "加载中…" } else { "刷新" },
                w::ButtonKind::Primary,
                !loading,
                move |_, cx| {
                    e_refresh.update(cx, |ui, cx| ui.load_topics(cx));
                },
            )
            .into_any_element()],
        );

        let kpis = div()
            .flex()
            .gap(px(12.0))
            .child(w::kpi(
                "▤",
                "Topic 总数",
                format!("{}", self.topics.len()),
                format!("业务 {} · 内部 {}", biz, internal),
                "",
            ))
            .child(w::kpi(
                "≣",
                "分区总数",
                format!("{}", total_parts),
                if self.topics.is_empty() {
                    "—".into()
                } else {
                    format!(
                        "平均 {:.1} / topic",
                        total_parts as f64 / self.topics.len() as f64
                    )
                },
                "",
            ));

        // 工具栏
        let toolbar = div()
            .flex()
            .items_center()
            .gap(px(8.0))
            .px(px(16.0))
            .py(px(10.0))
            .border_b_1()
            .border_color(th::line_2())
            .child(
                div()
                    .w(px(260.0))
                    .h(px(30.0))
                    .px(px(9.0))
                    .flex()
                    .items_center()
                    .rounded(px(9.0))
                    .border_1()
                    .border_color(th::border())
                    .bg(th::bg_panel())
                    .child(self.topic_filter.clone()),
            )
            .child(w::chip(
                format!("{} 个", self.topics.len()),
                w::ChipKind::Neutral,
            ));

        // 列表
        let mut rows = div().flex().flex_col();
        if loading && self.topics.is_empty() {
            rows = rows.child(w::loading("加载 Topic 中…"));
        } else if self.topics.is_empty() {
            rows = rows.child(w::empty_state(
                "▤",
                "暂无 Topic",
                "点击「刷新」从集群拉取 Topic 列表",
            ));
        } else {
            let filter = self.topic_filter.read(cx).text().to_lowercase();
            let shown: Vec<_> = self
                .topics
                .iter()
                .filter(|t| filter.is_empty() || t.name.to_lowercase().contains(&filter))
                .cloned()
                .collect();
            rows = rows.child(w::table_header(&[
                ("Topic", 4.0),
                ("分区", 1.0),
                ("类型", 1.0),
                ("操作", 2.2),
            ]));
            for t in shown {
                let is_open = self.expanded_topic.as_deref() == Some(t.name.as_str());
                let e_toggle = entity.clone();
                let e_pull = entity.clone();
                let e_del = entity.clone();
                let name = t.name.clone();
                let name_pull = t.name.clone();
                let name_del = t.name.clone();
                rows = rows.child(
                    div()
                        .id(SharedString::from(format!("tp-row-{}", t.name)))
                        .flex()
                        .items_center()
                        .px(px(16.0))
                        .py(px(9.0))
                        .border_b_1()
                        .border_color(th::line_2())
                        .cursor_pointer()
                        .when(is_open, |s| s.bg(th::accent_bg()))
                        .hover(|s| s.bg(th::row_hover()))
                        .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                            let name = name.clone();
                            e_toggle.update(cx, |ui, cx| ui.toggle_topic(&name, cx));
                        })
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .flex()
                                .items_center()
                                .gap(px(8.0))
                                .child(
                                    div()
                                        .text_size(px(9.0))
                                        .text_color(if is_open {
                                            th::accent()
                                        } else {
                                            th::text_faint()
                                        })
                                        .child(if is_open { "▼" } else { "▶" }),
                                )
                                .child(
                                    div()
                                        .text_size(th::text_md())
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .text_color(th::text())
                                        .child(t.name.clone()),
                                )
                                .when(t.internal, |s| {
                                    s.child(w::chip("内部", w::ChipKind::Neutral))
                                }),
                        )
                        .child(w::tcell(
                            1.0,
                            format!("{}", t.partitions),
                            th::text_body(),
                            false,
                        ))
                        .child(w::tcell(
                            1.0,
                            if t.internal {
                                "内部".to_string()
                            } else {
                                "业务".to_string()
                            },
                            th::text_muted(),
                            false,
                        ))
                        .child(
                            div()
                                .flex()
                                .w(px(180.0))
                                .flex_shrink_0()
                                .gap(px(6.0))
                                .child(w::button_sm(
                                    "tp-pull",
                                    "拉取",
                                    w::ButtonKind::Secondary,
                                    true,
                                    move |_, cx| {
                                        let name = name_pull.clone();
                                        e_pull.update(cx, |ui, cx| ui.open_topic_pull(&name, cx));
                                    },
                                ))
                                .child(w::button_sm(
                                    "tp-del",
                                    "删除",
                                    w::ButtonKind::Danger,
                                    true,
                                    move |_, cx| {
                                        let name = name_del.clone();
                                        e_del.update(cx, |ui, cx| ui.delete_topic(name, cx));
                                    },
                                )),
                        ),
                );
                if is_open {
                    let detail: AnyElement = if self.loading_detail {
                        div()
                            .p(px(16.0))
                            .child(w::loading("拉取 describeTopics 中…"))
                            .into_any_element()
                    } else if let Some(d) = &self.topic_detail {
                        render_partition_detail(d)
                    } else {
                        div().into_any_element()
                    };
                    rows = rows.child(
                        div()
                            .bg(th::accent_bg())
                            .border_b_1()
                            .border_color(th::line_2())
                            .child(detail),
                    );
                }
            }
        }

        let list_card = w::card(div().flex().flex_col().child(toolbar).child(rows));

        // 创建 topic 工具条
        let e_create = entity.clone();
        let e_minus = entity.clone();
        let e_plus = entity.clone();
        let pc = self.partition_count;
        let rc = self.replica_count;
        let create_bar = div()
            .flex()
            .items_center()
            .flex_wrap()
            .gap(px(8.0))
            .p(px(12.0))
            .rounded(px(12.0))
            .border_1()
            .border_color(th::border())
            .bg(th::bg_panel())
            .child(
                div()
                    .text_size(th::text_sm())
                    .text_color(th::text_muted())
                    .child("新建 Topic："),
            )
            .child(
                div()
                    .w(px(220.0))
                    .h(px(30.0))
                    .px(px(9.0))
                    .flex()
                    .items_center()
                    .rounded(px(9.0))
                    .border_1()
                    .border_color(th::border())
                    .bg(th::bg_panel())
                    .child(self.new_topic.clone()),
            )
            .child(w::stepper("分区", pc, move |d, cx| {
                let e = e_minus.clone();
                e.update(cx, |ui, cx| {
                    ui.partition_count = (ui.partition_count + d).clamp(1, 200);
                    cx.notify();
                });
            }))
            .child(w::stepper("副本", rc, move |d, cx| {
                let e = e_plus.clone();
                e.update(cx, |ui, cx| {
                    ui.replica_count = (ui.replica_count + d).clamp(1, 10);
                    cx.notify();
                });
            }))
            .child(w::button(
                "tp-create",
                "创建 Topic",
                w::ButtonKind::Primary,
                !loading,
                move |_, cx| {
                    e_create.update(cx, |ui, cx| ui.create_topic(cx));
                },
            ));

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(kpis)
            .child(create_bar)
            .child(div().flex_1().min_h_0().overflow_hidden().child(list_card))
            .into_any_element()
    }

    // ═══════════ 3. Brokers ═══════════
    fn view_brokers(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let e_refresh = entity.clone();
        let loading = self.loading_brokers;
        let n = self.brokers.len();
        let ctrl = self.brokers_cluster_id.clone();

        let head = w::page_head(
            "Brokers",
            Some("CLUSTER"),
            "看存活，也看倾斜：分区分布是否均衡。",
            vec![w::button(
                "br-refresh",
                if loading { "加载中…" } else { "刷新" },
                w::ButtonKind::Primary,
                !loading,
                move |_, cx| {
                    e_refresh.update(cx, |ui, cx| ui.load_brokers(cx));
                },
            )
            .into_any_element()],
        );

        let kpis = div()
            .flex()
            .gap(px(12.0))
            .child(w::kpi(
                "▦",
                "Broker",
                format!("{}", n),
                if n > 0 {
                    "在线".to_string()
                } else {
                    "—".to_string()
                },
                "",
            ))
            .child(w::kpi(
                "★",
                "Controller",
                ctrl.clone().unwrap_or_else(|| "—".into()),
                ctrl.map(|c| format!("broker {}", c))
                    .unwrap_or_else(|| "—".into()),
                "",
            ));

        let mut table = div().flex().flex_col();
        table = table.child(w::table_header(&[
            ("Broker", 1.2),
            ("主机", 2.0),
            ("端口", 0.8),
            ("角色", 1.2),
            ("状态", 1.0),
        ]));
        if loading && self.brokers.is_empty() {
            table = table.child(w::loading("拉取 Broker 列表中…"));
        } else if self.brokers.is_empty() {
            table = table.child(w::empty_state(
                "▦",
                "暂无 Broker 信息",
                "选择集群后点击刷新",
            ));
        } else {
            for b in &self.brokers {
                table = table.child(
                    div()
                        .flex()
                        .items_center()
                        .px(px(16.0))
                        .py(px(9.0))
                        .border_b_1()
                        .border_color(th::line_2())
                        .hover(|s| s.bg(th::row_hover()))
                        .child(w::tcell(1.2, format!("broker-{}", b.id), th::text(), true))
                        .child(w::tcell(2.0, b.host.clone(), th::text_body(), false))
                        .child(w::tcell(
                            0.8,
                            format!("{}", b.port),
                            th::text_muted(),
                            false,
                        ))
                        .child(if b.is_controller {
                            div()
                                .w(px(84.0))
                                .flex_shrink_0()
                                .child(w::chip("Controller", w::ChipKind::Info))
                                .into_any_element()
                        } else {
                            div()
                                .w(px(84.0))
                                .flex_shrink_0()
                                .child(
                                    div()
                                        .text_size(th::text_sm())
                                        .text_color(th::text_faint())
                                        .child("Follower"),
                                )
                                .into_any_element()
                        })
                        .child(w::tcell(1.0, "在线", th::success_fg(), false)),
                );
            }
        }

        let list_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "▦",
                    "Broker 列表",
                    Some("id / 主机 / 端口 / 角色".into()),
                    None,
                ))
                .child(table),
        );

        // 分区分布（按 leader 角色简单可视化）
        let mut dist = div().flex().flex_col().gap(px(10.0)).p(px(16.0));
        if self.brokers.is_empty() {
            dist = dist.child(
                div()
                    .text_size(th::text_sm())
                    .text_color(th::text_faint())
                    .child("暂无数据"),
            );
        } else {
            for b in &self.brokers {
                let ratio = if b.is_controller { 1.0 } else { 0.9 };
                dist = dist.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(px(10.0))
                        .child(
                            div()
                                .w(px(64.0))
                                .child(w::mono(format!("b{}", b.id), th::text_muted())),
                        )
                        .child(
                            div()
                                .flex_1()
                                .child(w::lagbar(ratio, if b.is_controller { "ok" } else { "" })),
                        )
                        .child(div().w(px(90.0)).child(w::mono(
                            if b.is_controller {
                                "controller"
                            } else {
                                "follower"
                            },
                            th::text_faint(),
                        ))),
                );
            }
        }
        let dist_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "≣",
                    "分区分布",
                    Some("每根柱 = 该 broker 的分区承载概览".into()),
                    None,
                ))
                .child(dist),
        );

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(kpis)
            .child(list_card)
            .child(dist_card)
            .into_any_element()
    }

    // ═══════════ 4. 拉取消息 ═══════════
    fn view_consume(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let loading = self.loading_msgs;

        let head = w::page_head(
            "拉取消息",
            Some("READ ONLY"),
            "按分区与位点读取，不提交位点，不影响消费组进度。",
            vec![],
        );

        // 选择 topic + 条数
        let e_minus = entity.clone();
        let e_plus = entity.clone();
        let e_pull = entity.clone();
        let e_pull2 = entity.clone();
        let count = self.msg_count;
        let topic_label = self
            .current_topic
            .clone()
            .unwrap_or_else(|| "未选择 Topic".into());

        let mut topic_picker = div().flex().items_center().flex_wrap().gap(px(6.0));
        for t in self.topics.iter().take(12) {
            let tname = t.name.clone();
            let e = entity.clone();
            let active = self.current_topic.as_deref() == Some(tname.as_str());
            topic_picker = topic_picker.child(
                div()
                    .id(SharedString::from(format!("consume-topic-{}", tname)))
                    .px(px(9.0))
                    .py(px(4.0))
                    .rounded(px(7.0))
                    .border_1()
                    .border_color(if active { th::accent() } else { th::border() })
                    .bg(if active {
                        th::accent_soft()
                    } else {
                        th::bg_panel()
                    })
                    .text_size(th::text_xs())
                    .text_color(if active {
                        th::accent()
                    } else {
                        th::text_muted()
                    })
                    .cursor_pointer()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        let tname = tname.clone();
                        e.update(cx, |ui, cx| {
                            ui.current_topic = Some(tname);
                            ui.load_messages(cx);
                        });
                    })
                    .child(t.name.clone()),
            );
        }
        if self.topics.is_empty() {
            topic_picker = topic_picker.child(
                div()
                    .text_size(th::text_xs())
                    .text_color(th::text_faint())
                    .child("（先在 Topics 页加载列表）"),
            );
        }

        let controls = div()
            .flex()
            .flex_col()
            .gap(px(10.0))
            .p(px(16.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(8.0))
                    .child(
                        div()
                            .text_size(th::text_xs())
                            .text_color(th::text_muted())
                            .child("Topic："),
                    )
                    .child(w::chip(topic_label, w::ChipKind::Purple)),
            )
            .child(topic_picker)
            .child(
                div()
                    .flex()
                    .items_center()
                    .flex_wrap()
                    .gap(px(8.0))
                    .child(
                        div()
                            .text_size(th::text_xs())
                            .text_color(th::text_muted())
                            .child("每个分区最近条数："),
                    )
                    .child(w::button(
                        "cm-minus",
                        "−",
                        w::ButtonKind::Ghost,
                        count > 1,
                        move |_, cx| {
                            e_minus.update(cx, |ui, cx| {
                                ui.msg_count = ui.msg_count.saturating_sub(10).max(1);
                                ui.load_messages(cx);
                            });
                        },
                    ))
                    .child(
                        div()
                            .w(px(40.0))
                            .flex()
                            .justify_center()
                            .text_size(th::text_sm())
                            .child(format!("{}", count)),
                    )
                    .child(w::button(
                        "cm-plus",
                        "＋",
                        w::ButtonKind::Ghost,
                        count < 2000,
                        move |_, cx| {
                            e_plus.update(cx, |ui, cx| {
                                ui.msg_count = (ui.msg_count + 10).min(2000);
                                ui.load_messages(cx);
                            });
                        },
                    ))
                    .child(w::button(
                        "cm-pull",
                        if loading { "拉取中…" } else { "拉取" },
                        w::ButtonKind::Primary,
                        !loading && self.current_topic.is_some(),
                        move |_, cx| {
                            e_pull.update(cx, |ui, cx| ui.load_messages(cx));
                        },
                    ))
                    .child(w::button(
                        "cm-pull2",
                        "实时刷新",
                        w::ButtonKind::Secondary,
                        !loading && self.current_topic.is_some(),
                        move |_, cx| {
                            e_pull2.update(cx, |ui, cx| ui.load_messages(cx));
                        },
                    )),
            );

        let controls_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "↓",
                    "拉取消息",
                    Some("只读，不提交位点".into()),
                    None,
                ))
                .child(controls),
        );

        // 消息表
        let mut msgs = div().flex().flex_col();
        msgs = msgs.child(w::table_header(&[
            ("#", 0.6),
            ("Partition", 1.0),
            ("Offset", 1.2),
            ("时间", 2.2),
            ("Key", 2.0),
            ("内容", 6.0),
        ]));
        if loading {
            msgs = msgs.child(w::loading("拉取消息中…"));
        } else if self.msgs.messages.is_empty() {
            msgs = msgs.child(w::empty_state(
                "↓",
                "尚未拉取消息",
                "选择 topic 后点击「拉取」",
            ));
        } else {
            let list = self.msgs.messages.clone();
            let mut scroll = div().flex().flex_col().max_h(px(420.0)).overflow_hidden();
            for (idx, m) in list.iter().enumerate() {
                let e_sel = entity.clone();
                let sel = self.selected_msg == Some(idx);
                scroll = scroll.child(
                    div()
                        .id(("m-row", idx))
                        .flex()
                        .items_center()
                        .px(px(16.0))
                        .py(px(7.0))
                        .border_b_1()
                        .border_color(th::line_2())
                        .when(sel, |s| s.bg(th::accent_bg()))
                        .when(!sel && idx % 2 == 1, |s| s.bg(th::row_alt()))
                        .cursor_pointer()
                        .hover(|s| s.bg(th::row_hover()))
                        .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                            e_sel.update(cx, |ui, cx| {
                                ui.selected_msg = if ui.selected_msg == Some(idx) {
                                    None
                                } else {
                                    Some(idx)
                                };
                                cx.notify();
                            });
                        })
                        .child(w::tcell(
                            0.6,
                            format!("{}", idx + 1),
                            th::text_faint(),
                            false,
                        ))
                        .child(w::tcell(
                            1.0,
                            format!("{}", m.partition),
                            th::text_body(),
                            false,
                        ))
                        .child(w::tcell(
                            1.2,
                            format!("{}", m.offset),
                            th::text_body(),
                            false,
                        ))
                        .child(w::tcell(2.2, m.timestamp_text(), th::text_muted(), false))
                        .child(w::tcell(
                            2.0,
                            m.key.clone().unwrap_or_else(|| "—".into()),
                            th::text_muted(),
                            false,
                        ))
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .overflow_hidden()
                                .font_family("monospace")
                                .text_size(px(10.5))
                                .text_color(th::text_body())
                                .child(m.one_line_value()),
                        ),
                );
            }
            msgs = msgs.child(scroll);

            if let Some(sel) = self.selected_msg {
                if let Some(m) = list.get(sel) {
                    msgs = msgs.child(render_msg_detail(m));
                }
            }
        }
        let msgs_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "≡",
                    "消息",
                    Some(format!("{} 条 · 点行展开完整内容", self.msgs.messages.len()).into()),
                    None,
                ))
                .child(msgs),
        );

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(controls_card)
            .child(div().flex_1().min_h_0().overflow_hidden().child(msgs_card))
            .into_any_element()
    }

    // ═══════════ 5. 模拟发送 ═══════════
    fn view_produce(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let e_send = entity.clone();
        let topic_label = self
            .current_topic
            .clone()
            .unwrap_or_else(|| "未选择 Topic".into());

        let head = w::page_head(
            "模拟发送",
            Some("WRITE"),
            "向指定 topic 投递一条测试消息，回执只代表 broker 落盘。",
            vec![],
        );

        // 目标 topic 选择
        let mut picker = div().flex().items_center().flex_wrap().gap(px(6.0));
        for t in self.topics.iter().take(12) {
            let tname = t.name.clone();
            let e = entity.clone();
            let active = self.current_topic.as_deref() == Some(tname.as_str());
            picker = picker.child(
                div()
                    .id(SharedString::from(format!("produce-topic-{}", tname)))
                    .px(px(9.0))
                    .py(px(4.0))
                    .rounded(px(7.0))
                    .border_1()
                    .border_color(if active { th::accent() } else { th::border() })
                    .bg(if active {
                        th::accent_soft()
                    } else {
                        th::bg_panel()
                    })
                    .text_size(th::text_xs())
                    .text_color(if active {
                        th::accent()
                    } else {
                        th::text_muted()
                    })
                    .cursor_pointer()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        let tname = tname.clone();
                        e.update(cx, |ui, cx| {
                            ui.current_topic = Some(tname);
                            cx.notify();
                        });
                    })
                    .child(t.name.clone()),
            );
        }
        if self.topics.is_empty() {
            picker = picker.child(
                div()
                    .text_size(th::text_xs())
                    .text_color(th::text_faint())
                    .child("（先在 Topics 页加载列表）"),
            );
        }

        let form = div()
            .flex()
            .flex_col()
            .gap(px(12.0))
            .p(px(16.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(8.0))
                    .child(
                        div()
                            .text_size(th::text_xs())
                            .text_color(th::text_muted())
                            .child("目标 Topic："),
                    )
                    .child(w::chip(topic_label, w::ChipKind::Purple)),
            )
            .child(picker)
            .child(w::field("消息 Key（可选）", self.send_key.clone()))
            .child(w::field("Value（JSON / 文本）", self.send_value.clone()))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(8.0))
                    .child(w::button(
                        "p-send",
                        "发送",
                        w::ButtonKind::Primary,
                        self.current_topic.is_some(),
                        move |_, cx| {
                            e_send.update(cx, |ui, cx| ui.send(cx));
                        },
                    ))
                    .child(
                        div()
                            .text_size(th::text_xs())
                            .text_color(th::text_faint())
                            .child("发送后只等 broker ack，不代表业务消费成功"),
                    ),
            );

        let form_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "↑",
                    "模拟发送",
                    Some("走真实 producer，只影响目标 topic".into()),
                    None,
                ))
                .child(form),
        );

        // 结果卡
        let result_card = match &self.send_result {
            None => w::card(
                div()
                    .flex()
                    .flex_col()
                    .child(w::card_header(
                        "✓",
                        "发送结果",
                        Some("partition / offset / 耗时".into()),
                        None,
                    ))
                    .child(w::empty_state("↑", "尚未发送", "填写内容后点击「发送」")),
            )
            .into_any_element(),
            Some(out) => {
                let summary = out.summary();
                w::card(
                    div()
                        .flex()
                        .flex_col()
                        .child(w::card_header("✓", "发送结果", None, None))
                        .child(
                            div()
                                .flex()
                                .flex_col()
                                .gap(px(8.0))
                                .p(px(16.0))
                                .child(w::banner("success", summary))
                                .child(
                                    div()
                                        .text_size(th::text_xs())
                                        .text_color(th::text_faint())
                                        .child("ack 仅代表 broker 已落盘，不代表业务方已消费。"),
                                ),
                        ),
                )
                .into_any_element()
            }
        };

        // 历史
        let mut hist = div().flex().flex_col();
        hist = hist.child(w::table_header(&[
            ("时间", 1.0),
            ("Topic", 3.0),
            ("P", 0.6),
            ("Offset", 1.2),
        ]));
        if self.send_history.is_empty() {
            hist = hist.child(w::empty_state(
                "↺",
                "本会话暂无发送",
                "发送记录会显示在这里",
            ));
        } else {
            for (t, topic, p, off) in self.send_history.iter().take(20) {
                hist = hist.child(
                    div()
                        .flex()
                        .items_center()
                        .px(px(16.0))
                        .py(px(7.0))
                        .border_b_1()
                        .border_color(th::line_2())
                        .child(w::tcell(1.0, t.clone(), th::text_muted(), false))
                        .child(w::tcell(3.0, topic.clone(), th::text(), false))
                        .child(w::tcell(0.6, format!("{}", p), th::text_muted(), false))
                        .child(w::tcell(1.2, format!("{}", off), th::text_body(), false)),
                );
            }
        }
        let hist_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header("↺", "最近发送", Some("本会话".into()), None))
                .child(hist),
        );

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(px(16.0))
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(div().flex_1().min_w_0().child(form_card))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap(px(14.0))
                            .w(px(380.0))
                            .flex_shrink_0()
                            .child(result_card)
                            .child(hist_card),
                    ),
            )
            .into_any_element()
    }

    // ═══════════ 6. 消费组 ═══════════
    fn view_groups(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let entity = cx.entity().clone();
        let e_refresh = entity.clone();
        let loading = self.loading_groups;

        let head = w::page_head(
            "消费组",
            Some("LAG"),
            "先看哪个组掉队，再看是整组离线、重平衡，还是单分区倾斜。",
            vec![w::button(
                "gr-refresh",
                if loading { "查询中…" } else { "刷新" },
                w::ButtonKind::Primary,
                !loading,
                move |_, cx| {
                    e_refresh.update(cx, |ui, cx| ui.load_groups(cx));
                },
            )
            .into_any_element()],
        );

        // 左侧列表
        let mut list = div().flex().flex_col();
        list = list.child(w::table_header(&[
            ("消费组", 3.0),
            ("lag", 1.2),
            ("成员", 0.8),
        ]));
        if loading {
            list = list.child(w::loading("查询消费组中…"));
        } else if self.groups.is_empty() {
            list = list.child(w::empty_state(
                "◍",
                "暂无消费组",
                "当前集群没有可查的消费组",
            ));
        } else {
            for g in &self.groups {
                let gname = g.group.clone();
                let e_pick = entity.clone();
                let active = self.selected_group.as_deref() == Some(gname.as_str());
                let lag_tone = if g.total_lag > 10000 {
                    th::error()
                } else if g.total_lag > 500 {
                    th::warn()
                } else {
                    th::text_body()
                };
                list = list.child(
                    div()
                        .id(SharedString::from(format!("group-{}", gname)))
                        .flex()
                        .items_center()
                        .px(px(16.0))
                        .py(px(8.0))
                        .border_b_1()
                        .border_color(th::line_2())
                        .cursor_pointer()
                        .when(active, |s| s.bg(th::accent_bg()))
                        .hover(|s| s.bg(th::row_hover()))
                        .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                            let gname = gname.clone();
                            e_pick.update(cx, |ui, cx| {
                                ui.selected_group = Some(gname);
                                cx.notify();
                            });
                        })
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .flex()
                                .flex_col()
                                .child(
                                    div()
                                        .text_size(th::text_md())
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .text_color(th::text())
                                        .child(g.group.clone()),
                                )
                                .child(
                                    div()
                                        .text_size(px(10.0))
                                        .text_color(th::text_faint())
                                        .child(format!(
                                            "{} 分区 · {}",
                                            g.partitions.len(),
                                            if g.state.is_empty() { "-" } else { &g.state }
                                        )),
                                ),
                        )
                        .child(w::tcell(
                            1.2,
                            format!("{}", g.total_lag),
                            lag_tone,
                            g.total_lag > 0,
                        ))
                        .child(w::tcell(
                            0.8,
                            format!("{}", g.members.len()),
                            th::text_muted(),
                            false,
                        )),
                );
            }
        }
        let list_card = w::card(
            div()
                .flex()
                .flex_col()
                .child(w::card_header(
                    "◍",
                    "消费组",
                    Some(format!("{} 个", self.groups.len()).into()),
                    None,
                ))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .max_h(px(620.0))
                        .overflow_hidden()
                        .child(list),
                ),
        );

        // 右侧详情
        let detail: AnyElement = match self
            .selected_group
            .as_deref()
            .and_then(|n| self.groups.iter().find(|g| g.group == n))
        {
            None => w::card(
                div()
                    .flex()
                    .flex_col()
                    .child(w::card_header(
                        "◍",
                        "消费组详情",
                        Some("选择左侧消费组".into()),
                        None,
                    ))
                    .child(w::empty_state(
                        "◍",
                        "选择一个消费组查看 lag",
                        "点左侧列表里的消费组",
                    )),
            )
            .into_any_element(),
            Some(g) => render_group_detail(g),
        };

        div()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .size_full()
            .child(head)
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(px(16.0))
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(
                        div()
                            .w(px(380.0))
                            .flex_shrink_0()
                            .max_h_full()
                            .overflow_hidden()
                            .child(list_card),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .max_h_full()
                            .overflow_hidden()
                            .child(detail),
                    ),
            )
            .into_any_element()
    }
}

/// 分区详情（Topics 行内展开）。
fn render_partition_detail(d: &topics::TopicDetail) -> AnyElement {
    let mut table = div().flex().flex_col();
    table = table.child(w::table_header(&[
        ("Partition", 1.0),
        ("Leader", 1.0),
        ("Replicas", 2.0),
        ("ISR", 1.6),
        ("LEO", 1.2),
    ]));
    for p in &d.partitions {
        let isr_lag = p.isr.len() < p.replicas.len();
        table = table.child(
            div()
                .flex()
                .items_center()
                .px(px(16.0))
                .py(px(5.0))
                .border_b_1()
                .border_color(th::line_2())
                .child(w::tcell(1.0, format!("{}", p.partition), th::text(), true))
                .child(w::tcell(
                    1.0,
                    format!("b{}", p.leader),
                    th::text_body(),
                    false,
                ))
                .child(w::tcell(2.0, fmt_ids(&p.replicas), th::text_muted(), false))
                .child(
                    div()
                        .w(px(112.0))
                        .flex_shrink_0()
                        .flex()
                        .items_center()
                        .gap(px(6.0))
                        .child(w::mono(
                            format!("{}/{}", p.isr.len(), p.replicas.len()),
                            if isr_lag {
                                th::warn()
                            } else {
                                th::text_muted()
                            },
                        ))
                        .when(isr_lag, |s| {
                            s.child(w::chip("副本未同步", w::ChipKind::Warn))
                        }),
                )
                .child(w::tcell(
                    1.2,
                    p.log_end_offset
                        .map(|o| o.to_string())
                        .unwrap_or_else(|| "—".into()),
                    th::text_body(),
                    false,
                )),
        );
    }
    div()
        .flex()
        .flex_col()
        .gap(px(8.0))
        .p(px(16.0))
        .child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_muted())
                .child(format!(
                    "分区明细 · {} 个分区{}",
                    d.partitions.len(),
                    if d.internal { "（内部）" } else { "" }
                )),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .rounded(px(9.0))
                .border_1()
                .border_color(th::border())
                .bg(th::bg_panel())
                .overflow_hidden()
                .child(table),
        )
        .into_any_element()
}

/// 消息详情（拉取页点行展开）。
fn render_msg_detail(m: &messages::MessageView) -> AnyElement {
    let mut headers = div().flex().flex_col().gap(px(4.0));
    if m.headers.is_empty() {
        headers = headers.child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_faint())
                .child("无 header"),
        );
    } else {
        for (k, v) in &m.headers {
            headers = headers.child(
                div()
                    .flex()
                    .gap(px(10.0))
                    .child(
                        div()
                            .w(px(120.0))
                            .child(w::mono(k.clone(), th::text_muted())),
                    )
                    .child(w::mono(v.clone(), th::text_body())),
            );
        }
    }
    div()
        .flex()
        .gap(px(16.0))
        .p(px(16.0))
        .border_t_1()
        .border_color(th::line_2())
        .bg(th::accent_bg())
        .child(
            div()
                .flex()
                .flex_col()
                .gap(px(4.0))
                .w(px(260.0))
                .flex_shrink_0()
                .child(
                    div()
                        .text_size(th::text_xs())
                        .text_color(th::text_muted())
                        .child("Headers"),
                )
                .child(headers)
                .child(
                    div()
                        .flex()
                        .gap(px(10.0))
                        .mt(px(6.0))
                        .child(div().w(px(120.0)).child(w::mono("key", th::text_muted())))
                        .child(w::mono(
                            m.key.clone().unwrap_or_else(|| "—".into()),
                            th::text_body(),
                        )),
                )
                .child(
                    div()
                        .flex()
                        .gap(px(10.0))
                        .child(
                            div()
                                .w(px(120.0))
                                .child(w::mono("partition", th::text_muted())),
                        )
                        .child(w::mono(format!("{}", m.partition), th::text_body())),
                )
                .child(
                    div()
                        .flex()
                        .gap(px(10.0))
                        .child(
                            div()
                                .w(px(120.0))
                                .child(w::mono("offset", th::text_muted())),
                        )
                        .child(w::mono(format!("{}", m.offset), th::text_body())),
                ),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .p(px(12.0))
                .rounded(px(10.0))
                .bg(th::json_box_bg())
                .text_color(th::json_text())
                .font_family("monospace")
                .text_size(px(11.0))
                .child(pretty_json(&m.value)),
        )
        .into_any_element()
}

/// 消费组详情。
fn render_group_detail(g: &consumer::GroupView) -> AnyElement {
    let total = g.total_lag;
    let tone = if total > 10000 {
        "bad"
    } else if total > 500 {
        "warn"
    } else {
        ""
    };
    let state_kind = match g.state.as_str() {
        "Empty" => w::ChipKind::Bad,
        s if s.contains("Rebalance") => w::ChipKind::Warn,
        "Stable" => w::ChipKind::Ok,
        _ => w::ChipKind::Neutral,
    };

    let hint: AnyElement = match g.state.as_str() {
        "Empty" => w::banner(
            "error",
            "无成员：消费组已离线，lag 会一直涨。先确认消费进程是否存活、是否连的是这个集群。",
        )
        .into_any_element(),
        s if s.contains("Rebalance") => w::banner(
            "warn",
            "正在重平衡：此期间消费暂停，lag 短暂上涨属正常；持续 > 2 分钟要查成员是否反复掉线。",
        )
        .into_any_element(),
        _ => div().into_any_element(),
    };

    let kpis = div()
        .flex()
        .gap(px(12.0))
        .child(w::kpi(
            "≡",
            "总 lag",
            format!("{}", total),
            if total > 0 {
                "有积压".to_string()
            } else {
                "已追平".to_string()
            },
            tone,
        ))
        .child(w::kpi(
            "◍",
            "成员",
            format!("{}", g.members.len()),
            g.status_label(),
            "",
        ))
        .child(w::kpi(
            "≣",
            "分区",
            format!("{}", g.partitions.len()),
            "分区数",
            "",
        ));

    let mut parts = div().flex().flex_col();
    parts = parts.child(w::table_header(&[
        ("分区", 1.0),
        ("当前位点", 1.6),
        ("末尾位点", 1.6),
        ("lag", 1.2),
        ("占比", 2.0),
    ]));
    let max_lag = g.partitions.iter().map(|p| p.lag).max().unwrap_or(1).max(1);
    for p in &g.partitions {
        let ratio = (p.lag.max(0) as f32) / (max_lag as f32);
        let ptone = if p.lag > 5000 {
            "bad"
        } else if p.lag > 0 {
            ""
        } else {
            "ok"
        };
        let lag_color = if p.lag > 5000 {
            th::error()
        } else if p.lag > 0 {
            th::warn()
        } else {
            th::text_muted()
        };
        parts = parts.child(
            div()
                .flex()
                .items_center()
                .px(px(16.0))
                .py(px(6.0))
                .border_b_1()
                .border_color(th::line_2())
                .child(w::tcell(
                    1.0,
                    format!("{}", p.partition),
                    th::text_body(),
                    false,
                ))
                .child(w::tcell(
                    1.6,
                    format!("{}", p.committed_offset),
                    th::text_body(),
                    false,
                ))
                .child(w::tcell(
                    1.6,
                    format!("{}", p.log_end_offset),
                    th::text_body(),
                    false,
                ))
                .child(w::tcell(1.2, format!("{}", p.lag), lag_color, p.lag > 0))
                .child(div().flex_1().px(px(4.0)).child(w::lagbar(ratio, ptone))),
        );
    }

    let members = g
        .members
        .iter()
        .map(|m| format!("{}@{}", m.client_id, m.client_host))
        .collect::<Vec<_>>();

    w::card(
        div()
            .flex()
            .flex_col()
            .child(w::card_header(
                "◍",
                g.group.clone(),
                None,
                Some(w::AnyElementAlias(
                    w::chip(g.status_label(), state_kind).into_any_element(),
                )),
            ))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(12.0))
                    .p(px(16.0))
                    .child(hint)
                    .child(kpis)
                    .when(!members.is_empty(), |s| {
                        s.child(
                            div()
                                .flex()
                                .flex_col()
                                .gap(px(3.0))
                                .child(
                                    div()
                                        .text_size(th::text_xs())
                                        .text_color(th::text_muted())
                                        .child("成员"),
                                )
                                .child(
                                    div()
                                        .font_family("monospace")
                                        .text_size(px(10.5))
                                        .text_color(th::text_body())
                                        .child(members.join("   ")),
                                ),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .rounded(px(9.0))
                            .border_1()
                            .border_color(th::border())
                            .bg(th::bg_panel())
                            .overflow_hidden()
                            .child(parts),
                    ),
            ),
    )
    .into_any_element()
}

/// 供 main 使用：后台任务执行器类型别名。
pub type UiEntity = Entity<KafkaUi>;
