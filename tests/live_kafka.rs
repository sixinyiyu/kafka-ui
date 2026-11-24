//! 端到端集成测试（需可访问的 Kafka 集群）。
//!
//! 默认连 `10.57.180.150:9092`，可用 `KAFKA_BOOTSTRAP` 覆盖：
//! ```sh
//! cargo test --test live_kafka -- --ignored --nocapture
//! ```
//!
//! 覆盖：连接测试 → 建 topic → 发消息 → 拉最近 N 条 → topic 详情 → 消费组 → 删 topic。

use std::time::Duration;

use kafka_ui::kafka::client::{test_connection, ClusterConfig, SecurityProtocolKind};
use kafka_ui::kafka::{build_client, consumer, messages, producer, topics};

fn cfg() -> ClusterConfig {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "10.57.180.150:9092".to_string());
    ClusterConfig {
        id: "live".into(),
        name: "live-test".into(),
        bootstrap_servers: bootstrap,
        security_protocol: SecurityProtocolKind::Plaintext,
        ..ClusterConfig::default()
    }
}

#[tokio::test]
#[ignore]
async fn end_to_end_against_live_kafka() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let cfg = cfg();

    // 1. 连接测试
    let t = test_connection(&cfg).await;
    println!(
        "[连接] ok={} msg={} brokers={}",
        t.ok, t.message, t.broker_count
    );
    assert!(t.ok, "连接失败: {}", t.message);
    assert!(t.broker_count >= 1);

    let client = build_client(&cfg).await.expect("build client");

    // 2. 建 topic（带分区/副本）
    let topic_name = format!(
        "ui-it-{}-{}",
        std::process::id(),
        chrono::Local::now().format("%H%M%S")
    );
    let spec = topics::CreateTopicSpec {
        name: topic_name.clone(),
        num_partitions: Some(3),
        replication_factor: Some(1),
        configs: Vec::new(),
    };
    let (existed, msg) = topics::create_topic(&client.admin(), &spec)
        .await
        .expect("create");
    println!("[建 topic] existed={} {}", existed, msg);
    assert!(!existed);

    // 3. 发消息
    for i in 0..5 {
        let s = producer::SendSpec {
            topic: topic_name.clone(),
            key: format!("k{}", i),
            value: format!(r#"{{"n":{},"hello":"world"}}"#, i),
            partition: None,
            headers: vec![("source".into(), "kafka-ui-test".into())],
        };
        let out = producer::send_message(&client, &s).await.expect("send");
        println!("[发送] partition={} offset={}", out.partition, out.offset);
        // 每个分区发送后立即查看 LEO，便于定位丢失
        let e = topics::log_end_offsets(&client, &topic_name)
            .await
            .unwrap_or_default();
        println!("    立即 LEO={:?}", e);
        assert_eq!(out.topic, topic_name);
    }

    // 4. 列 topic
    let list = topics::list_topics(&client.admin()).await.expect("list");
    println!("[列表] {} 个 topic", list.len());
    assert!(list.iter().any(|x| x.name == topic_name));

    // 5. 拉最近 N 条（含 headers）
    let fetched = messages::fetch_latest(&client, &topic_name, 10, Duration::from_secs(8))
        .await
        .expect("fetch");
    println!(
        "[拉取] {} 条, ends={:?}, elapsed={}ms",
        fetched.messages.len(),
        fetched.ends,
        fetched.elapsed_ms
    );
    assert_eq!(fetched.messages.len(), 5, "应拉到 5 条");
    let last = fetched.messages.last().unwrap();
    assert!(last.value.contains("hello"));
    assert!(!last.headers.is_empty(), "headers 应保留");

    // 6. topic 详情（分区 LEO）
    let detail = topics::describe_topic(&client, &topic_name)
        .await
        .expect("describe");
    println!("[详情] 分区数={}", detail.partitions.len());
    assert_eq!(detail.partitions.len(), 3);
    let total: i64 = detail
        .partitions
        .iter()
        .filter_map(|p| p.log_end_offset)
        .sum();
    assert_eq!(total, 5);

    // 7. 消费组查询（无组时应为空而不报错）
    let groups = consumer::topic_consumers(&client.admin(), Some(&topic_name))
        .await
        .expect("groups");
    println!("[消费组] {} 个", groups.len());

    // 8. 删除 topic
    let del = topics::delete_topic(&client.admin(), &topic_name)
        .await
        .expect("delete");
    println!("[删除] {}", del);

    let _ = client.close().await;
    println!("✅ 端到端测试全部通过");
}
