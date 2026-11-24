//! Kafka UI 桌面应用入口。

// release 构建在 Windows 下不弹出控制台窗口（debug 保留控制台方便看日志）。
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // 日志：KAFKA_UI_LOG 控制级别，默认 info
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("kafka_ui=info,warn")),
        )
        .init();

    tracing::info!(
        "Kafka UI 启动，配置文件: {}",
        kafka_ui::store::Store::config_path().display()
    );
    kafka_ui::ui::run();
}
