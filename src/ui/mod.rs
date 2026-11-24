//! UI 层：基于 GPUI 的桌面界面。

pub mod app;
pub mod runtime;
pub mod text_input;
pub mod theme;
pub mod widgets;

use gpui::prelude::*;
use gpui::{px, size, App, Bounds, TitlebarOptions, WindowBounds, WindowOptions};

/// 启动 GPUI 应用（阻塞直到所有窗口关闭）。
pub fn run() {
    gpui::Application::new().run(|cx: &mut App| {
        text_input::bind_keys(cx);

        let bounds = Bounds::centered(None, size(px(1280.0), px(820.0)), cx);
        let window_options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            titlebar: Some(TitlebarOptions {
                title: Some("Kafka UI".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        cx.open_window(window_options, |window, cx| {
            let view = cx.new(|cx| app::KafkaUi::new(window, cx));
            // 默认选中第一个集群后尝试加载 Topic（有配置时）
            let weak = view.downgrade();
            cx.spawn(async move |cx| {
                weak.update(cx, |ui, cx| {
                    if ui.selected_config().is_some() {
                        ui.load_topics(cx);
                    }
                })
                .ok();
            })
            .detach();
            view
        })
        .expect("无法创建窗口");

        cx.activate(true);
    });
}
