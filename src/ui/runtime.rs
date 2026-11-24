//! GPUI ↔ Tokio 桥接。
//!
//! GPUI 主线程有自己的执行器，而 `kafka_client` 是 async 的，且不能在 GPUI
//! 任务里直接 `tokio::spawn`（GPUI 的 local runtime 会安装自己的 tokio 上下文）。
//!
//! 做法：
//! 1. 后台线程常驻一个多线程 tokio runtime，接收任务并执行；
//! 2. UI 侧用 `Context::spawn` 起一个 GPUI 任务，awaits 一个 `tokio::sync::oneshot`；
//!    该 oneshot 由后台任务完成后发送（`tokio::sync::oneshot::Receiver` 在 GPUI
//!    执行器里被 poll 时是纯内存操作，不依赖 tokio 定时器/IO 驱动）；
//! 3. 结果回来后直接在 UI 线程更新实体。

use std::future::Future;
use std::sync::{mpsc, OnceLock};

use gpui::Context;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Backend {
    tx: mpsc::Sender<Job>,
}

fn backend() -> &'static Backend {
    static BACKEND: OnceLock<Backend> = OnceLock::new();
    BACKEND.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("kafka-ui-tokio".into())
            .spawn(move || {
                let rt = Runtime::new().expect("无法创建 tokio runtime");
                for job in rx {
                    rt.spawn(async move { job() });
                }
            })
            .expect("无法启动 tokio 线程");
        Backend { tx }
    })
}

/// 在后台 runtime 执行 `work`，完成后在 UI 线程调用 `done` 并刷新界面。
pub fn run_task<T, R, F>(
    work: F,
    cx: &mut Context<T>,
    done: impl FnOnce(&mut T, &mut Context<T>, Result<R, String>) + 'static,
) where
    T: 'static,
    R: Send + 'static,
    F: Future<Output = Result<R, String>> + Send + 'static,
{
    let (otx, orx) = oneshot::channel::<Result<R, String>>();
    let entity = cx.entity().downgrade();

    if backend()
        .tx
        .send(Box::new(move || {
            tokio::spawn(async move {
                let _ = otx.send(work.await);
            });
        }))
        .is_err()
    {
        tracing::error!("后台 runtime 已退出，任务被丢弃");
        return;
    }

    cx.spawn(async move |_, cx| {
        let res = match orx.await {
            Ok(r) => r,
            Err(_) => Err("后台任务被取消".to_string()),
        };
        entity
            .update(cx, |this, cx| {
                done(this, cx, res);
                cx.notify();
            })
            .ok();
    })
    .detach();
}
