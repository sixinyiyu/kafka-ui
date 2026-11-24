# Kafka UI（GPUI 桌面版）

基于 **Rust + [GPUI](https://crates.io/crates/gpui)（Zed 的 GPU 加速 UI 框架）+ [kafka_client](https://crates.io/crates/kafka_client)** 的
Kafka 图形化管理工具。纯 Rust、原生桌面窗口，无需浏览器。

Kafka 客户端用法参考 [kafka-proxy](https://github.com/sixinyiyu/kafka-proxy)（`kafka_client` 的 `Client` / `AdminClient` / `Producer` / `Consumer`）。

## 功能

界面参考 `kafka-console` 设计：浅色运维控制台风格 —— 左侧 218px 分组导航（连接 / 集群 / 数据）+ 顶部 56px 顶栏（集群切换、连接状态、刷新）+ 内容区，共六个页面。

| 页面 | 说明 |
| --- | --- |
| **集群连接** | 左侧连接列表 + 右侧编辑表单。名称 / Bootstrap Servers / 认证（安全协议 PLAINTEXT·SSL·SASL_PLAINTEXT·SASL_SSL）× SASL 机制（PLAIN·SCRAM-SHA-256/512·GSSAPI Kerberos）/ 备注；认证 × 协议冲突实时提示；一键测试连接 |
| **Topics** | KPI（Topic 总数 / 分区总数）+ 关键字过滤；点击行**展开分区树**（Partition / Leader / Replicas / ISR / LEO，副本未同步标黄），详情按需加载；行内「拉取」跳转；新建 / 删除 Topic |
| **Brokers** | KPI（Broker 数 / Controller）+ Broker 列表（id / 主机 / 端口 / 角色）+ 分区承载分布条形图 |
| **拉取消息** | 选择 Topic + 每分区最近条数；表格展示 partition / offset / 时间 / key / 内容，点击行展开 headers 与格式化 JSON。只读，不提交位点 |
| **模拟发送** | 选择目标 Topic、Key、Value 发送；回执显示 partition / offset / 耗时；保留本会话发送历史 |
| **消费组** | 左侧消费组列表（lag 按严重度染色）+ 右侧详情：总 lag / 成员 / 分区数 KPI、Empty / Rebalance 提示条、分区 lag 表（当前位点 / 末尾位点 / lag / 占比条） |

## 设计要点

```
src/
├── kafka/            # 业务层，与 UI 完全解耦，全部是 async fn
│   ├── client.rs     # 连接配置 → kafka_client::ClientBuilder（TLS / SASL / Kerberos）
│   ├── topics.rs     # Topic 列表 / 详情 / 创建 / 删除 / LEO 查询
│   ├── messages.rs   # 拉取最近 N 条消息
│   ├── producer.rs   # 发送消息
│   └── consumer.rs   # 消费组与 lag
├── store.rs          # 连接配置持久化（YAML）
└── ui/               # GPUI 界面
    ├── app.rs        # 主窗口：分组导航 + 六个页面
    ├── text_input.rs # 单行输入框（基于 EntityInputHandler）
    ├── runtime.rs    # GPUI ↔ Tokio 桥接
    ├── widgets.rs    # 卡片 / KPI / chip / lag 条 / 表格等
    └── theme.rs      # 配色（对齐 kafka-console 浅色主题）
```

### 几个关键实现

1. **必须发给分区 leader**：`ListOffsets`（查 LEO）只有分区 leader 会正确应答。
   把全部分区塞进一个「发往任意 broker」的请求时，非该 broker 领导的分区会返回
   `NOT_LEADER(6)` 被静默丢弃——表现为多分区 Topic 只能看到部分分区的数据。
   本项目按 leader 分组，**直连各 leader 发送**（`connection::Builder::build_sequential`）。
2. **发送走 `send_direct`**：`kafka_client` 在 `linger_ms = 0` 时会 panic
   （`tokio::time::interval` 要求 period 非零），且新 Topic 的元数据可能尚未缓存，
   因此发送前先 `describe_topics` + `refresh_metadata`，再用 `send_direct` 立即发送。
3. **UI 不被 Kafka IO 阻塞**：后台常驻一个多线程 Tokio runtime，UI 任务通过
   `oneshot` 等待结果后回到 GPUI 主线程更新实体。

## 运行

```bash
cargo run --release
```

连接配置文件默认位于 `$XDG_CONFIG_HOME/kafka-ui/clusters.yaml`
（可用环境变量 `KAFKA_UI_CONFIG` 指定路径）。首次启动后在「集群连接」页新增集群即可。

日志级别可用 `RUST_LOG` 控制：

```bash
RUST_LOG=kafka_ui=debug cargo run
```

## Windows 构建与打包安装包

GPUI 依赖 Windows + MSVC 与本机图形栈，**无法从 Linux 交叉编译**，需要在 Windows 机器
（或 GitHub Actions 的 `windows-latest`）上原生构建。

### 方式一：在 Windows 机器上（一键脚本）

前置：安装 Rust（MSVC 工具链，rustup 默认即是）与 Visual Studio C++ 生成工具；如需生成
安装包再装 [NSIS](https://nsis.sourceforge.io)（`choco install nsis`）。

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-windows.ps1
# 只出 exe + zip、不装 NSIS 时：
powershell -ExecutionPolicy Bypass -File .\scripts\build-windows.ps1 -SkipInstaller
```

产物（`dist\`）：
- `kafka-ui.exe`：单文件可执行；
- `kafka-ui-<version>.zip`：免安装压缩包；
- `kafka-ui-<version>-setup.exe`：NSIS 安装包（写开始菜单 / 桌面快捷方式 / 卸载项）。

> 首次构建会从 crates.io 拉取 `gpui = "0.2"` 及其依赖，需能访问 crates.io。
> 仓库默认 `Cargo.toml` 即为 Windows 可用的可移植清单，直接构建即可。

### 方式二：GitHub Actions 流水线（推荐，无需本地 Windows）

流水线文件：`.github/workflows/windows-build.yml`（**Windows CI**），在 `windows-latest`
上依次执行：

```
prepare（读取版本号）
  └─ check：cargo fmt --check → cargo clippy --no-deps -D warnings → cargo test --lib
       └─ build：cargo build --release → NSIS 打包（exe / zip / setup.exe）
            └─ release：创建 GitHub Release 并上传安装包（仅特定触发）
```

> CI 直接使用仓库默认的 `Cargo.toml`（`gpui = "0.2"`，crates.io），会自动从 crates.io 拉取依赖。

**如何触发流水线：**

| 触发方式 | 操作 | 结果 |
| --- | --- | --- |
| **手动触发** | 仓库 → **Actions** → 左侧 **Windows CI** → **Run workflow** | 选择分支，可选填 `version`；勾选 `release` 则额外发布 Release |
| **推送代码** | 推送到 `main` 且改动 `src/**`、`Cargo*`、`scripts/**`、`packaging/**` 等 | 自动跑 fmt/clippy/test/build，产物上传为 artifact |
| **提交 PR** | 向 `main` 提 PR | 自动跑 fmt/clippy/test/build 做校验 |
| **打 tag** | `git tag v0.1.0 && git push origin v0.1.0` | 自动构建并创建 GitHub Release，附带 `setup.exe` / `zip` |

**获取构建产物：**
- 普通触发：Actions 运行详情页底部 **Artifacts** 下载 `kafka-ui-windows-<版本>`（含 `kafka-ui.exe`、`zip`、`setup.exe`）；
- Release 触发：仓库 **Releases** 页面直接下载安装包。

本地等价命令（在仓库根目录，使用可移植清单）：
```bash
cp Cargo.portable.toml Cargo.toml
cargo fmt --all -- --check
cargo clippy --all-targets --no-deps -- -D warnings
cargo test --lib
```

### GPUI 依赖说明

GPUI 已发布到 crates.io（Zed 官方 GPU 加速 UI 框架）：<https://crates.io/crates/gpui>。
本仓库直接使用正式版本：

```toml
gpui = "0.2"
```

> 默认特性已含 Linux 的 `wayland` / `x11` 与 Windows 的 `windows-manifest`，跨平台开箱即用。
> 无需 git 依赖、无需 `[patch]`，`cargo build` 会自动从 crates.io 拉取。

`Cargo.lock` 不纳入版本控制（保证不同环境可解析各自可用的依赖版本）。

### 内网离线构建（可选）

若目标机器**无法访问 crates.io / GitHub**，可改用本地 vendored 的 zed 源码清单
`Cargo.offline.toml`（`gpui` 指向本地源码 + 本地 blade/font-kit）：

| 文件 | 用途 |
| --- | --- |
| `Cargo.toml` | **默认**：`gpui = "0.2"`（crates.io 正式版，提交到仓库的就是这份） |
| `Cargo.portable.toml` | 与 `Cargo.toml` 相同的副本，供脚本切换 |
| `Cargo.offline.toml` | 内网离线：`gpui = file:///tmp/zed-src/...` + `[patch]` 本地 blade/font-kit |

```bash
scripts/use-offline.sh     # 切到本地化依赖（内网离线构建）
scripts/use-portable.sh    # 切回 crates.io 正式版
```

### 运行时依赖（Linux）

GPUI 的 GPU 后端使用 Vulkan，需要 `libvulkan.so.1`；X11 窗口还需要
`libxkbcommon-x11`：

```bash
sudo yum install -y vulkan-loader mesa-vulkan-drivers libxkbcommon-x11 libxkbcommon-x11-devel
```

## 测试

```bash
# 单元测试（配置解析、存储、消息格式化等）
cargo test --lib

# 端到端测试（需要可访问的 Kafka，默认 10.57.180.150:9092）
KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo test --test live_kafka -- --ignored --nocapture
```

端到端测试覆盖：连接测试 → 建 Topic → 发消息 → 拉取最近消息 → Topic 详情(LEO)
→ 消费组查询 → 删除 Topic。

## 系统依赖（Linux）

GPUI 需要 X11/Wayland 开发库与字体库：

```bash
# Debian/Ubuntu
sudo apt install libxkbcommon-dev libwayland-dev libx11-xcb-dev libfontconfig-dev libfreetype-dev
# RHEL/CentOS
sudo yum install libxkbcommon-devel wayland-devel libxcb-devel fontconfig-devel freetype-devel
