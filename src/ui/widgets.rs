//! 复用的基础控件：按钮、徽标、卡片、页头、KPI、lag 条、tab 等。
//!
//! 视觉对齐 `kafka-console` 参考设计：白色卡片 + 双色阴影 + 语义化 chip/dot。

use gpui::prelude::*;
use gpui::FontWeight;
use gpui::{div, px, App, ElementId, Entity, IntoElement, MouseButton, Rgba, SharedString, Window};

/// 全透明色（Rgba，避免 Hsla 类型不一致）。
fn transparent() -> Rgba {
    gpui::rgba(0x00_00_00_00)
}

use crate::ui::text_input::TextInput;
use crate::ui::theme as th;

/// 按钮样式。
#[derive(Clone, Copy, PartialEq)]
pub enum ButtonKind {
    Primary,
    Secondary,
    Danger,
    Ghost,
}

/// 带点击回调的按钮。
pub fn button(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    kind: ButtonKind,
    enabled: bool,
    on_click: impl Fn(&mut Window, &mut App) + 'static,
) -> impl IntoElement {
    let label: SharedString = label.into();
    let (bg, fg, border) = match kind {
        ButtonKind::Primary => (th::accent(), th::accent_text(), th::accent()),
        ButtonKind::Secondary => (th::bg_panel(), th::text(), th::border()),
        ButtonKind::Danger => (th::bg_panel(), th::error(), th::error_border()),
        ButtonKind::Ghost => (transparent(), th::text_muted(), transparent()),
    };
    let hover_bg = match kind {
        ButtonKind::Primary => th::accent_hover(),
        ButtonKind::Danger => th::error_bg(),
        _ => th::bg_hover(),
    };

    let mut el = div()
        .id(id)
        .flex()
        .items_center()
        .justify_center()
        .gap(px(6.0))
        .h(px(30.0))
        .px(px(12.0))
        .rounded(px(9.0))
        .border_1()
        .border_color(if enabled { border } else { th::border() })
        .bg(if enabled { bg } else { th::bg_hover() })
        .text_size(th::text_sm())
        .text_color(if enabled { fg } else { th::text_faint() })
        .child(label);

    if kind == ButtonKind::Primary {
        el = el.font_weight(FontWeight::SEMIBOLD);
    }
    if enabled {
        el = el
            .cursor_pointer()
            .hover(move |s| s.bg(hover_bg).border_color(th::border_strong()))
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                on_click(window, cx);
            });
    }
    el
}

/// 小尺寸按钮。
pub fn button_sm(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    kind: ButtonKind,
    enabled: bool,
    on_click: impl Fn(&mut Window, &mut App) + 'static,
) -> impl IntoElement {
    let label: SharedString = label.into();
    let (bg, fg, border) = match kind {
        ButtonKind::Primary => (th::accent(), th::accent_text(), th::accent()),
        ButtonKind::Danger => (th::bg_panel(), th::error(), th::error_border()),
        ButtonKind::Ghost => (transparent(), th::text_muted(), transparent()),
        ButtonKind::Secondary => (th::bg_panel(), th::text(), th::border()),
    };
    let mut el = div()
        .id(id)
        .flex()
        .items_center()
        .justify_center()
        .gap(px(4.0))
        .h(px(24.0))
        .px(px(8.0))
        .rounded(px(7.0))
        .border_1()
        .border_color(if enabled { border } else { th::border() })
        .bg(if enabled { bg } else { th::bg_hover() })
        .text_size(th::text_xs())
        .text_color(if enabled { fg } else { th::text_faint() })
        .child(label);
    if enabled {
        el = el
            .cursor_pointer()
            .hover(move |s| match kind {
                ButtonKind::Primary => s.bg(th::accent_hover()),
                _ => s.bg(th::bg_hover()),
            })
            .on_mouse_down(MouseButton::Left, move |_, window, cx| on_click(window, cx));
    }
    el
}

/// chip 语义。
#[derive(Clone, Copy, PartialEq)]
pub enum ChipKind {
    Neutral,
    Ok,
    Bad,
    Warn,
    Info,
    Purple,
}

fn chip_colors(kind: ChipKind) -> (Rgba, Rgba, Rgba) {
    match kind {
        ChipKind::Ok => (th::success(), th::success_bg(), th::success_border()),
        ChipKind::Bad => (th::error(), th::error_bg(), th::error_border()),
        ChipKind::Warn => (th::warn(), th::warn_bg(), th::warn_border()),
        ChipKind::Info => (th::accent(), th::accent_soft(), th::accent_border()),
        ChipKind::Purple => (th::purple(), th::purple_bg(), th::purple_border()),
        ChipKind::Neutral => (th::text_muted(), th::bg_app(), th::border()),
    }
}

/// 语义化胶囊标签。
pub fn chip(text: impl Into<SharedString>, kind: ChipKind) -> impl IntoElement {
    let (fg, bg, border) = chip_colors(kind);
    div()
        .flex()
        .items_center()
        .gap(px(5.0))
        .h(px(20.0))
        .px(px(8.0))
        .rounded(px(999.0))
        .border_1()
        .border_color(border)
        .bg(bg)
        .text_color(fg)
        .text_size(th::text_xs())
        .child(text.into())
}

/// 带圆点的 chip。
pub fn status_chip(text: impl Into<SharedString>, kind: ChipKind) -> impl IntoElement {
    let dot_color = match kind {
        ChipKind::Ok => th::success_fg(),
        ChipKind::Bad => th::error(),
        ChipKind::Warn => rgb_amber(),
        ChipKind::Info => th::accent(),
        ChipKind::Purple => th::purple(),
        ChipKind::Neutral => th::text_faint(),
    };
    let (fg, bg, border) = chip_colors(kind);
    div()
        .flex()
        .items_center()
        .gap(px(5.0))
        .h(px(20.0))
        .px(px(8.0))
        .rounded(px(999.0))
        .border_1()
        .border_color(border)
        .bg(bg)
        .text_color(fg)
        .text_size(th::text_xs())
        .child(dot(dot_color))
        .child(text.into())
}

fn rgb_amber() -> Rgba {
    gpui::rgb(0xf5_9e_0b)
}

/// 小圆点。
pub fn dot(color: Rgba) -> impl IntoElement {
    div()
        .w(px(7.0))
        .h(px(7.0))
        .rounded_full()
        .bg(color)
        .flex_shrink_0()
}

/// 小徽标（旧名兼容）。
pub fn badge(text: impl Into<SharedString>, fg: Rgba, bg: Rgba) -> impl IntoElement {
    div()
        .px(px(7.0))
        .py(px(1.0))
        .rounded(px(999.0))
        .bg(bg)
        .text_color(fg)
        .text_size(th::text_xs())
        .child(text.into())
}

/// 卡片容器（白底 / 圆角 / 边框）。
pub fn card(child: impl IntoElement) -> impl IntoElement {
    div()
        .bg(th::bg_panel())
        .border_1()
        .border_color(th::border())
        .rounded(px(14.0))
        .overflow_hidden()
        .child(child)
}

/// 卡片头部：图标 + 标题 + 说明 + 右侧操作。
pub fn card_header(
    icon: &'static str,
    title: impl Into<SharedString>,
    hint: Option<SharedString>,
    right: Option<AnyElementAlias>,
) -> impl IntoElement {
    let mut h = div()
        .flex()
        .items_center()
        .gap(px(10.0))
        .px(px(16.0))
        .py(px(12.0))
        .border_b_1()
        .border_color(th::line_2())
        .bg(th::card_header_bg())
        .child(
            div()
                .flex()
                .items_center()
                .gap(px(8.0))
                .text_size(th::text_base())
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(th::text())
                .child(
                    div()
                        .flex()
                        .items_center()
                        .justify_center()
                        .w(px(20.0))
                        .h(px(20.0))
                        .rounded(px(6.0))
                        .bg(th::accent_bg())
                        .border_1()
                        .border_color(th::accent_border())
                        .text_color(th::accent())
                        .text_size(px(10.0))
                        .child(icon),
                )
                .child(title.into()),
        );
    if let Some(hint) = hint {
        h = h.child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_muted())
                .font_weight(FontWeight::NORMAL)
                .child(hint),
        );
    }
    if let Some(right) = right {
        h = h.child(div().flex_1()).child(right.0);
    }
    h
}

/// 右侧操作容器别名。
pub struct AnyElementAlias(pub gpui::AnyElement);

/// 页头：标题 + 大写字距标签 + 说明 + 右侧动作。
pub fn page_head(
    title: impl Into<SharedString>,
    tag: Option<&'static str>,
    subtitle: impl Into<SharedString>,
    actions: Vec<gpui::AnyElement>,
) -> impl IntoElement {
    let mut left = div()
        .flex()
        .items_center()
        .gap(px(9.0))
        .text_size(th::text_lg())
        .font_weight(FontWeight::BOLD)
        .text_color(th::text())
        .child(title.into());
    if let Some(tag) = tag {
        left = left.child(
            div()
                .text_size(px(9.5))
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(th::accent())
                .bg(th::accent_soft())
                .border_1()
                .border_color(th::accent_border())
                .rounded(px(5.0))
                .px(px(6.0))
                .py(px(1.0))
                .child(tag),
        );
    }
    div()
        .flex()
        .items_end()
        .gap(px(14.0))
        .child(
            div().flex().flex_col().gap(px(2.0)).child(left).child(
                div()
                    .text_size(th::text_xs())
                    .text_color(th::text_muted())
                    .child(subtitle.into()),
            ),
        )
        .child(div().flex_1())
        .child(div().flex().items_center().gap(px(8.0)).children(actions))
}

/// KPI 块：图标块 + 大号数字 + 副行。`tone` = "" | "warn" | "bad"。
pub fn kpi(
    icon: &'static str,
    title: impl Into<SharedString>,
    value: impl Into<SharedString>,
    sub: impl Into<SharedString>,
    tone: &'static str,
) -> impl IntoElement {
    let (ico_fg, ico_bg, ico_border, val_color) = match tone {
        "warn" => (th::warn(), th::warn_bg(), th::warn_border(), th::warn()),
        "bad" => (th::error(), th::error_bg(), th::error_border(), th::error()),
        _ => (
            th::accent(),
            th::accent_bg(),
            th::accent_border(),
            th::text(),
        ),
    };
    div()
        .flex()
        .items_start()
        .gap(px(11.0))
        .flex_1()
        .min_w(px(0.0))
        .p(px(12.0))
        .rounded(px(14.0))
        .border_1()
        .border_color(th::border())
        .bg(th::bg_panel())
        .child(
            div()
                .flex()
                .items_center()
                .justify_center()
                .w(px(28.0))
                .h(px(28.0))
                .flex_shrink_0()
                .rounded(px(8.0))
                .bg(ico_bg)
                .border_1()
                .border_color(ico_border)
                .text_color(ico_fg)
                .text_size(px(12.0))
                .child(icon),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .min_w(px(0.0))
                .child(
                    div()
                        .text_size(th::text_xs())
                        .text_color(th::text_muted())
                        .child(title.into()),
                )
                .child(
                    div()
                        .text_size(th::text_kpi())
                        .font_weight(FontWeight::BOLD)
                        .text_color(val_color)
                        .child(value.into()),
                )
                .child(
                    div()
                        .text_size(th::text_xs())
                        .text_color(th::text_faint())
                        .child(sub.into()),
                ),
        )
}

/// lag 进度条。`tone` = "" | "ok" | "bad"。
pub fn lagbar(ratio: f32, tone: &'static str) -> impl IntoElement {
    let color = match tone {
        "ok" => th::success_fg(),
        "bad" => th::error(),
        _ => rgb_amber(),
    };
    let ratio = ratio.clamp(0.0, 1.0);
    div()
        .h(px(6.0))
        .w_full()
        .min_w(px(70.0))
        .rounded(px(4.0))
        .bg(th::line_2())
        .overflow_hidden()
        .child(
            div()
                .h(px(6.0))
                .rounded(px(4.0))
                .bg(color)
                .w(gpui::relative(ratio.max(0.02))),
        )
}

/// 空态占位。
pub fn empty_state(
    icon: &'static str,
    title: impl Into<SharedString>,
    hint: impl Into<SharedString>,
) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .items_center()
        .justify_center()
        .gap(px(8.0))
        .py(px(38.0))
        .child(
            div()
                .flex()
                .items_center()
                .justify_center()
                .w(px(40.0))
                .h(px(40.0))
                .rounded(px(12.0))
                .border_1()
                .border_color(th::border())
                .bg(th::accent_bg())
                .text_size(px(16.0))
                .text_color(th::text_faint())
                .child(icon),
        )
        .child(
            div()
                .text_size(th::text_base())
                .text_color(th::text_muted())
                .child(title.into()),
        )
        .child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_faint())
                .child(hint.into()),
        )
}

/// 提示条：`success` / `error` / `info` / `warn`。
pub fn banner(kind: &str, text: impl Into<SharedString>) -> impl IntoElement {
    let (fg, bg, border) = match kind {
        "success" => (th::success(), th::success_bg(), th::success_border()),
        "error" => (th::error(), th::error_bg(), th::error_border()),
        "warn" => (th::warn(), th::warn_bg(), th::warn_border()),
        _ => (th::text_muted(), th::bg_hover(), th::border()),
    };
    div()
        .px(px(12.0))
        .py(px(9.0))
        .rounded(px(9.0))
        .border_1()
        .border_color(border)
        .bg(bg)
        .text_color(fg)
        .text_size(th::text_sm())
        .child(text.into())
}

/// 加载中占位。
pub fn loading(text: impl Into<SharedString>) -> impl IntoElement {
    div()
        .flex()
        .items_center()
        .justify_center()
        .py(px(28.0))
        .gap(px(8.0))
        .text_size(th::text_sm())
        .text_color(th::text_faint())
        .child("⏳")
        .child(text.into())
}

/// 分割线。
pub fn divider() -> impl IntoElement {
    div().h(px(1.0)).w_full().bg(th::line_2())
}

// ===========================================================================
// 组合控件
// ===========================================================================

/// 带标签的输入框。
pub fn field(label: impl Into<SharedString>, input: Entity<TextInput>) -> impl IntoElement {
    field_with_help(label, input, None)
}

/// 带标签 + 说明的输入框。
pub fn field_with_help(
    label: impl Into<SharedString>,
    input: Entity<TextInput>,
    help: Option<&'static str>,
) -> impl IntoElement {
    let mut col = div()
        .flex()
        .flex_col()
        .gap(px(5.0))
        .min_w(px(0.0))
        .child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_muted())
                .child(label.into()),
        )
        .child(
            div()
                .h(px(32.0))
                .px(px(9.0))
                .flex()
                .items_center()
                .rounded(px(9.0))
                .border_1()
                .border_color(th::border())
                .bg(th::bg_panel())
                .child(input),
        );
    if let Some(help) = help {
        col = col.child(
            div()
                .text_size(px(9.5))
                .text_color(th::text_faint())
                .child(help),
        );
    }
    col
}

/// 横向分段选择（胶囊 tab）。选中项回调 idx。
pub fn seg_tabs(
    id_prefix: &'static str,
    options: Vec<SharedString>,
    selected: usize,
    on_pick: impl Fn(usize, &mut App) + Clone + 'static,
) -> impl IntoElement {
    let mut row = div()
        .flex()
        .items_center()
        .gap(px(2.0))
        .p(px(3.0))
        .rounded(px(9.0))
        .bg(th::bg_app())
        .border_1()
        .border_color(th::border());
    for (idx, text) in options.into_iter().enumerate() {
        let sel = idx == selected;
        let cb = on_pick.clone();
        row = row.child(
            div()
                .id(SharedString::from(format!("{id_prefix}-{idx}")))
                .px(px(11.0))
                .py(px(4.0))
                .rounded(px(7.0))
                .when(sel, |s| {
                    s.bg(th::bg_panel())
                        .text_color(th::accent())
                        .font_weight(FontWeight::SEMIBOLD)
                })
                .when(!sel, |s| s.text_color(th::text_muted()))
                .text_size(th::text_sm())
                .cursor_pointer()
                .on_mouse_down(MouseButton::Left, move |_, _, cx| cb(idx, cx))
                .child(text),
        );
    }
    row
}

/// 横向单选按钮组（旧名兼容）。
pub fn choice_row(
    label: &'static str,
    options: Vec<&'static str>,
    selected: usize,
    on_pick: impl Fn(usize, &mut App) + Clone + 'static,
) -> impl IntoElement {
    let mut row = div()
        .flex()
        .items_center()
        .flex_wrap()
        .gap(px(6.0))
        .py(px(2.0))
        .child(
            div()
                .w(px(96.0))
                .text_size(th::text_xs())
                .text_color(th::text_muted())
                .child(label),
        );
    for (idx, text) in options.into_iter().enumerate() {
        let sel = idx == selected;
        let cb = on_pick.clone();
        row = row.child(
            div()
                .id(SharedString::from(format!("{label}-{text}")))
                .px(px(9.0))
                .py(px(3.0))
                .rounded(px(7.0))
                .border_1()
                .border_color(if sel { th::accent() } else { th::border() })
                .bg(if sel { th::accent() } else { th::bg_panel() })
                .text_size(th::text_xs())
                .text_color(if sel {
                    th::accent_text()
                } else {
                    th::text_muted()
                })
                .cursor_pointer()
                .on_mouse_down(MouseButton::Left, move |_, _, cx| cb(idx, cx))
                .child(text),
        );
    }
    row
}

/// 数字步进器。
pub fn stepper(
    label: &'static str,
    value: i32,
    on_delta: impl Fn(i32, &mut App) + Clone + 'static,
) -> impl IntoElement {
    let minus = on_delta.clone();
    let plus = on_delta.clone();
    div()
        .flex()
        .items_center()
        .gap(px(3.0))
        .child(
            div()
                .text_size(th::text_xs())
                .text_color(th::text_muted())
                .child(label),
        )
        .child(button(
            "st-m",
            "−",
            ButtonKind::Ghost,
            value > 1,
            move |_, cx| minus(-1, cx),
        ))
        .child(
            div()
                .w(px(28.0))
                .flex()
                .justify_center()
                .text_size(th::text_sm())
                .child(format!("{value}")),
        )
        .child(button(
            "st-p",
            "＋",
            ButtonKind::Ghost,
            value < 500,
            move |_, cx| plus(1, cx),
        ))
}

/// 表格表头。`cols` 为 (标题, 权重)。
pub fn table_header(cols: &[(&'static str, f32)]) -> impl IntoElement {
    let mut h = div()
        .flex()
        .items_center()
        .px(px(16.0))
        .py(px(8.0))
        .bg(th::table_head_bg())
        .border_b_1()
        .border_color(th::border())
        .text_size(th::text_xs())
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(th::text_muted());
    for (title, weight) in cols {
        let mut c = div().min_w_0().px(px(4.0)).child(*title);
        if *weight >= 1.0 {
            c = c.flex_1();
        } else {
            c = c.w(px(weight * 70.0)).flex_shrink_0();
        }
        h = h.child(c);
    }
    h
}

/// 表格单元格。
pub fn tcell(
    weight: f32,
    text: impl Into<SharedString>,
    color: Rgba,
    bold: bool,
) -> impl IntoElement {
    let mut d = div()
        .min_w_0()
        .px(px(4.0))
        .overflow_hidden()
        .text_size(th::text_sm())
        .text_color(color)
        .child(text.into());
    if weight >= 1.0 {
        d = d.flex_1();
    } else {
        d = d.w(px(weight * 70.0)).flex_shrink_0();
    }
    if bold {
        d = d.font_weight(FontWeight::SEMIBOLD);
    }
    d
}

/// 等宽数字文本。
pub fn mono(text: impl Into<SharedString>, color: Rgba) -> impl IntoElement {
    div()
        .font_family("monospace")
        .text_size(th::text_xs())
        .text_color(color)
        .child(text.into())
}

/// 文本辅助。
pub fn h(text: impl Into<SharedString>, _size: gpui::Pixels) -> SharedString {
    text.into()
}
