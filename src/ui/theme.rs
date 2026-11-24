//! 统一配色与尺寸，避免各处散落魔法值。
//!
//! 取值对齐 `kafka-console` 参考设计（浅色运维台）：
//! 冷白背景 + 白色卡片 + 蓝色主色 + 绿/黄/红/紫语义色。
//!
//! 颜色用 `fn` 而非 `const`（`gpui::rgb` 不是 const fn）。

use gpui::{px, rgb, Rgba};

// ── 背景 / 表面 ─────────────────────────────────────────────
pub fn bg_app() -> Rgba {
    rgb(0xf5_f8_fc)
}
pub fn bg_panel() -> Rgba {
    rgb(0xff_ff_ff)
}
pub fn bg_hover() -> Rgba {
    rgb(0xf4_f8_ff)
}
pub fn bg_active() -> Rgba {
    rgb(0xf0_f7_ff)
}
pub fn card_header_bg() -> Rgba {
    rgb(0xfd_fe_ff)
}
pub fn table_head_bg() -> Rgba {
    rgb(0xf7_f9_fd)
}

// ── 线条 ────────────────────────────────────────────────────
pub fn border() -> Rgba {
    rgb(0xe6_ec_f5)
}
pub fn border_strong() -> Rgba {
    rgb(0xcf_dc_f0)
}
pub fn line_2() -> Rgba {
    rgb(0xee_f2_f8)
}
pub fn border_blue() -> Rgba {
    rgb(0xa9_c8_fd)
}

// ── 文字 ────────────────────────────────────────────────────
pub fn text() -> Rgba {
    rgb(0x0d_15_26)
}
pub fn text_body() -> Rgba {
    rgb(0x33_41_55)
}
pub fn text_muted() -> Rgba {
    rgb(0x64_74_8b)
}
pub fn text_faint() -> Rgba {
    rgb(0x94_a3_b8)
}

// ── 主色（蓝）──────────────────────────────────────────────
pub fn accent() -> Rgba {
    rgb(0x25_63_eb)
}
pub fn accent_2() -> Rgba {
    rgb(0x3b_82_f6)
}
pub fn accent_hover() -> Rgba {
    rgb(0x1d_4e_d8)
}
pub fn accent_text() -> Rgba {
    rgb(0xff_ff_ff)
}
pub fn accent_soft() -> Rgba {
    rgb(0xe8_f0_ff)
}
pub fn accent_bg() -> Rgba {
    rgb(0xf2_f7_ff)
}
pub fn accent_border() -> Rgba {
    rgb(0xdb_e8_fb)
}

// ── 语义色：绿（正常）/ 黄（注意）/ 红（异常）/ 紫（topic 标记）
pub fn success() -> Rgba {
    rgb(0x0f_7a_3d)
}
pub fn success_fg() -> Rgba {
    rgb(0x15_a3_4a)
}
pub fn success_bg() -> Rgba {
    rgb(0xe3_f8_ec)
}
pub fn success_border() -> Rgba {
    rgb(0xc3_ec_d3)
}

pub fn warn() -> Rgba {
    rgb(0x9a_5b_06)
}
pub fn warn_bg() -> Rgba {
    rgb(0xfd_f3_dc)
}
pub fn warn_border() -> Rgba {
    rgb(0xf3_df_b0)
}

pub fn error() -> Rgba {
    rgb(0xb4_23_18)
}
pub fn error_bg() -> Rgba {
    rgb(0xfd_e8_e6)
}
pub fn error_border() -> Rgba {
    rgb(0xf6_c9_c4)
}

pub fn purple() -> Rgba {
    rgb(0x6d_28_d9)
}
pub fn purple_bg() -> Rgba {
    rgb(0xf0_ea_fe)
}
pub fn purple_border() -> Rgba {
    rgb(0xe0_d5_fb)
}

// ── 兼容旧调用 ──────────────────────────────────────────────
pub fn sidebar_bg() -> Rgba {
    rgb(0xff_ff_ff)
}
pub fn row_alt() -> Rgba {
    rgb(0xfb_fd_ff)
}
pub fn row_hover() -> Rgba {
    rgb(0xf6_fa_ff)
}
pub fn json_box_bg() -> Rgba {
    rgb(0x0b_12_20)
}
pub fn json_text() -> Rgba {
    rgb(0xdb_ea_fe)
}

// ── 字号 ────────────────────────────────────────────────────
pub fn text_xs() -> gpui::Pixels {
    px(10.5)
}
pub fn text_sm() -> gpui::Pixels {
    px(11.8)
}
pub fn text_base() -> gpui::Pixels {
    px(12.8)
}
pub fn text_md() -> gpui::Pixels {
    px(13.0)
}
pub fn text_lg() -> gpui::Pixels {
    px(16.0)
}
pub fn text_xl() -> gpui::Pixels {
    px(21.0)
}
pub fn text_kpi() -> gpui::Pixels {
    px(21.0)
}
