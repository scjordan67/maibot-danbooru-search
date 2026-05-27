"""
danbooru-search — Danbooru 图片搜索插件

功能：
  - @Tool  search_danbooru  : LLM 可调用的 Tag 搜索工具
  - @Command /danbooru      : 用户直接发送 /danbooru <tags> 触发搜索

搜索流程：
  1. 向 Danbooru REST API 发起请求（可匿名，也可配置 API Key）
  2. 按评级过滤帖子（由 config.toml 控制）
  3. 随机抽取 send_count 张，下载图片并 base64 编码
  4. 通过 ctx.send.image() 发送给用户
"""

from __future__ import annotations

import asyncio
import base64
import random
import re
from typing import Any

import aiohttp

from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

# ─── Danbooru API ─────────────────────────────────────────────────────────────

_DANBOORU_BASE = "https://danbooru.donmai.us"
_POSTS_ENDPOINT = f"{_DANBOORU_BASE}/posts.json"

# 评级映射（Danbooru API 返回单字母）
_RATING_MAP = {"g": "通用", "s": "敏感", "q": "可疑", "e": "明确"}


# ─── 配置模型 ─────────────────────────────────────────────────────────────────


class DanbooruSection(PluginConfigBase):
    """Danbooru 搜索配置（对应 config.toml 的 [danbooru] 节）"""

    __ui_label__ = "Danbooru 搜索设置"

    login: str = Field(default="", description="Danbooru 登录名（可选，不填则匿名）")
    api_key: str = Field(default="", description="Danbooru API Key（可选）")
    allowed_ratings: str = Field(
        default="g,s",
        description="允许的图片评级，多值用逗号分隔：g=通用, s=敏感, q=可疑, e=明确",
    )
    max_results: int = Field(
        default=20,
        description="每次 API 请求最多获取的帖子数（1-200）",
    )
    send_count: int = Field(
        default=1,
        description="每次命令/工具调用发送的图片数量",
    )
    use_preview: bool = Field(
        default=False,
        description="是否使用缩略图（更快）而非原图",
    )
    download_timeout: int = Field(
        default=15,
        description="图片下载超时时间（秒）",
    )


class DanbooruPluginConfig(PluginConfigBase):
    """插件根配置（对应 config.toml 整体结构）"""

    danbooru: DanbooruSection = Field(default_factory=DanbooruSection)


# ─── 插件主体 ─────────────────────────────────────────────────────────────────


class DanbooruSearchPlugin(MaiBotPlugin):
    """通过 Tag 从 Danbooru 搜索并发送图片的 MaiBot 插件。"""

    config_model = DanbooruPluginConfig

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    async def on_load(self) -> None:
        self.ctx.logger.info("Danbooru 搜索插件已加载")

    async def on_unload(self) -> None:
        self.ctx.logger.info("Danbooru 搜索插件已卸载")

    async def on_config_update(
        self, scope: str, config_data: dict, version: str
    ) -> None:
        self.ctx.logger.info("配置已更新 (scope=%s, version=%s)", scope, version)

    # ── @Tool —— LLM 可调用 ───────────────────────────────────────────────────

    @Tool(
        "search_danbooru",
        brief_description="通过 Tag 在 Danbooru 上搜索插画并发送图片",
        detailed_description=(
            "从 Danbooru 图片站按 Tag 搜索插画，随机选取结果并发送给用户。\n"
            "参数说明：\n"
            "- tags：string，必填。Danbooru 格式的 Tag，多个 Tag 用空格分隔。\n"
            "  例如：'hatsune_miku solo' 或 'blue_archive rating:g'。\n"
            "- count：integer，可选（默认 1）。发送图片数量，上限为配置中的 send_count。\n"
            "- stream_id：string，必填。当前聊天流 ID。"
        ),
        parameters=[
            ToolParameterInfo(
                name="tags",
                param_type=ToolParamType.STRING,
                description="Danbooru Tag，多个用空格分隔，例如 'hatsune_miku solo'",
                required=True,
            ),
            ToolParameterInfo(
                name="count",
                param_type=ToolParamType.INTEGER,
                description="发送图片数量（默认 1）",
                required=False,
                default=1,
            ),
            ToolParameterInfo(
                name="stream_id",
                param_type=ToolParamType.STRING,
                description="当前聊天流 ID",
                required=True,
            ),
        ],
    )
    async def handle_tool_search(
        self, tags: str, stream_id: str, count: int = 1, **kwargs: Any
    ) -> dict:
        cfg = self.config.danbooru
        send_n = min(count, cfg.send_count, 10)  # 安全上限 10

        posts = await self._fetch_posts(tags, cfg)
        if not posts:
            await self.ctx.send.text(
                f"没有找到符合条件的图片（tags: {tags}）", stream_id
            )
            return {"success": False, "message": "未找到结果", "tags": tags}

        selected = random.sample(posts, min(send_n, len(posts)))
        sent_ids: list[int] = []

        for post in selected:
            ok = await self._send_post(post, stream_id, cfg)
            if ok:
                sent_ids.append(post["id"])

        if not sent_ids:
            await self.ctx.send.text("图片下载失败，请稍后重试。", stream_id)
            return {"success": False, "message": "图片下载失败", "tags": tags}

        return {
            "success": True,
            "sent_count": len(sent_ids),
            "post_ids": sent_ids,
            "tags": tags,
        }

    # ── @Command —— /danbooru <tags> ─────────────────────────────────────────

    @Command(
        "danbooru",
        description="从 Danbooru 按 Tag 搜索图片",
        pattern=r"^/danbooru(?:\s+(?P<tags>.+))?$",
        aliases=["/db", "/D站"],
    )
    async def handle_command_search(self, **kwargs: Any):
        stream_id: str = kwargs["stream_id"]
        matched: dict = kwargs.get("matched_groups", {})
        tags: str = (matched.get("tags") or "").strip()

        if not tags:
            await self.ctx.send.text(
                "用法：/danbooru <tags>\n"
                "例如：/danbooru hatsune_miku solo\n"
                "多个 Tag 用空格分隔。",
                stream_id,
            )
            return False, "缺少 tags 参数", 1

        cfg = self.config.danbooru
        posts = await self._fetch_posts(tags, cfg)
        if not posts:
            await self.ctx.send.text(
                f"没有找到符合条件的图片（tags: {tags}）", stream_id
            )
            return False, "未找到结果", 1

        send_n = min(cfg.send_count, len(posts))
        selected = random.sample(posts, send_n)
        sent = 0

        for post in selected:
            ok = await self._send_post(post, stream_id, cfg)
            if ok:
                sent += 1

        if sent == 0:
            await self.ctx.send.text("图片下载失败，请稍后重试。", stream_id)
            return False, "图片下载失败", 1

        return True, f"已发送 {sent} 张图片（tags: {tags}）", 2

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    async def _fetch_posts(
        self, tags: str, cfg: DanbooruSection
    ) -> list[dict]:
        """从 Danbooru API 获取帖子列表并按评级过滤。"""
        params: dict[str, Any] = {
            "tags": tags,
            "limit": min(max(cfg.max_results, 1), 200),
        }
        if cfg.login and cfg.api_key:
            params["login"] = cfg.login
            params["api_key"] = cfg.api_key

        allowed = {r.strip().lower() for r in cfg.allowed_ratings.split(",") if r.strip()}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    _POSTS_ENDPOINT,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                    headers={"User-Agent": "MaiBot-DanbooruSearch/1.0"},
                ) as resp:
                    if resp.status != 200:
                        self.ctx.logger.warning(
                            "Danbooru API 返回非 200 状态: %d", resp.status
                        )
                        return []
                    data: list[dict] = await resp.json()
        except Exception as exc:
            self.ctx.logger.error("Danbooru API 请求失败: %s", exc, exc_info=True)
            return []

        # 评级过滤 + 确保有可用图片 URL
        filtered = [
            p for p in data
            if isinstance(p, dict)
            and p.get("rating", "").lower() in allowed
            and (p.get("file_url") or p.get("large_file_url") or p.get("preview_file_url"))
        ]
        return filtered

    async def _send_post(
        self, post: dict, stream_id: str, cfg: DanbooruSection
    ) -> bool:
        """下载单张图片并通过 ctx.send.image 发送；返回是否成功。"""
        url = self._pick_image_url(post, cfg.use_preview)
        if not url:
            self.ctx.logger.warning("帖子 %s 无可用图片 URL", post.get("id"))
            return False

        image_b64 = await self._download_image_b64(url, cfg.download_timeout)
        if not image_b64:
            return False

        try:
            ok = await self.ctx.send.image(
                image_base64=image_b64, stream_id=stream_id
            )
            return bool(ok)
        except Exception as exc:
            self.ctx.logger.error("发送图片失败 (post %s): %s", post.get("id"), exc)
            return False

    @staticmethod
    def _pick_image_url(post: dict, use_preview: bool) -> str | None:
        """根据配置选取合适的图片 URL。"""
        if use_preview:
            return post.get("preview_file_url") or post.get("large_file_url") or post.get("file_url")
        # 对超大原图（>10 MB）降级使用 large_file_url
        file_size: int = post.get("file_size", 0) or 0
        if file_size > 10 * 1024 * 1024:
            return post.get("large_file_url") or post.get("file_url")
        return post.get("file_url") or post.get("large_file_url") or post.get("preview_file_url")

    async def _download_image_b64(self, url: str, timeout: int) -> str | None:
        """下载图片并返回 base64 字符串；失败返回 None。"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    headers={"User-Agent": "MaiBot-DanbooruSearch/1.0"},
                ) as resp:
                    if resp.status != 200:
                        self.ctx.logger.warning(
                            "图片下载失败 (url=%s, status=%d)", url, resp.status
                        )
                        return None
                    data = await resp.read()
            return base64.b64encode(data).decode("utf-8")
        except asyncio.TimeoutError:
            self.ctx.logger.warning("图片下载超时: %s", url)
            return None
        except Exception as exc:
            self.ctx.logger.error("图片下载出错 (url=%s): %s", url, exc, exc_info=True)
            return None


# ─── 插件入口 ──────────