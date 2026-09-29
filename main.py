"""AstrBot 插件：《群史》编纂委员会

用维基百科的严肃文体，自动把群里的名场面编成正史。
- 每日凌晨自动侦察前一天的群聊，评审值得入史的事件并撰写词条
- 群友可用 /快记 申报重大事件，/群史 随机抽阅或检索
- 词条带修订史与真实引用，支持人物档案与《群史》出版导出
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, File, Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from .group_history import fetcher as F
from .group_history import prompts
from .group_history.exporter import export_book
from .group_history.pipeline import EditorialPipeline, PipelineError
from .group_history.storage import HistoryDB

PLUGIN_NAME = "astrbot_plugin_group_history"
PLUGIN_VERSION = "1.0.0"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class GroupHistoryCommittee(Star):
    """《群史》编纂委员会。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.db: HistoryDB | None = None
        self.pipeline: EditorialPipeline | None = None
        self.data_dir: Path | None = None
        self.main_db_path: Path | None = None
        self._tasks: list[asyncio.Task] = []

    # ================= 生命周期 =================

    async def initialize(self):
        self.data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        self.db = HistoryDB(self.data_dir / "history.db")
        self.pipeline = EditorialPipeline(self._llm_call, self._cfg)
        # AstrBot 主数据库定位：data/plugin_data/<plugin>/ -> data/
        candidate = self.data_dir.parent.parent / "data_v4.db"
        self.main_db_path = candidate if candidate.exists() else None
        if not self.db.get_meta("install_date"):
            self.db.set_meta("install_date", datetime.now().strftime("%Y-%m-%d"))
        self._tasks.append(asyncio.create_task(self._startup()))
        self._tasks.append(asyncio.create_task(self._trigger_watcher()))
        logger.info(f"[群史 v{PLUGIN_VERSION}] 编纂委员会已挂牌开工")

    async def terminate(self):
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        if self.db:
            self.db.close()

    def _cfg(self, key: str, default=None):
        try:
            v = self.config.get(key, default)
            return default if v is None else v
        except Exception:
            return default

    # ================= LLM 调用 =================

    def resolve_provider(self):
        """按配置取 Provider；未配置或不可用时回退默认 Provider。"""
        pid = str(self._cfg("provider_id", "") or "").strip()
        if pid:
            try:
                p = self.context.get_provider_by_id(pid)
            except Exception:
                p = None
            if p is not None and self._is_chat_provider(p):
                return p, pid
            logger.warning(f"[群史] 配置的 Provider「{pid}」不可用，回退到默认 Provider")
        return self.context.get_using_provider(), pid or "（默认）"

    @staticmethod
    def _provider_meta(p):
        """取 Provider 元数据（meta() 是方法，兼容属性式实现）。"""
        m = getattr(p, "meta", None)
        if callable(m):
            try:
                return m()
            except Exception:
                return None
        return m

    def _is_chat_provider(self, p) -> bool:
        m = self._provider_meta(p)
        pt = getattr(m, "provider_type", None)
        val = getattr(pt, "value", None) or str(pt or "")
        return "chat_completion" in str(val)

    def _model_name(self) -> str:
        provider, pid = self.resolve_provider()
        try:
            return provider.get_model() or str(pid)
        except Exception:
            return str(pid)

    def list_chat_providers(self) -> list[tuple[str, str]]:
        out = []
        try:
            providers = self.context.get_all_providers()
        except Exception:
            return out
        for p in providers or []:
            if not self._is_chat_provider(p):
                continue
            m = self._provider_meta(p)
            pid = str(getattr(m, "id", "?"))
            try:
                model = p.get_model() or ""
            except Exception:
                model = ""
            out.append((pid, model))
        return out

    async def _llm_call(self, system: str, user: str) -> str:
        provider, _ = self.resolve_provider()
        if provider is None:
            raise PipelineError("没有可用的 LLM Provider，请在面板配置后在 /群史模型 中检查")
        resp = await provider.text_chat(
            prompt=user,
            session_id=f"{PLUGIN_NAME}",
            contexts=[],
            system_prompt=system,
        )
        text = (getattr(resp, "completion_text", "") or "").strip()
        if not text:
            chain = getattr(resp, "result_chain", None)
            if chain:
                text = "".join(
                    getattr(seg, "text", "") for seg in getattr(chain, "chain", [])
                ).strip()
        if not text:
            raise PipelineError("LLM 返回了空内容")
        return text

    # ================= 编纂主流程 =================

    async def _startup(self):
        await asyncio.sleep(15)  # 等平台与 Provider 就绪
        try:
            await self._catch_up()
        except Exception as e:
            logger.error(f"[群史] 启动补编失败：{e}")
        self._tasks.append(asyncio.create_task(self._scheduler_loop()))

    async def _catch_up(self):
        last = self.db.get_meta("last_run_date")
        today = datetime.now()
        today_str = today.strftime("%Y-%m-%d")
        if not last:
            days = int(self._cfg("backfill_days", 14) or 0)
            if days > 0:
                logger.info(f"[群史] 首次启动，静默回溯编纂近 {days} 天……")
                await self._seed_groups_from_napcat()
                for i in range(days, 0, -1):
                    try:
                        await self._compile_day(days_ago=i, announce=False, reason="backfill")
                    except Exception as e:
                        logger.error(f"[群史] 回溯第 {i} 天前失败：{e}")
        else:
            try:
                last_date = datetime.strptime(last, "%Y-%m-%d")
            except Exception:
                last_date = today - timedelta(days=1)
            missed: list[int] = []
            d = last_date + timedelta(days=1)
            today_start = today.replace(hour=0, minute=0, second=0, microsecond=0)
            while d < today_start and len(missed) < 7:
                missed.append((today.date() - d.date()).days)
                d += timedelta(days=1)
            for i in sorted(missed, reverse=True):
                logger.info(f"[群史] 补编 {i} 天前的漏编日")
                await self._compile_day(days_ago=i, announce=False, reason="catchup")
        self.db.set_meta("last_run_date", today_str)

    async def _seed_groups_from_napcat(self):
        """从 napcat 拉群列表，记住群名与平台 ID，方便回溯与出版。"""
        for inst_pid in self._known_platform_ids():
            bot = self._onebot_bot(inst_pid)
            if bot is None:
                continue
            groups = await F.onebot_group_list(bot)
            for g in groups:
                self.db.set_meta(f"group_name_{g['group_id']}", g["group_name"])
            logger.info(f"[群史] 平台 {inst_pid} 可见群 {len(groups)} 个")
            if groups and self.db.get_meta("primary_platform") is None:
                self.db.set_meta("primary_platform", inst_pid)

    def _known_platform_ids(self) -> list[str]:
        pids = set()
        if self.db is not None:
            for row in self.db._conn.execute(
                "SELECT key FROM meta WHERE key LIKE 'platform_of_%'"
            ).fetchall():
                umo = row["key"].replace("platform_of_", "", 1)
                pids.add(umo.split(":")[0])
        primary = self.db.get_meta("primary_platform") if self.db else None
        if primary:
            pids.add(primary)
        return [p for p in pids if p] or ["aiocqhttp"]

    async def _scheduler_loop(self):
        while True:
            try:
                run_hour = int(self._cfg("run_hour", 2) or 2)
                interval = max(1, int(self._cfg("run_interval_days", 1) or 1))
                now = datetime.now()
                target = now.replace(
                    hour=run_hour, minute=random_minute(), second=0, microsecond=0
                )
                if target <= now:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                # 编纂周期：每 N 天跑一次
                if interval > 1 and datetime.now().toordinal() % interval != 0:
                    logger.info(f"[群史] 今日不在编纂周期内（每 {interval} 天一次），歇笔一天")
                    continue
                await self._compile_day(
                    days_ago=1, announce=bool(self._cfg("announce_enabled", True)), reason="daily"
                )
                self.db.set_meta("last_run_date", datetime.now().strftime("%Y-%m-%d"))
                self.db.set_meta("last_run_ok", _now())
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[群史] 每日编纂异常：{e}")
                await asyncio.sleep(600)

    def _day_group_scope(self, umo_filter: str | None, start_ts: int, end_ts: int) -> list[str]:
        """确定某日要编纂的群范围（unified_msg_origin 列表）。"""
        if umo_filter:
            return [umo_filter]
        allowed = [str(g) for g in (self._cfg("allowed_groups", []) or [])]
        umos = []
        for umo, gid, _n in self.db.raw_group_ids_between(start_ts, end_ts):
            if allowed and gid not in allowed:
                continue
            if F.is_group_umo(umo):
                umos.append(umo)
        return umos

    async def _compile_day(
        self,
        days_ago: int,
        announce: bool,
        reason: str,
        umo_filter: str | None = None,
        focus_hint: str = "",
    ) -> list[dict]:
        """对某一天执行完整流水线。返回产物摘要列表（供试跑/远程结果）。"""
        start, end = F.day_bounds(days_ago=days_ago)
        date_str = start.strftime("%Y-%m-%d")
        results = []
        for umo in self._day_group_scope(umo_filter, int(start.timestamp()), int(end.timestamp())):
            if not F.is_group_umo(umo):
                continue
            group_id = F.parse_umo(umo)[2]
            rows = await self._ensure_day_rows(umo, group_id, start, end)
            if len(rows) < 15:
                continue  # 太冷清，不入史
            entry_results = await self._compile_one(umo, group_id, date_str, rows, focus_hint)
            # 官宣频控：每次编纂最多官宣 max_announces_per_day 条（0 = 只入库不官宣），
            # 超出的按史评等级静默入库
            announce_cap = int(self._cfg("max_announces_per_day", 2) or 0)
            rankable = sorted(
                entry_results, key=lambda x: -(x.get("significance") or 0)
            )
            kept_ids = {id(er) for er in rankable[:announce_cap]}
            for er in entry_results:
                if id(er) not in kept_ids:
                    er["announce_text"] = ""
            results.append(
                {
                    "group_id": group_id,
                    "date": date_str,
                    "message_count": len(rows),
                    "entries": entry_results,
                }
            )
            if announce and announce_cap > 0:
                for er in entry_results:
                    if er.get("announce_text"):
                        await self._send(umo, er["announce_text"])
            if (
                not entry_results
                and announce
                and bool(self._cfg("announce_empty_day", False))
                and reason == "daily"
                and not focus_hint
            ):
                await self._send(umo, prompts.NO_EVENT_ANNOUNCE)
        return results

    async def _compile_one(
        self, umo: str, group_id: str, date_str: str, rows: list[dict], focus_hint: str = ""
    ) -> list[dict]:
        lines = F.to_lines(rows)
        events = await self.pipeline.scout_day(date_str, lines, focus_hint=focus_hint)
        if not events:
            return []
        picked = events if focus_hint else self.pipeline.select_events(events)
        if not picked:
            return []
        existing = self.db.list_entries(umo=umo, limit=1000)
        existing_titles = [e["title"] for e in existing]
        model = self._model_name()
        out = []
        for ev in picked:
            try:
                old = self.db.find_candidate(umo, ev["title"], [])
                if old:
                    out.append(
                        await self._do_revision(old, ev, date_str, model, extra_reason="")
                    )
                else:
                    written = await self.pipeline.write_new_entry(date_str, ev, existing_titles)
                    dup = self.db.find_candidate(umo, written["title"], written["aliases"])
                    if dup:
                        # 撰写出的标题撞上了既有词条 → 并为修订
                        out.append(
                            await self._do_revision(
                                dup, ev, date_str, model,
                                extra_reason="（新撰标题与既有词条相合，并入之）",
                            )
                        )
                        continue
                    entry = self.db.add_entry(
                        umo=umo,
                        group_id=group_id,
                        title=written["title"],
                        aliases=written["aliases"],
                        categories=written["categories"],
                        content=written["content"],
                        significance=written["significance"],
                        first_date=date_str,
                        evidence=written["evidence_used"],
                        people=[{"name": p, "sender_id": ""} for p in ev.get("participants", [])],
                    )
                    existing_titles.append(entry["title"])
                    announce_text = prompts.ANNOUNCE_NEW_TMPL.format(
                        header=prompts.ANNOUNCE_HEADER,
                        title=entry["title"],
                        serial=entry["serial"],
                        content=entry["content"],
                        draft_note=self._draft_note(),
                    )
                    out.append(
                        {
                            "mode": "new",
                            "serial": entry["serial"],
                            "title": entry["title"],
                            "significance": entry["significance"],
                            "announce_text": announce_text,
                            "content": entry["content"],
                        }
                    )
            except PipelineError as e:
                logger.error(f"[群史] 词条「{ev.get('title')}」编纂失败：{e}")
            except Exception as e:
                logger.error(f"[群史] 词条「{ev.get('title')}」编纂异常：{e}")
        return out

    async def _do_revision(self, old: dict, ev: dict, date_str: str, model: str,
                           extra_reason: str = "") -> dict:
        written = await self.pipeline.write_revision(
            old["serial"], old["title"], old["content"], date_str, ev
        )
        updated = self.db.update_revision(
            old["id"],
            written["content"],
            reason=f"{date_str} 后续进展{extra_reason}",
            model=model,
            significance=written["significance"],
            categories=written["categories"],
            evidence=written["evidence_used"],
            people=[{"name": p, "sender_id": ""} for p in ev.get("participants", [])],
        )
        announce_text = prompts.ANNOUNCE_REVISION_TMPL.format(
            header=prompts.ANNOUNCE_HEADER,
            title=written["title"],
            serial=old["serial"],
            revision_count=updated["revision_count"],
            reason=f"{date_str} 事件有新进展，经编委会查证后并入原词条",
            draft_note=self._draft_note(),
        )
        return {
            "mode": "revision",
            "serial": old["serial"],
            "title": written["title"],
            "significance": updated["significance"],
            "announce_text": announce_text,
            "content": written["content"],
        }

    def _draft_note(self) -> str:
        try:
            install = datetime.strptime(self.db.get_meta("install_date") or "", "%Y-%m-%d")
        except Exception:
            return ""
        days = int(self._cfg("probation_days", 14) or 0)
        if days <= 0:
            return ""
        if (datetime.now() - install).days < days:
            return prompts.DRAFT_NOTE
        return ""

    async def _send(self, umo: str, text: str):
        try:
            await self.context.send_message(umo, MessageChain(chain=[Plain(text)]))
        except Exception as e:
            logger.error(f"[群史] 主动播报失败（{umo}）：{e}")

    # ================= 工具 =================

    @staticmethod
    def _plain_text(event: AstrMessageEvent) -> str:
        parts = []
        for seg in event.get_messages() or []:
            t = getattr(seg, "text", None)
            if isinstance(t, str):
                parts.append(t)
        return "".join(parts).strip()

    @staticmethod
    def _strip_command(text: str, names: tuple[str, ...]) -> str:
        for n in sorted(names, key=len, reverse=True):
            m = re.search(rf"^[/！!。.，,？?\s]*{re.escape(n)}\s*", text)
            if m:
                return text[m.end():].strip()
        return ""

    @staticmethod
    def _at_target(event: AstrMessageEvent) -> tuple[str, str]:
        """返回消息中 At 的 (qq, name)。"""
        for seg in event.get_messages() or []:
            if isinstance(seg, At):
                return str(getattr(seg, "qq", "") or ""), str(getattr(seg, "name", "") or "")
        return "", ""

    def _require_ready(self) -> bool:
        return self.db is not None and self.pipeline is not None

    # ================= 实时采录 =================

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """监听全部群消息写入本地卷宗（只记录，不响应、不打断）。"""
        if not self._require_ready():
            return
        try:
            umo = event.unified_msg_origin
            if not F.is_group_umo(umo):
                return
            self_id = str(event.get_self_id() or "")
            sender_id = str(event.get_sender_id() or "")
            if sender_id and sender_id == self_id:
                return
            text = self._event_to_text(event)
            if not text:
                return
            ts = int(time.time())
            self.db.insert_raw(umo, F.parse_umo(umo)[2], sender_id,
                               str(event.get_sender_name() or sender_id), text, ts)
            # 记住平台 ID，供回溯时取 bot 客户端
            key = f"platform_of_{umo}"
            if self.db.get_meta(key) is None:
                self.db.set_meta(key, str(event.get_platform_id() or umo.split(":")[0]))
        except Exception as e:
            logger.error(f"[群史] 采录消息失败：{e}")

    @staticmethod
    def _event_to_text(event: AstrMessageEvent) -> str:
        """把事件消息链压成一行文本（与 napcat 侧格式尽量一致）。"""
        parts = []
        for seg in event.get_messages() or []:
            cname = type(seg).__name__
            if cname == "Plain":
                t = str(getattr(seg, "text", "")).strip()
                if t:
                    parts.append(t)
            elif cname == "At":
                parts.append(f"@{getattr(seg, 'name', '') or getattr(seg, 'qq', '')}")
            elif cname == "AtAll":
                parts.append("@全体成员")
            elif cname == "Image":
                parts.append("[图]")
            elif cname == "Face":
                parts.append("[表情]")
            elif cname == "Record":
                parts.append("[语音]")
            elif cname == "Video":
                parts.append("[视频]")
            elif cname == "Reply":
                continue
            elif cname in ("Forward", "Nodes", "Node"):
                parts.append("[合并转发]")
            elif cname == "File":
                parts.append("[文件]")
        return " ".join(parts).strip()[:500]

    # ================= napcat 客户端 =================

    def _onebot_bot(self, platform_id: str | None = None):
        """取 OneBot 客户端：先按指定平台 ID 找，找不到则枚举全部平台适配器兜底。"""
        candidates: list[str] = []
        pid = (platform_id or "").strip()
        if pid:
            candidates.append(pid)
        try:
            for inst in self.context.platform_manager.platform_insts or []:
                try:
                    candidates.append(str(inst.meta().id))
                except Exception:
                    continue
        except Exception:
            pass
        seen = set()
        for cand in candidates:
            if not cand or cand in seen:
                continue
            seen.add(cand)
            try:
                inst = self.context.get_platform_inst(cand)
            except Exception:
                inst = None
            if inst is None:
                continue
            bot = getattr(inst, "bot", None)
            if bot is not None and hasattr(bot, "call_action"):
                return bot
        return None

    async def _ensure_day_rows(self, umo: str, group_id: str, start: datetime,
                               end: datetime) -> list[dict]:
        """取某群某日的卷宗行；本地不足则先经 napcat 回填。"""
        start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
        rows = self.db.raw_day_rows(group_id, start_ts, end_ts)
        if len(rows) >= 15:
            return rows
        bot = self._onebot_bot(self.db.get_meta(f"platform_of_{umo}") or umo.split(":")[0])
        if bot is None:
            return rows
        try:
            backfilled = await F.onebot_fetch_day(bot, group_id, start_ts, end_ts)
        except Exception as e:
            logger.error(f"[群史] napcat 回填群 {group_id} 失败：{e}")
            return rows
        if backfilled:
            self.db.insert_raw_many(umo, group_id, backfilled, start_ts)
            rows = self.db.raw_day_rows(group_id, start_ts, end_ts)
            logger.info(f"[群史] 经 napcat 回填群 {group_id} {start:%m-%d} 实录 {len(backfilled)} 条")
        return rows

    # ================= 指令：/群史 =================

    @filter.command("群史", alias={"查群史", "群史查询"})
    async def cmd_history(self, event: AstrMessageEvent):
        """随机抽阅 / 检索 / 查编号 / 查人物档案"""
        if not self._require_ready():
            yield event.plain_result("编纂委员会尚在挂牌中，请稍候再试。")
            return
        umo = event.unified_msg_origin
        arg = self._strip_command(self._plain_text(event), ("群史", "查群史", "群史查询"))
        qq, at_name = self._at_target(event)
        try:
            if qq or at_name:
                async for r in self._person_dossier(event, umo, qq, at_name):
                    yield r
                return
            if not arg:
                entry = self.db.random_entry(umo)
                if not entry:
                    yield event.plain_result(
                        "编纂委员会查遍书架，本群尚无任何词条。\n"
                        "史册空白，诸君共勉——多搞点事情，或用「/快记 事件描述」申报入史。"
                    )
                    return
                yield event.plain_result(self._render_entry(entry))
                return
            if arg.isdigit():
                entry = self.db.get_entry(arg, umo=umo)
                if entry and entry["umo"] == umo:
                    yield event.plain_result(self._render_entry(entry))
                else:
                    yield event.plain_result(f"查无「群史字第 {arg} 号」此号。")
                return
            hits = self.db.search(arg, umo=umo, limit=8)
            if not hits:
                yield event.plain_result(
                    f"遍查群史，未见与「{arg}」相关之记载。\n若此事确曾发生，可「/快记 {arg}」补录。"
                )
            elif len(hits) == 1:
                yield event.plain_result(self._render_entry(hits[0]))
            else:
                lines = [f"检索「{arg}」得 {len(hits)} 条：", ""]
                for h in hits:
                    lines.append(f"· 第 {h['serial']:04d} 号【{h['title']}】（{h['first_date']}）")
                lines.append("")
                lines.append("可「/群史 编号」调阅全文。")
                yield event.plain_result("\n".join(lines))
        except Exception as e:
            logger.error(f"[群史] /群史 异常：{e}")
            yield event.plain_result("档案馆临时闭馆（内部错误），请稍后再来。")

    def _render_entry(self, entry: dict) -> str:
        head = f"【{entry['title']}】（群史字第 {entry['serial']:04d} 号）"
        meta = (
            f"事发 {entry['first_date']} · 载入 {entry['created_at'][:10]}"
            f" · 第 {entry['revision_count']} 版"
        )
        cats = ""
        if entry["categories"]:
            cats = "分类：" + " / ".join(entry["categories"])
        tail = "—— 群史编纂委员会"
        if entry["revision_count"] > 1:
            tail += f"（/群史修订 {entry['serial']} 查修订史）"
        return "\n".join(x for x in [head, "", entry["content"], "", meta, cats, tail] if x)

    # ================= 人物档案 =================

    async def _person_dossier(
        self, event: AstrMessageEvent, umo: str, qq: str, at_name: str
    ):
        name, sender_id = at_name, qq
        info = None
        try:
            info = self.db.raw_lookup_sender(sender_id=qq, sender_name=at_name)
        except Exception:
            info = None
        if info is None and self.main_db_path:
            try:
                info = F.lookup_sender(self.main_db_path, sender_id=qq, sender_name=at_name)
            except Exception:
                info = None
        if info:
            sender_id = sender_id or info["sender_id"]
            name = at_name or info["sender_name"]
        if not name:
            yield event.plain_result("档案处需要知道此人是谁。")
            return
        cache_key = f"{sender_id}:{name}"
        cached = self.db.get_entity_cache(cache_key)
        if cached:
            yield event.plain_result(f"📖 群史档案（调阅）\n\n{cached}\n\n—— 档案处")
            return
        entries = self.db.entries_by_person(umo, name, sender_id)
        entries_brief = "、".join(f"【{e['title']}】" for e in entries[:10]) if entries else ""
        evids = self.db.person_evidence(name, sender_id)
        evidence_brief = (
            "；".join(f"{e['sender']}：{e['quote'][:50]}" for e in evids[:12]) if evids else ""
        )
        samples = self.db.raw_sender_samples(sender_id=sender_id, sender_name=name if not sender_id else "", limit=300)
        if not samples and self.main_db_path:
            try:
                samples = F.fetch_sender_samples(
                    self.main_db_path,
                    sender_id=sender_id,
                    sender_name=name if not sender_id else "",
                    limit=300,
                )
            except Exception:
                samples = []
        try:
            page = await self.pipeline.write_person_page(
                name, entries_brief, evidence_brief, samples
            )
        except PipelineError as e:
            yield event.plain_result(f"档案处修撰失败：{e}")
            return
        self.db.save_entity_cache(cache_key, sender_id, page)
        yield event.plain_result(f"📖 群史档案（新修）\n\n{page}\n\n—— 档案处")

    # ================= 指令：/群史列表 /分类 /修订 =================

    @filter.command("群史列表")
    async def cmd_list(self, event: AstrMessageEvent):
        if not self._require_ready():
            return
        entries = self.db.list_entries(umo=event.unified_msg_origin, limit=30)
        if not entries:
            yield event.plain_result("本群史册尚为空白。")
            return
        total = self.db.count_entries(umo=event.unified_msg_origin)
        lines = [f"《群史》总目（共 {total} 条，列最近 {len(entries)} 条）：", ""]
        for e in entries:
            lines.append(f"· 第 {e['serial']:04d} 号【{e['title']}】{e['first_date']}")
        yield event.plain_result("\n".join(lines))

    @filter.command("群史分类")
    async def cmd_categories(self, event: AstrMessageEvent):
        if not self._require_ready():
            return
        umo = event.unified_msg_origin
        arg = self._strip_command(self._plain_text(event), ("群史分类",))
        cats = self.db.list_categories(umo)
        if not cats:
            yield event.plain_result("尚无分类。史册未满，分类暂缺。")
            return
        if not arg:
            lines = ["《群史》分类表：", ""]
            for c, n in cats.items():
                lines.append(f"· {c}（{n} 条）")
            lines.append("")
            lines.append("「/群史分类 分类名」可查看该分类下的词条。")
            yield event.plain_result("\n".join(lines))
            return
        target = next((c for c in cats if c.lower() == arg.lower()), arg)
        entries = self.db.entries_by_category(umo, target)
        if not entries:
            yield event.plain_result(f"分类「{target}」下暂无词条。")
            return
        lines = [f"分类「{target}」共 {len(entries)} 条：", ""]
        for e in entries[:20]:
            lines.append(f"· 第 {e['serial']:04d} 号【{e['title']}】")
        yield event.plain_result("\n".join(lines))

    @filter.command("群史修订", alias={"修订史"})
    async def cmd_revisions(self, event: AstrMessageEvent):
        if not self._require_ready():
            return
        arg = self._strip_command(self._plain_text(event), ("群史修订", "修订史"))
        if not arg:
            yield event.plain_result("用法：/群史修订 编号（如 /群史修订 13）")
            return
        entry = self.db.get_entry(arg, umo=event.unified_msg_origin)
        if not entry:
            yield event.plain_result(f"查无此词条：{arg}")
            return
        revs = self.db.get_revisions(entry["id"])
        lines = [f"【{entry['title']}】修订史（现行为第 {entry['revision_count']} 版）：", ""]
        if len(revs) <= 1:
            lines.append("此词条尚未修订过，初版即定本。")
        for i, r in enumerate(revs, 1):
            reason = r["reason"] or "（未注记）"
            lines.append(f"第 {i} 版 · {r['created_at'][:10]} · {reason}")
        lines.append("")
        lines.append("调阅现行版：/群史 " + str(entry["serial"]))
        yield event.plain_result("\n".join(lines))

    # ================= 指令：/快记 =================

    @filter.command("快记", alias={"入史"})
    async def cmd_fast_track(self, event: AstrMessageEvent):
        """群友现场申报重大事件，委员会立即查记录立卷"""
        if not self._require_ready():
            yield event.plain_result("编纂委员会尚在挂牌中。")
            return
        if not bool(self._cfg("fast_track_enabled", True)):
            yield event.plain_result("编委会已暂停受理群友申报。")
            return
        umo = event.unified_msg_origin
        desc = self._strip_command(self._plain_text(event), ("快记", "入史"))
        if not desc:
            yield event.plain_result(
                "用法：/快记 刚才发生的大事（如：/快记 凯子又说龙虾不是海鲜）"
            )
            return
        sender_id = event.get_sender_id()
        cooldown = int(self._cfg("fast_track_cooldown_minutes", 10) or 0)
        cd_key = f"ft_cd_{umo}_{sender_id}"
        if cooldown > 0:
            last = self.db.get_meta(cd_key)
            if last:
                try:
                    elapsed = time.time() - float(last)
                    if elapsed < cooldown * 60:
                        remain = int((cooldown * 60 - elapsed) / 60) + 1
                        yield event.plain_result(
                            f"编委会正在处理上一份申报。为免史官过劳，请 {remain} 分钟后再来。"
                        )
                        return
                except Exception:
                    pass
            self.db.set_meta(cd_key, str(time.time()))
        yield event.plain_result("编委会已受理申报，正在调阅近几小时实录……")
        hours = int(self._cfg("fast_track_hours_lookback", 8) or 8)
        start = datetime.now() - timedelta(hours=hours)
        try:
            grouped = F.fetch_messages_between(self.main_db_path, start, datetime.now())
        except Exception as e:
            yield event.plain_result(f"调阅实录失败：{e}")
            return
        rows = grouped.get(umo) or []
        if len(rows) < 10:
            yield event.plain_result("近几小时实录过于冷清，编委会无法从卷宗中找到对应事件。")
            return
        date_str = datetime.now().strftime("%Y-%m-%d")
        try:
            results = await self._compile_one(
                umo, F.parse_umo(umo)[2], date_str, rows, focus_hint=desc
            )
        except Exception as e:
            logger.error(f"[群史] 快记异常：{e}")
            results = []
        if not results:
            yield event.plain_result(
                "编委会审阅卷宗后，认为该事件暂不足以入史（或与既有词条重复且无新进展）。\n史笔如铁，恕难从命。"
            )
            return
        for r in results:
            yield event.plain_result(r["announce_text"])

    # ================= 指令：/删史（管理） =================

    @filter.command("删史")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_delete(self, event: AstrMessageEvent):
        arg = self._strip_command(self._plain_text(event), ("删史",))
        m = re.match(r"^(\d+)(\s+(确认|confirm))?$", arg)
        if not m:
            yield event.plain_result("用法：/删史 编号 确认（需两步确认）")
            return
        serial = int(m.group(1))
        entry = self.db.get_entry(str(serial), umo=event.unified_msg_origin)
        if not entry:
            yield event.plain_result(f"查无「第 {serial} 号」词条。")
            return
        if not m.group(3):
            yield event.plain_result(
                f"即将抹除「第 {serial} 号【{entry['title']}】」。\n"
                f"历史不可轻动，请再发「/删史 {serial} 确认」执行。"
            )
            return
        self.db.soft_delete(serial)
        self.db.set_meta(f"deleted_{serial}", f"{entry['title']} @ {_now()}")
        yield event.plain_result(
            f"「第 {serial} 号【{entry['title']}】」已从群史中抹去。\n—— 但编委会的记性没那么差。"
        )

    # ================= 指令：/群史模型 =================

    @filter.command("群史模型")
    async def cmd_model(self, event: AstrMessageEvent):
        arg = self._strip_command(self._plain_text(event), ("群史模型",))
        providers = self.list_chat_providers()
        if not arg:
            current, _ = self.resolve_provider()
            current_id = str(getattr(self._provider_meta(current), "id", "（默认）"))
            lines = ["可用模型（Provider）：", ""]
            for pid, model in providers:
                mark = " ← 当前" if pid == current_id else ""
                lines.append(f"· {pid}" + (f"（{model}）" if model else "") + mark)
            lines.append("")
            lines.append("设置：/群史模型 provider_id（需管理员）")
            lines.append("也可在插件配置页的下拉框中选择。")
            yield event.plain_result("\n".join(lines))
            return
        pid = arg.strip()
        valid = {p for p, _ in providers}
        if pid not in valid:
            yield event.plain_result(f"不存在 Provider「{pid}」。发 /群史模型 查看列表。")
            return
        self.config["provider_id"] = pid
        try:
            self.config.save_config()
        except Exception as e:
            logger.warning(f"[群史] 配置保存失败：{e}")
        yield event.plain_result(f"执笔用笔已换为「{pid}」，即刻生效。")

    # ================= 指令：/群史自检 =================

    @filter.command("群史自检")
    async def cmd_selfcheck(self, event: AstrMessageEvent):
        report = self.build_selfcheck()
        llm_state = "✓"
        try:
            reply = await self._llm_call("你是一个回声测试器。只回复两个字：正常", "ping")
            llm_state = f"✓（回复：{reply[:12]}）"
        except Exception as e:
            llm_state = f"✗（{e}）"
        lines = [
            "📜 群史编纂委员会 · 自检报告",
            "",
            f"· 插件版本：v{PLUGIN_VERSION}",
            f"· 主数据库：{'✓ 已接入' if self.main_db_path else '✗ 未找到'}",
            f"· 群史库：✓ 词条 {report['stats']['entries']} 条 / 修订 "
            f"{report['stats']['revisions']} 次 / 引文 {report['stats']['evidence']} 条",
            f"· 执笔 Provider：{report['provider']} {llm_state}",
            f"· 每日编纂：{report['run_hour']} 点 | 上次成功：{report['last_run_ok'] or '尚未运行'}",
            f"· 试运行期：{'进行中（' + report['probation_left'] + '）' if report['probation_left'] else '已结束'}",
        ]
        yield event.plain_result("\n".join(lines))

    def build_selfcheck(self) -> dict:
        provider, pid = self.resolve_provider()
        install = self.db.get_meta("install_date") or ""
        try:
            install_dt = datetime.strptime(install, "%Y-%m-%d")
            left_days = int(self._cfg("probation_days", 14) or 0) - (datetime.now() - install_dt).days
            probation = f"剩 {left_days} 天" if left_days > 0 else ""
        except Exception:
            probation = ""
        return {
            "provider": str(getattr(self._provider_meta(provider), "id", pid) or pid)
            if provider is not None
            else "（无可用 Provider）",
            "chat_providers": [p for p, _ in self.list_chat_providers()],
            "stats": self.db.stats(),
            "run_hour": self._cfg("run_hour", 2),
            "last_run_ok": self.db.get_meta("last_run_ok") or "",
            "last_run_date": self.db.get_meta("last_run_date") or "",
            "probation_left": probation,
            "main_db": bool(self.main_db_path),
        }

    # ================= 指令：/群史试跑（管理） =================

    @filter.command("群史试跑")
    async def cmd_test_run(self, event: AstrMessageEvent):
        arg = self._strip_command(self._plain_text(event), ("群史试跑",))
        days_ago = 1
        if arg.isdigit():
            days_ago = max(1, min(30, int(arg)))
        yield event.plain_result(f"编委会开始试编 {days_ago} 天前的卷宗，完成后当场宣读……")
        try:
            results = await self._compile_day(
                days_ago=days_ago,
                announce=False,
                reason="test",
                umo_filter=event.unified_msg_origin,
            )
        except Exception as e:
            logger.error(f"[群史] 试跑失败：{e}")
            yield event.plain_result(f"试编失败：{e}")
            return
        entries = [er for r in results for er in r.get("entries", [])]
        if not entries:
            msg = f"{days_ago} 天前的卷宗审阅完毕：无新事件入史，亦无既有词条需要修订。"
            if not results:
                msg += "（该日实录冷清或无群消息）"
            yield event.plain_result("📜 " + msg)
            return
        for er in entries:
            yield event.plain_result(er["announce_text"])

    # ================= 指令：/群史出版 =================

    @filter.command("群史出版", alias={"出版群史"})
    async def cmd_publish(self, event: AstrMessageEvent):
        if not self._require_ready():
            return
        umo = event.unified_msg_origin
        entries = self.db.list_entries(umo=umo, limit=100000)
        entries.reverse()  # 按编号升序装订
        if not entries:
            yield event.plain_result("史册空白，无书可出。")
            return
        group_id = F.parse_umo(umo)[2]
        group_name = self.db.get_meta(f"group_name_{group_id}") or f"群 {group_id}"
        yield event.plain_result(f"编委会正在装订《群史·第一卷》（{len(entries)} 条）……")
        try:
            html_path, pdf_path = export_book(entries, self.data_dir / "exports", group_name)
        except Exception as e:
            logger.error(f"[群史] 出版失败：{e}")
            yield event.plain_result(f"装订失败：{e}")
            return
        try:
            await self.context.send_message(
                umo, MessageChain(chain=[File(name=html_path.name, file=str(html_path))])
            )
            if pdf_path and pdf_path.exists():
                await self.context.send_message(
                    umo, MessageChain(chain=[File(name=pdf_path.name, file=str(pdf_path))])
                )
            else:
                await self._send(
                    umo,
                    "附注：本机未装 PDF 引擎，已交付 HTML 版。浏览器打开后「打印 → 另存为 PDF」即可成书。",
                )
            await self._send(
                umo, f"《群史·第一卷》装订完成，凡 {len(entries)} 条，已呈上。\n—— 群史编纂委员会"
            )
        except Exception as e:
            logger.error(f"[群史] 送书失败：{e}")
            yield event.plain_result(f"送书失败：{e}（文件在 {html_path}）")

    # ================= 指令：/群史帮助 =================

    @filter.command("群史帮助")
    async def cmd_help(self, event: AstrMessageEvent):
        yield event.plain_result(
            "📜 群史编纂委员会 · 使用说明\n"
            "\n"
            "本委员会以维基百科之严肃，记载本群之抽象。\n"
            "\n"
            "· /群史 —— 随机抽阅一条群史（按史评等级加权）\n"
            "· /群史 关键词 —— 检索（如 /群史 龙虾、/群史 13）\n"
            "· /群史 @某人 —— 调阅其人物档案\n"
            "· /群史列表 —— 总目\n"
            "· /群史分类 [分类] —— 分类表 / 分类词条\n"
            "· /群史修订 编号 —— 查词条修订史\n"
            "· /快记 事件描述 —— 申报重大事件入史\n"
            "· /群史出版 —— 装订《群史·第一卷》（HTML 成书）\n"
            "· /删史 编号 确认 —— （管理员）抹除某段历史\n"
            "· /群史模型 —— 查看执笔模型（管理员可切换）\n"
            "· /群史试跑 [天数] —— （管理员）试编某日卷宗\n"
            "· /群史自检 —— 委员会健康检查\n"
            "\n"
            "每日凌晨，委员会自动翻阅前一日实录；无事则歇笔，有事则官宣。"
        )

    # ================= 远程试跑通道（文件触发，供运维/调试） =================

    async def _trigger_watcher(self):
        """轮询 data_dir/trigger.json。动作：test_run / selfcheck / run_daily。
        结果写 result.json。便于在不进群的情况下验证流水线。"""
        trigger = self.data_dir / "trigger.json"
        while True:
            try:
                await asyncio.sleep(8)
                if not trigger.exists():
                    continue
                try:
                    req = json.loads(trigger.read_text(encoding="utf-8"))
                except Exception as e:
                    logger.error(f"[群史] trigger.json 解析失败：{e}")
                    trigger.unlink(missing_ok=True)
                    continue
                trigger.unlink(missing_ok=True)
                action = str(req.get("action", ""))
                logger.info(f"[群史] 收到远程触发：{action}")
                result = {"ok": False, "action": action, "at": _now()}
                try:
                    if action == "test_run":
                        results = await self._compile_day(
                            days_ago=int(req.get("days_ago", 1)),
                            announce=bool(req.get("announce", False)),
                            reason="remote_test",
                            umo_filter=req.get("umo") or None,
                            focus_hint=str(req.get("focus", "") or ""),
                        )
                        result.update(ok=True, results=results)
                    elif action == "probe_history":
                        bot = self._onebot_bot(req.get("platform_id") or self.db.get_meta("primary_platform"))
                        if bot is None:
                            result["error"] = "未找到可用的 OneBot 客户端"
                        else:
                            gid = str(req.get("group_id") or "")
                            probe = await F.onebot_probe_history(bot, gid)
                            groups = await F.onebot_group_list(bot)
                            result.update(ok=True, probe=probe, groups=groups)
                    elif action == "reset":
                        self.db.set_meta("last_run_date", "")
                        self.db.set_meta("install_date", datetime.now().strftime("%Y-%m-%d"))
                        result.update(ok=True, note="已重置回溯状态，重启后生效")
                    elif action == "selfcheck":
                        result.update(ok=True, report=self.build_selfcheck())
                    elif action == "run_daily":
                        await self._compile_day(
                            days_ago=1, announce=bool(req.get("announce", True)), reason="daily"
                        )
                        self.db.set_meta("last_run_date", datetime.now().strftime("%Y-%m-%d"))
                        result.update(ok=True)
                    else:
                        result["error"] = f"未知动作 {action}"
                except Exception as e:
                    logger.error(f"[群史] 远程触发执行失败：{e}")
                    result["error"] = str(e)
                (self.data_dir / "result.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[群史] trigger watcher 异常：{e}")


def random_minute() -> int:
    """编纂开工时刻的分钟数加一点随机，避免整点高峰。"""
    import random

    return random.randint(0, 30)
