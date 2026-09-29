"""《群史》编纂委员会 - 编辑流水线

侦察(Scout) → 评审合并(Merge) → 撰写(Writer) → 修订(Revision)
全部通过 AstrBot 的 Provider 完成 LLM 调用。
"""

from __future__ import annotations

import json
import re
from typing import Any, Awaitable, Callable

from . import prompts
from .fetcher import chunk_lines, subsample_lines

_LlmFn = Callable[[str, str], Awaitable[str]]  # (system, user) -> text


class PipelineError(Exception):
    pass


def extract_json(text: str) -> dict:
    """从 LLM 输出中稳健地抠出 JSON 对象。"""
    text = (text or "").strip()
    # 直接解析
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # 剥离 markdown 代码块
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if m:
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    # 首个大括号到最后一个 大括号
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        try:
            obj = json.loads(text[start : end + 1])
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    raise PipelineError("LLM 输出无法解析为 JSON")


def _as_list(v: Any) -> list:
    if isinstance(v, list):
        return v
    if v is None or v == "":
        return []
    return [str(v)]


def _clean_quote(q: Any) -> dict:
    if not isinstance(q, dict):
        return {}
    return {
        "time": str(q.get("time", "")).strip()[:24],
        "sender": str(q.get("sender", "")).strip()[:60],
        "quote": str(q.get("quote", "")).strip()[:300],
    }


class EditorialPipeline:
    """一次编纂任务 = 侦察 → 合并 → 逐条撰写/修订。LLM 调用函数由外部注入，便于测试。"""

    def __init__(self, llm_fn: _LlmFn, get_config):
        self._llm_fn = llm_fn
        self._get_config = get_config

    def _cfg(self, key: str, default):
        return self._get_config(key, default)

    async def _llm_json(self, system: str, user: str, max_retry: int = 1) -> dict:
        last_err: Exception | None = None
        prompt = user
        for _ in range(max_retry + 1):
            text = await self._llm_fn(system, prompt)
            try:
                return extract_json(text)
            except PipelineError as e:
                last_err = e
                prompt = (
                    user
                    + "\n\n【注意】你上一次的输出无法被解析为合法 JSON。"
                    "请只输出一个纯 JSON 对象，不要任何多余文字、注释或代码块标记。"
                )
        raise PipelineError(f"JSON 解析失败：{last_err}")

    # ---------- 阶段一：侦察 ----------

    async def scout_day(self, date_str: str, lines: list[str], focus_hint: str = "") -> list[dict]:
        if not lines:
            return []
        lines, sampled = subsample_lines(lines, int(self._cfg("max_context_chars", 120000)))
        chunks = chunk_lines(lines, int(self._cfg("chunk_chars", 45000)))
        note = "（本实录经过抽样，可能不完整，请基于可见内容判断）" if sampled else ""
        all_events: list[dict] = []
        for i, chunk in enumerate(chunks):
            focus = ""
            if focus_hint:
                focus = f"【编委会特别提示】本日收到群友申报线索：「{focus_hint}」，请优先围绕该线索梳理事件；其余显著事件也应照常报告。"
            user = prompts.SCOUT_USER_TMPL.format(
                date=date_str, lines="\n".join(chunk), focus_hint=focus + note
            )
            obj = await self._llm_json(prompts.SCOUT_SYSTEM, user)
            for ev in obj.get("events") or []:
                if not isinstance(ev, dict) or not str(ev.get("title", "")).strip():
                    continue
                all_events.append(
                    {
                        "title": str(ev.get("title", "")).strip()[:60],
                        "summary": str(ev.get("summary", "")).strip()[:400],
                        "participants": [str(p)[:60] for p in _as_list(ev.get("participants"))],
                        "significance": _to_int(ev.get("significance"), 5),
                        "reason": str(ev.get("reason", "")).strip()[:300],
                        "quotes": [q for q in (_clean_quote(x) for x in _as_list(ev.get("quotes"))) if q.get("quote")],
                    }
                )
        if len(chunks) > 1 and all_events:
            all_events = await self._merge_events(date_str, all_events)
        return all_events

    async def _merge_events(self, date_str: str, events: list[dict]) -> list[dict]:
        try:
            obj = await self._llm_json(
                prompts.SCOUT_MERGE_SYSTEM,
                prompts.SCOUT_MERGE_USER_TMPL.format(
                    date=date_str, candidates_json=json.dumps(events, ensure_ascii=False)[:60000]
                ),
            )
            merged = []
            for ev in obj.get("events") or []:
                if isinstance(ev, dict) and str(ev.get("title", "")).strip():
                    ev["significance"] = _to_int(ev.get("significance"), 5)
                    ev["quotes"] = [q for q in (_clean_quote(x) for x in _as_list(ev.get("quotes"))) if q.get("quote")]
                    merged.append(ev)
            return merged or events
        except Exception:
            return events  # 合并失败就用原始集合

    # ---------- 阶段二：定选 ----------

    def select_events(self, events: list[dict]) -> list[dict]:
        threshold = int(self._cfg("significance_threshold", 6))
        cap = int(self._cfg("max_entries_per_day", 2))
        picked = [e for e in events if e.get("significance", 0) >= threshold]
        picked.sort(key=lambda e: -e.get("significance", 0))
        return picked[: max(1, cap)]

    # ---------- 阶段三：撰写 / 修订 ----------

    async def write_new_entry(
        self, date_str: str, event: dict, existing_titles: list[str]
    ) -> dict:
        system = self._writer_system()
        user = prompts.WRITER_NEW_TMPL.format(
            date=date_str,
            candidate=json.dumps(event, ensure_ascii=False),
            existing_titles="、".join(existing_titles[-30:]) or "（暂无）",
        )
        obj = await self._llm_json(system, user)
        content = str(obj.get("content", "")).strip()
        if not content:
            raise PipelineError("撰写结果为空")
        return {
            "title": str(obj.get("title", "")).strip()[:80] or event["title"],
            "aliases": [str(a).strip()[:40] for a in _as_list(obj.get("aliases")) if str(a).strip()][:3],
            "categories": [str(c).strip().strip("/")[:24] for c in _as_list(obj.get("categories")) if str(c).strip()][:3],
            "significance": max(event.get("significance", 5), _to_int(obj.get("significance"), 5)),
            "content": content[:1800],
            "evidence_used": [q for q in (_clean_quote(x) for x in _as_list(obj.get("evidence_used"))) if q.get("quote")],
        }

    async def write_revision(
        self, serial: int, title: str, old_content: str, date_str: str, event: dict
    ) -> dict:
        system = self._writer_system()
        user = prompts.WRITER_REVISION_TMPL.format(
            serial=serial, title=title, old_content=old_content[:2400], date=date_str,
            candidate=json.dumps(event, ensure_ascii=False),
        )
        obj = await self._llm_json(system, user)
        content = str(obj.get("content", "")).strip()
        if not content:
            raise PipelineError("修订结果为空")
        return {
            "title": str(obj.get("title", "")).strip()[:80] or title,
            "aliases": [str(a).strip()[:40] for a in _as_list(obj.get("aliases")) if str(a).strip()][:3],
            "categories": [str(c).strip().strip("/")[:24] for c in _as_list(obj.get("categories")) if str(c).strip()][:3],
            "significance": _to_int(obj.get("significance"), event.get("significance", 5)),
            "content": content[:1800],
            "evidence_used": [q for q in (_clean_quote(x) for x in _as_list(obj.get("evidence_used"))) if q.get("quote")],
        }

    def _writer_system(self) -> str:
        style = (str(self._cfg("style_prompt", "")) or "").strip()
        base = prompts.DEFAULT_STYLE
        extra = f"\n\n【本群文体补充要求】\n{style}" if style else ""
        # 模板内含 JSON 花括号，不能用 str.format，用显式替换
        return prompts.WRITER_SYSTEM_TMPL.replace("{style}", base + extra)

    # ---------- 人物档案 ----------

    async def write_person_page(
        self, name: str, entries_brief: str, evidence_brief: str, samples: list[dict]
    ) -> str:
        sample_lines = []
        for s in samples[:300]:
            sample_lines.append(f"{s['time'][5:16]} {s['text'][:120]}")
        page = await self._llm_fn(
            prompts.PERSON_SYSTEM,
            prompts.PERSON_USER_TMPL.format(
                name=name,
                entries_brief=entries_brief or "（暂无）",
                evidence_brief=evidence_brief or "（暂无）",
                samples="\n".join(sample_lines) or "（样本不足）",
            ),
        )
        return page.strip()[:2000]


def _to_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default
