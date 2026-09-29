"""《群史》编纂委员会 - 群聊记录采集层

直接读取 AstrBot 主数据库 platform_message_history 表：
- user_id 列实际存放 unified_msg_origin（如 yuehua:GroupMessage:123456）
- content 列是序列化的消息链 JSON：{"type":"user","message":[{...},...]}
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# 各类消息段的紧凑表示
_SEGMENT_LABELS = {
    "image": "[图]",
    "face": "[表情]",
    "record": "[语音]",
    "video": "[视频]",
    "file": "[文件]",
    "reply": "[回复]",
    "forward": "[转发]",
    "json": "[卡片]",
    "xml": "[卡片]",
    "share": "[分享]",
    "poke": "[戳一戳]",
    "node": "[合并转发]",
    "markdown": "[md]",
}


def parse_umo(umo: str) -> tuple[str, str, str]:
    """yuehua:GroupMessage:123 -> (yuehua, GroupMessage, 123)"""
    parts = (umo or "").split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    return (umo or "", "", "")


def is_group_umo(umo: str) -> bool:
    parts = parse_umo(umo)
    return len(parts) == 3 and parts[1].lower() in ("groupmessage", "group")


def day_bounds(days_ago: int = 1, date_str: str | None = None) -> tuple[datetime, datetime]:
    """返回某自然日的 [start, end)。days_ago=1 即昨天。"""
    if date_str:
        base = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        base = datetime.now() - timedelta(days=days_ago)
    start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def parse_chain_content(raw: str) -> str:
    """把序列化消息链压成一行可读文本。"""
    text_parts: list[str] = []
    try:
        obj = json.loads(raw)
        chain = obj.get("message") or obj.get("chain") or []
    except Exception:
        raw = (raw or "").strip()
        return raw[:200] if raw else ""
    for seg in chain:
        if not isinstance(seg, dict):
            continue
        stype = str(seg.get("type", "")).lower()
        if stype == "plain":
            t = str(seg.get("text", "")).strip()
            if t:
                text_parts.append(t)
        elif stype == "at":
            name = seg.get("name") or seg.get("qq") or ""
            text_parts.append(f"@{name}" if name != "all" else "@全体成员")
        elif stype == "at_all":
            text_parts.append("@全体成员")
        else:
            label = _SEGMENT_LABELS.get(stype, f"[{stype}]" if stype else "")
            if label:
                text_parts.append(label)
    return " ".join(text_parts).strip()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_messages_between(
    db_path: Path, start: datetime, end: datetime, exclude_webchat: bool = True
) -> dict[str, list[dict]]:
    """按会话(umo)抓取时间段内全部消息。返回 {umo: [{time,sender,sender_id,text}]}，按时间升序。"""
    conn = _connect(db_path)
    try:
        sql = (
            "SELECT user_id, sender_id, sender_name, content, created_at "
            "FROM platform_message_history WHERE created_at >= ? AND created_at < ?"
        )
        args: list = [start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")]
        if exclude_webchat:
            sql += " AND platform_id != 'webchat'"
        sql += " ORDER BY user_id, created_at"
        grouped: dict[str, list[dict]] = {}
        for row in conn.execute(sql, args):
            umo = row["user_id"] or ""
            text = parse_chain_content(row["content"] or "")
            if not text:
                continue
            grouped.setdefault(umo, []).append(
                {
                    "time": str(row["created_at"]),
                    "sender": str(row["sender_name"] or row["sender_id"] or "无名氏"),
                    "sender_id": str(row["sender_id"] or ""),
                    "text": text,
                }
            )
        return grouped
    finally:
        conn.close()


def to_lines(rows: list[dict], max_line_chars: int = 220) -> list[str]:
    """把消息记录压成「HH:MM 名字：内容」的日志行。"""
    lines = []
    for r in rows:
        hhmm = ""
        try:
            hhmm = r["time"][11:16]
        except Exception:
            pass
        text = (r["text"] or "").replace("\n", " ")
        if len(text) > max_line_chars:
            text = text[:max_line_chars] + "…"
        lines.append(f"{hhmm} {r['sender']}：{text}")
    return lines


def chunk_lines(lines: list[str], chunk_chars: int) -> list[list[str]]:
    """按字符数把日志行切块，供多次侦察调用。"""
    if not lines:
        return []
    if chunk_chars <= 0:
        chunk_chars = 45000
    chunks: list[list[str]] = []
    cur: list[str] = []
    size = 0
    for line in lines:
        n = len(line) + 1
        if cur and size + n > chunk_chars:
            chunks.append(cur)
            cur, size = [], 0
        cur.append(line)
        size += n
    if cur:
        chunks.append(cur)
    return chunks


def subsample_lines(lines: list[str], max_total_chars: int) -> tuple[list[str], bool]:
    """总字符超限时均匀抽稀。返回 (lines, was_sampled)。"""
    total = sum(len(x) + 1 for x in lines)
    if total <= max_total_chars or not lines:
        return lines, False
    keep_ratio = max_total_chars / total
    keep_n = max(50, int(len(lines) * keep_ratio))
    if keep_n >= len(lines):
        return lines, False
    step = len(lines) / keep_n
    picked = [lines[int(i * step)] for i in range(keep_n)]
    return picked, True


def lookup_sender(
    db_path: Path, sender_id: str = "", sender_name: str = ""
) -> Optional[dict]:
    """反查某个 QQ 号对应的最近一次发言记录（拿到昵称）。"""
    conn = _connect(db_path)
    try:
        if sender_id:
            row = conn.execute(
                "SELECT sender_id, sender_name FROM platform_message_history "
                "WHERE sender_id=? AND sender_name != '' ORDER BY id DESC LIMIT 1",
                (sender_id,),
            ).fetchone()
            if row:
                return {"sender_id": row["sender_id"], "sender_name": row["sender_name"]}
        if sender_name:
            row = conn.execute(
                "SELECT sender_id, sender_name FROM platform_message_history "
                "WHERE sender_name=? ORDER BY id DESC LIMIT 1",
                (sender_name,),
            ).fetchone()
            if row:
                return {"sender_id": row["sender_id"], "sender_name": row["sender_name"]}
        return None
    finally:
        conn.close()


def fetch_sender_samples(
    db_path: Path, sender_id: str = "", sender_name: str = "", limit: int = 300
) -> list[dict]:
    """抓取某人最近的发言样本（倒序取再反转成正序）。"""
    conn = _connect(db_path)
    try:
        if sender_id:
            cond, args = "sender_id=?", [sender_id]
        elif sender_name:
            cond, args = "sender_name=?", [sender_name]
        else:
            return []
        rows = conn.execute(
            f"SELECT sender_id, sender_name, content, created_at FROM platform_message_history "
            f"WHERE {cond} AND platform_id != 'webchat' ORDER BY id DESC LIMIT ?",
            (*args, limit),
        ).fetchall()
        out = []
        for row in reversed(rows):
            text = parse_chain_content(row["content"] or "")
            if text:
                out.append(
                    {
                        "time": str(row["created_at"]),
                        "sender": str(row["sender_name"] or ""),
                        "sender_id": str(row["sender_id"] or ""),
                        "text": text,
                    }
                )
        return out
    finally:
        conn.close()


def overall_message_count(db_path: Path) -> int:
    conn = _connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM platform_message_history").fetchone()[0]
    finally:
        conn.close()


# ---------------- OneBot(napcat) 历史抓取 ----------------

_CQ_RE = None


def _parse_onebot_segments(message) -> str:
    """OneBot 消息体 → 一行文本。兼容数组格式与 CQ 码字符串格式。"""
    if message is None:
        return ""
    if isinstance(message, str):
        # CQ 码字符串：去掉 [CQ:xxx,data] 保留纯文本
        import re

        text = re.sub(r"\[CQ:[^\]]*\]", " ", message)
        return text.strip()[:300]
    parts = []
    for seg in message or []:
        if not isinstance(seg, dict):
            continue
        stype = str(seg.get("type", "")).lower()
        data = seg.get("data") or {}
        if stype == "text":
            t = str(data.get("text", "")).strip()
            if t:
                parts.append(t)
        elif stype == "at":
            qq = str(data.get("qq", ""))
            if qq == "all":
                parts.append("@全体成员")
            else:
                parts.append(f"@{data.get('name') or qq}")
        else:
            label = _SEGMENT_LABELS.get(stype, f"[{stype}]" if stype else "")
            if label:
                parts.append(label)
    return " ".join(parts).strip()[:400]


def _normalize_ob_message(m: dict) -> dict | None:
    """把一条 OneBot 消息归一化成 {key, seq, mid, ts, sender, sender_id, text}。"""
    if not isinstance(m, dict):
        return None
    sender = m.get("sender") or {}
    seq = m.get("message_seq") or m.get("real_id") or m.get("seq")
    mid = m.get("message_id") or m.get("real_id") or m.get("message_seq")
    ts = int(m.get("time") or 0)
    text = _parse_onebot_segments(m.get("message"))
    if not text:
        return None
    return {
        "key": str(mid) if mid is not None else f"{ts}:{seq}",
        "seq": int(seq) if seq is not None else 0,
        "mid": mid,
        "ts": ts,
        "sender": str(sender.get("card") or sender.get("nickname") or sender.get("user_id") or "无名氏"),
        "sender_id": str(sender.get("user_id") or ""),
        "text": text,
    }


def _extract_ob_messages(resp) -> list:
    """从 call_action 的返回中抽出消息数组（兼容包了一层 data 或没包的情况）。"""
    data = resp
    if isinstance(resp, dict):
        data = resp.get("data", resp)
        if isinstance(data, dict):
            msgs = data.get("messages") or data.get("message") or []
            return msgs if isinstance(msgs, list) else []
    if isinstance(data, list):
        return data
    return []


async def onebot_fetch_day(bot, group_id, start_ts: int, end_ts: int,
                           max_count: int = 6000, chunk_size: int = 100) -> list[dict]:
    """经 OneBot get_group_msg_history 回溯抓取 [start_ts, end_ts) 的群消息。

    分页方案抄自 astrbot_plugin_qq_group_daily_analysis（已在本环境验证可行）：
    - NapCat 需传 reverseOrder=True 做反向回溯
    - 锚点 = 本批时间最早消息的 message_seq/real_id/seq，不做 -1 偏移
    - 以 message_id 去重，锚点不动或时间到达起点即停
    """
    collected: dict[str, dict] = {}
    anchor = None
    now_ts = int(datetime.now().timestamp())
    while len(collected) < max_count:
        params = {
            "group_id": int(group_id),
            "count": min(chunk_size, max_count - len(collected)),
            "reverseOrder": True,
        }
        if anchor is not None:
            params["message_seq"] = anchor
        try:
            result = await bot.call_action("get_group_msg_history", **params)
        except Exception:
            break
        messages = []
        if isinstance(result, dict):
            messages = result.get("messages") or result.get("data", {}).get("messages") or []
        if not messages:
            break
        normalized = [m for m in (_normalize_ob_message(x) for x in messages) if m]
        fresh = [m for m in normalized if m["key"] not in collected]
        if not fresh:
            break
        for m in fresh:
            collected[m["key"]] = m
        earliest = min(fresh, key=lambda x: x["ts"])
        if earliest["ts"] <= start_ts or earliest["ts"] > now_ts:
            break
        new_anchor = earliest["seq"] or earliest["mid"]
        if new_anchor is None or (anchor is not None and str(new_anchor) == str(anchor)):
            break
        anchor = new_anchor
    rows = [
        {
            "time": datetime.fromtimestamp(m["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
            "_ts": m["ts"],
            "sender": m["sender"],
            "sender_id": m["sender_id"],
            "text": m["text"],
        }
        for m in sorted(collected.values(), key=lambda x: x["ts"])
        if start_ts <= m["ts"] < end_ts
    ]
    return rows


async def onebot_probe_history(bot, group_id) -> dict:
    """调试：报告 bot 连接状态并拉两页历史，用于实证分页方向。"""
    probe = {}

    async def safe(action, **kwargs):
        try:
            r = await bot.call_action(action, **kwargs)
            return {"ok": True, "resp_type": str(type(r))[:60]}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}", "repr": repr(e)[:200]}

    probe["get_login_info"] = await safe("get_login_info")
    probe["get_version_info"] = await safe("get_version_info")

    async def one_page(cursor):
        kwargs = {"group_id": int(group_id), "count": 20}
        if cursor is not None:
            kwargs["message_seq"] = cursor
        try:
            resp = await bot.call_action("get_group_msg_history", **kwargs)
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}", "repr": repr(e)[:200]}
        msgs = [m for m in (_normalize_ob_message(x) for x in _extract_ob_messages(resp)) if m]
        if not msgs:
            return {"count": 0, "resp_type": str(type(resp))[:60],
                    "resp_head": repr(resp)[:400]}
        return {
            "count": len(msgs),
            "seq_range": [min(m["seq"] for m in msgs), max(m["seq"] for m in msgs)],
            "time_range": [min(m["ts"] for m in msgs), max(m["ts"] for m in msgs)],
        }

    probe["page1_no_cursor"] = await one_page(None)
    p1 = probe["page1_no_cursor"]
    if "seq_range" in p1:
        probe["page2_at_min_seq"] = await one_page(p1["seq_range"][0])
        probe["page3_at_min_seq_minus1"] = await one_page(p1["seq_range"][0] - 1)
    return probe


async def onebot_group_list(bot) -> list[dict]:
    """经 OneBot 拉群列表，[{group_id, group_name}]。失败返回空。"""
    try:
        resp = await bot.call_action("get_group_list")
    except Exception:
        return []
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    if not isinstance(data, list):
        return []
    return [
        {"group_id": str(g.get("group_id")), "group_name": str(g.get("group_name") or "")}
        for g in data
        if isinstance(g, dict) and g.get("group_id")
    ]


def next_announce_due_ts(now_ts: int, announce_hour: int) -> int:
    """计算下一次官宣时刻：announce_hour 点的下一个出现（编纂在夜里跑，官宣在白天发）。"""
    from datetime import datetime as _dt

    now = _dt.fromtimestamp(now_ts)
    due = now.replace(hour=announce_hour, minute=0, second=0, microsecond=0)
    if due <= now:
        due += timedelta(days=1)
    return int(due.timestamp())
