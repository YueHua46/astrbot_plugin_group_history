"""《群史》编纂委员会 - 存储层

独立的 SQLite 存储，不污染 AstrBot 主数据库。
表结构：entries(词条) / revisions(修订史) / evidence(引用) / mentions(人物关联)
       / entities(人物档案缓存) / meta(内部状态)
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS entries(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    serial INTEGER UNIQUE,
    umo TEXT NOT NULL DEFAULT '',
    group_id TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    aliases TEXT NOT NULL DEFAULT '[]',
    categories TEXT NOT NULL DEFAULT '[]',
    content TEXT NOT NULL,
    significance INTEGER NOT NULL DEFAULT 5,
    first_date TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    revision_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'published'
);
CREATE TABLE IF NOT EXISTS revisions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL,
    msg_time TEXT NOT NULL DEFAULT '',
    sender TEXT NOT NULL DEFAULT '',
    quote TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mentions(
    entry_id INTEGER NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    sender_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_mentions_name ON mentions(name);
CREATE INDEX IF NOT EXISTS idx_mentions_sid ON mentions(sender_id);
CREATE TABLE IF NOT EXISTS entities(
    name TEXT PRIMARY KEY,
    sender_id TEXT NOT NULL DEFAULT '',
    page TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_messages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    umo TEXT NOT NULL DEFAULT '',
    group_id TEXT NOT NULL DEFAULT '',
    sender_id TEXT NOT NULL DEFAULT '',
    sender_name TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL DEFAULT '',
    ts INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_dedupe
    ON raw_messages(umo, ts, sender_id, substr(text,1,64));
CREATE INDEX IF NOT EXISTS idx_raw_group_ts ON raw_messages(group_id, ts);
CREATE TABLE IF NOT EXISTS announce_queue(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    umo TEXT NOT NULL,
    text TEXT NOT NULL,
    due_ts INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_due ON announce_queue(due_ts);
"""

_WORD_RE = re.compile(r"[\w\u4e00-\u9fff]+")


def _norm(text: str) -> str:
    """词条标题归一化：只保留字母数字汉字，转小写。"""
    return "".join(_WORD_RE.findall((text or "").lower()))


class HistoryDB:
    """线程安全的《群史》存储。操作都是毫秒级，直接同步调用即可。"""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self):
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    # ---------- meta ----------

    def get_meta(self, key: str) -> Optional[str]:
        cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ---------- 词条 ----------

    @staticmethod
    def _row_to_entry(row: sqlite3.Row) -> dict:
        try:
            aliases = json.loads(row["aliases"])
        except Exception:
            aliases = []
        try:
            categories = json.loads(row["categories"])
        except Exception:
            categories = []
        return {
            "id": row["id"],
            "serial": row["serial"],
            "umo": row["umo"],
            "group_id": row["group_id"],
            "title": row["title"],
            "aliases": aliases,
            "categories": categories,
            "content": row["content"],
            "significance": row["significance"],
            "first_date": row["first_date"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "revision_count": row["revision_count"],
            "status": row["status"],
        }

    def add_entry(
        self,
        umo: str,
        group_id: str,
        title: str,
        aliases: list[str],
        categories: list[str],
        content: str,
        significance: int,
        first_date: str,
        evidence: list[dict] | None = None,
        people: list[dict] | None = None,
    ) -> dict:
        """新建词条，返回完整词条 dict。evidence: [{msg_time,sender,quote}] people: [{name,sender_id}]"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock, self._conn:
            cur = self._conn.execute("SELECT COALESCE(MAX(serial),0)+1 FROM entries")
            serial = cur.fetchone()[0]
            cur = self._conn.execute(
                "INSERT INTO entries(serial,umo,group_id,title,aliases,categories,content,"
                "significance,first_date,created_at,updated_at,revision_count,status) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,'1','published')",
                (
                    serial,
                    umo,
                    group_id,
                    title,
                    json.dumps(aliases, ensure_ascii=False),
                    json.dumps(categories, ensure_ascii=False),
                    content,
                    max(1, min(10, int(significance or 5))),
                    first_date,
                    now,
                    now,
                ),
            )
            entry_id = cur.lastrowid
            self._conn.execute(
                "INSERT INTO revisions(entry_id,content,reason,model,created_at) "
                "VALUES(?,?,'初版（编委会立卷）','',?)",
                (entry_id, content, now),
            )
        self._save_children(entry_id, evidence, people)
        row = self._conn.execute("SELECT * FROM entries WHERE id=?", (entry_id,)).fetchone()
        return self._row_to_entry(row)

    def _save_children(self, entry_id: int, evidence: list[dict] | None, people: list[dict] | None):
        with self._lock, self._conn:
            for ev in evidence or []:
                self._conn.execute(
                    "INSERT INTO evidence(entry_id,msg_time,sender,quote) VALUES(?,?,?,?)",
                    (entry_id, str(ev.get("msg_time", "")), str(ev.get("sender", "")), str(ev.get("quote", ""))[:500]),
                )
            for p in people or []:
                name = str(p.get("name", "")).strip()
                if not name:
                    continue
                self._conn.execute(
                    "INSERT INTO mentions(entry_id,name,sender_id) VALUES(?,?,?)",
                    (entry_id, name, str(p.get("sender_id", ""))),
                )

    def find_candidate(
        self, umo: str, title: str, aliases: list[str]
    ) -> Optional[dict]:
        """依据标题与别名做模糊匹配，判断某事件是否已有词条（应走修订而非新建）。"""
        new_names = {_norm(title)} | {_norm(a) for a in aliases if a}
        new_names.discard("")
        if not new_names:
            return None
        rows = self._conn.execute(
            "SELECT * FROM entries WHERE umo=? AND status='published'", (umo,)
        ).fetchall()
        for row in rows:
            e = self._row_to_entry(row)
            old_names = {_norm(e["title"])} | {_norm(a) for a in e["aliases"]}
            old_names.discard("")
            if not old_names:
                continue
            if new_names & old_names:
                return e
            # 包含式匹配（长标题互含）
            for a in new_names | old_names:
                if len(a) >= 4:
                    for b in (new_names if a in new_names else old_names):
                        if len(b) >= 4 and b != a and (b in a or a in b):
                            return e
        return None

    def update_revision(
        self, entry_id: int, content: str, reason: str, model: str,
        significance: int | None = None, categories: list[str] | None = None,
        evidence: list[dict] | None = None, people: list[dict] | None = None,
    ) -> Optional[dict]:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO revisions(entry_id,content,reason,model,created_at) VALUES(?,?,?,?,?)",
                (entry_id, content, reason, model, now),
            )
            if significance is not None:
                self._conn.execute(
                    "UPDATE entries SET content=?, updated_at=?, "
                    "revision_count=revision_count+1, significance=? WHERE id=?",
                    (content, now, max(1, min(10, int(significance))), entry_id),
                )
            else:
                self._conn.execute(
                    "UPDATE entries SET content=?, updated_at=?, "
                    "revision_count=revision_count+1 WHERE id=?",
                    (content, now, entry_id),
                )
            if categories:
                row = self._conn.execute("SELECT categories FROM entries WHERE id=?", (entry_id,)).fetchone()
                try:
                    old = json.loads(row["categories"]) if row else []
                except Exception:
                    old = []
                merged = list(dict.fromkeys([*old, *categories]))
                self._conn.execute(
                    "UPDATE entries SET categories=? WHERE id=?",
                    (json.dumps(merged, ensure_ascii=False), entry_id),
                )
        self._save_children(entry_id, evidence, people)
        row = self._conn.execute("SELECT * FROM entries WHERE id=?", (entry_id,)).fetchone()
        return self._row_to_entry(row) if row else None

    def get_entry(self, ref: str, umo: str | None = None) -> Optional[dict]:
        """按编号（纯数字）或标题取词条。"""
        q = ref.strip()
        if q.isdigit():
            row = self._conn.execute(
                "SELECT * FROM entries WHERE serial=? AND status='published'", (int(q),)
            ).fetchone()
            if row and (umo is None or row["umo"] == umo):
                return self._row_to_entry(row)
            return None
        row = self._conn.execute(
            "SELECT * FROM entries WHERE title=? AND status='published'", (q,)
        ).fetchone()
        if row:
            return self._row_to_entry(row)
        # 标题模糊兜底
        rows = self.search(q, umo=umo, limit=1)
        return rows[0] if rows else None

    def get_revisions(self, entry_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM revisions WHERE entry_id=? ORDER BY id", (entry_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_evidence(self, entry_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM evidence WHERE entry_id=? ORDER BY id", (entry_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def search(self, keyword: str, umo: str | None = None, limit: int = 10) -> list[dict]:
        like = f"%{keyword.strip()}%"
        sql = ("SELECT * FROM entries WHERE status='published' AND "
               "(title LIKE ? OR aliases LIKE ? OR content LIKE ? OR categories LIKE ?)")
        args: list = [like, like, like, like]
        if umo:
            sql += " AND umo=?"
            args.append(umo)
        sql += " ORDER BY significance DESC, serial DESC LIMIT ?"
        args.append(limit)
        rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def random_entry(self, umo: str) -> Optional[dict]:
        rows = self._conn.execute(
            "SELECT * FROM entries WHERE umo=? AND status='published'", (umo,)
        ).fetchall()
        if not rows:
            return None
        entries = [self._row_to_entry(r) for r in rows]
        weights = [max(1, e["significance"]) for e in entries]
        import random as _r

        return _r.choices(entries, weights=weights, k=1)[0]

    def list_entries(self, umo: str | None = None, limit: int = 20, offset: int = 0) -> list[dict]:
        sql = "SELECT * FROM entries WHERE status='published'"
        args: list = []
        if umo:
            sql += " AND umo=?"
            args.append(umo)
        sql += " ORDER BY serial DESC LIMIT ? OFFSET ?"
        args += [limit, offset]
        return [self._row_to_entry(r) for r in self._conn.execute(sql, args).fetchall()]

    def count_entries(self, umo: str | None = None, include_deleted: bool = False) -> int:
        sql = "SELECT COUNT(*) FROM entries"
        cond, args = [], []
        if not include_deleted:
            cond.append("status='published'")
        if umo:
            cond.append("umo=?")
            args.append(umo)
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        return self._conn.execute(sql, args).fetchone()[0]

    def list_categories(self, umo: str) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT categories FROM entries WHERE umo=? AND status='published'", (umo,)
        ).fetchall()
        counter: dict[str, int] = {}
        for row in rows:
            try:
                cats = json.loads(row["categories"])
            except Exception:
                continue
            for c in cats or []:
                c = str(c).strip()
                if c:
                    counter[c] = counter.get(c, 0) + 1
        return dict(sorted(counter.items(), key=lambda kv: -kv[1]))

    def entries_by_category(self, umo: str, category: str) -> list[dict]:
        like = f"%{category}%"
        rows = self._conn.execute(
            "SELECT * FROM entries WHERE umo=? AND status='published' AND categories LIKE ? "
            "ORDER BY serial DESC",
            (umo, like),
        ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def soft_delete(self, serial: int) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE entries SET status='deleted' WHERE serial=?", (serial,)
            )
            return cur.rowcount > 0

    def entries_by_person(self, umo: str, name: str, sender_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT DISTINCT e.* FROM entries e JOIN mentions m ON m.entry_id=e.id "
            "WHERE e.umo=? AND e.status='published' AND (m.sender_id=? OR m.name=?) "
            "ORDER BY e.serial DESC",
            (umo, sender_id, name),
        ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def person_evidence(self, name: str, sender_id: str, limit: int = 40) -> list[dict]:
        rows = self._conn.execute(
            "SELECT ev.* FROM evidence ev JOIN mentions m ON m.entry_id=ev.entry_id "
            "WHERE m.sender_id=? OR m.name=? ORDER BY ev.id DESC LIMIT ?",
            (sender_id, name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------- 人物档案缓存 ----------

    def get_entity_cache(self, name: str, ttl_hours: int = 168) -> Optional[str]:
        row = self._conn.execute("SELECT * FROM entities WHERE name=?", (name,)).fetchone()
        if not row:
            return None
        try:
            ts = datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S")
            hours = (datetime.now() - ts).total_seconds() / 3600
            if hours <= ttl_hours:
                return row["page"]
        except Exception:
            pass
        return None

    def save_entity_cache(self, name: str, sender_id: str, page: str):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO entities(name,sender_id,page,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET sender_id=excluded.sender_id, "
                "page=excluded.page, updated_at=excluded.updated_at",
                (name, sender_id, page, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )

    # ---------- 实时采录卷宗（raw_messages） ----------

    def insert_raw(self, umo: str, group_id: str, sender_id: str, sender_name: str,
                   text: str, ts: int):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO raw_messages(umo,group_id,sender_id,sender_name,text,ts) "
                "VALUES(?,?,?,?,?,?)",
                (umo, group_id, sender_id, sender_name, text, int(ts)),
            )

    def insert_raw_many(self, umo: str, group_id: str, rows: list[dict], fallback_ts: int):
        """批量入库 napcat 回填的消息（行结构见 fetcher.onebot_fetch_day）。"""
        with self._lock, self._conn:
            for r in rows:
                ts = r.get("_ts") or fallback_ts
                try:
                    from datetime import datetime as _dt

                    ts = int(_dt.strptime(r["time"], "%Y-%m-%d %H:%M:%S").timestamp())
                except Exception:
                    pass
                self._conn.execute(
                    "INSERT OR IGNORE INTO raw_messages(umo,group_id,sender_id,sender_name,text,ts) "
                    "VALUES(?,?,?,?,?,?)",
                    (umo, group_id, str(r.get("sender_id", "")), str(r.get("sender", "")),
                     str(r.get("text", "")), int(ts)),
                )

    def raw_day_rows(self, group_id: str, start_ts: int, end_ts: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT sender_id, sender_name, text, ts FROM raw_messages "
            "WHERE group_id=? AND ts>=? AND ts<? ORDER BY ts, id",
            (group_id, start_ts, end_ts),
        ).fetchall()
        return [
            {
                "time": datetime.fromtimestamp(r["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
                "sender": r["sender_name"] or r["sender_id"],
                "sender_id": r["sender_id"],
                "text": r["text"],
            }
            for r in rows
        ]

    def raw_group_ids_between(self, start_ts: int, end_ts: int) -> list[tuple[str, str, int]]:
        """返回时间段内活跃会话 [(umo, group_id, count)]，按消息量降序。"""
        rows = self._conn.execute(
            "SELECT umo, group_id, COUNT(*) n FROM raw_messages WHERE ts>=? AND ts<? "
            "GROUP BY umo, group_id ORDER BY n DESC",
            (start_ts, end_ts),
        ).fetchall()
        return [(r["umo"], r["group_id"], r["n"]) for r in rows]

    def raw_count_between(self, group_id: str, start_ts: int, end_ts: int) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) FROM raw_messages WHERE group_id=? AND ts>=? AND ts<?",
            (group_id, start_ts, end_ts),
        ).fetchone()[0]

    def raw_all_group_ids(self) -> list[str]:
        return [
            r["group_id"]
            for r in self._conn.execute("SELECT DISTINCT group_id FROM raw_messages").fetchall()
        ]

    def raw_sender_samples(self, sender_id: str = "", sender_name: str = "",
                           limit: int = 300) -> list[dict]:
        if sender_id:
            cond, args = "sender_id=?", [sender_id]
        elif sender_name:
            cond, args = "sender_name=?", [sender_name]
        else:
            return []
        rows = self._conn.execute(
            f"SELECT sender_id, sender_name, text, ts FROM raw_messages WHERE {cond} "
            f"ORDER BY id DESC LIMIT ?",
            (*args, limit),
        ).fetchall()
        rows = list(reversed(rows))
        return [
            {
                "time": datetime.fromtimestamp(r["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
                "sender": r["sender_name"] or r["sender_id"],
                "sender_id": r["sender_id"],
                "text": r["text"],
            }
            for r in rows
        ]

    def raw_lookup_sender(self, sender_id: str = "", sender_name: str = "") -> Optional[dict]:
        if sender_id:
            row = self._conn.execute(
                "SELECT sender_id, sender_name FROM raw_messages WHERE sender_id=? AND "
                "sender_name!='' ORDER BY id DESC LIMIT 1",
                (sender_id,),
            ).fetchone()
        elif sender_name:
            row = self._conn.execute(
                "SELECT sender_id, sender_name FROM raw_messages WHERE sender_name=? "
                "ORDER BY id DESC LIMIT 1",
                (sender_name,),
            ).fetchone()
        else:
            return None
        if row:
            return {"sender_id": row["sender_id"], "sender_name": row["sender_name"]}
        return None

    # ---------- 官宣队列（夜间编纂，白天发报） ----------

    def queue_announce(self, umo: str, text: str, due_ts: int):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO announce_queue(umo,text,due_ts,attempts,created_at) "
                "VALUES(?,?,?,0,?)",
                (umo, text, int(due_ts), datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )

    def due_announcements(self, now_ts: int, limit: int = 20) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM announce_queue WHERE due_ts<=? AND attempts<3 ORDER BY id LIMIT ?",
            (int(now_ts), limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_announce_sent(self, item_id: int):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM announce_queue WHERE id=?", (item_id,))

    def bump_announce_attempt(self, item_id: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE announce_queue SET attempts=attempts+1 WHERE id=?", (item_id,)
            )

    def queue_size(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM announce_queue").fetchone()[0]

    # ---------- 统计 ----------

    def stats(self) -> dict:
        entries = self.count_entries(include_deleted=False)
        deleted = self.count_entries(include_deleted=True) - entries
        revs = self._conn.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
        evids = self._conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]
        return {"entries": entries, "deleted": deleted, "revisions": revs, "evidence": evids}
