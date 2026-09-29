"""插件本地回归测试：storage / pipeline(假LLM) / exporter / fetcher 纯函数。"""
import sys, json, tempfile, asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from group_history.storage import HistoryDB
from group_history.pipeline import EditorialPipeline

tmp = Path(tempfile.mkdtemp())
db = HistoryDB(tmp / "t.db")

e1 = db.add_entry("yuehua:GroupMessage:123", "123", "凯子龙虾事件", ["龙虾之夜"], ["暴言", "深夜"],
                  "凯子龙虾事件，发生于某日凌晨……", 8, "2026-09-14",
                  evidence=[{"msg_time": "23:47", "sender": "凯子", "quote": "龙虾不是海鲜"}],
                  people=[{"name": "凯子", "sender_id": "10001"}])
assert e1["serial"] == 1, e1
assert db.find_candidate("yuehua:GroupMessage:123", "凯子龙虾事件", [])["id"] == e1["id"]
assert db.find_candidate("yuehua:GroupMessage:123", "龙虾之夜", [])["id"] == e1["id"]
assert db.find_candidate("yuehua:GroupMessage:456", "凯子龙虾事件", []) is None
u = db.update_revision(e1["id"], "修订后正文……", "9-15 后续进展", "gemini-test", 9, ["暴言"])
assert u["revision_count"] == 2 and u["significance"] == 9
assert len(db.get_revisions(e1["id"])) == 2
assert db.search("龙虾", umo="yuehua:GroupMessage:123")[0]["id"] == e1["id"]
assert db.random_entry("yuehua:GroupMessage:123")["id"] == e1["id"]
assert db.list_categories("yuehua:GroupMessage:123")["暴言"] == 1
assert db.entries_by_person("yuehua:GroupMessage:123", "凯子", "10001")[0]["id"] == e1["id"]
assert db.get_entry("1")["title"] == "凯子龙虾事件"
assert db.soft_delete(999) is False
db.save_entity_cache("凯子", "10001", "档案正文")
assert db.get_entity_cache("凯子") == "档案正文"
print("storage OK")

SCOUT_JSON = json.dumps({"events": [{
    "title": "喵妹嘴硬事件", "summary": "喵妹拒绝承认吃醋", "participants": ["喵妹", "凯子"],
    "significance": 8, "reason": "名场面",
    "quotes": [{"time": "21:03", "sender": "喵妹", "quote": "我才没有吃醋"}]}]}, ensure_ascii=False)
WRITE_JSON = json.dumps({
    "title": "喵妹嘴硬事件", "aliases": ["吃醋疑云"], "categories": ["暴言"],
    "significance": 8, "content": "喵妹嘴硬事件，发生于某夜……据《群聊实录》21时03分记载，喵妹曰：『我才没有吃醋』。",
    "evidence_used": [{"time": "21:03", "sender": "喵妹", "quote": "我才没有吃醋"}]}, ensure_ascii=False)


async def fake_llm(system, user):
    return SCOUT_JSON if "侦察员" in system else WRITE_JSON


cfg = {"max_context_chars": 120000, "chunk_chars": 45000, "significance_threshold": 6,
       "max_entries_per_day": 2, "style_prompt": ""}
pipe = EditorialPipeline(fake_llm, lambda k, d=None: cfg.get(k, d))
lines = ["21:03 喵妹：我才没有吃醋", "21:04 凯子：你脸红了", "21:04 喵妹：我这是气的"]
events = asyncio.run(pipe.scout_day("2026-09-29", lines))
assert len(events) == 1 and events[0]["title"] == "喵妹嘴硬事件"
picked = pipe.select_events(events)
assert len(picked) == 1
written = asyncio.run(pipe.write_new_entry("2026-09-29", picked[0], []))
assert written["title"] == "喵妹嘴硬事件" and "群聊实录" in written["content"]
rev = asyncio.run(pipe.write_revision(2, "喵妹嘴硬事件", "原正文……", "2026-09-30", picked[0]))
assert rev["content"]
e2 = db.add_entry("yuehua:GroupMessage:123", "123", written["title"], written["aliases"],
                  written["categories"], written["content"], written["significance"], "2026-09-29",
                  written["evidence_used"],
                  [{"name": p, "sender_id": ""} for p in picked[0]["participants"]])
assert e2["serial"] == 2
bad_calls = {"n": 0}


async def flaky_llm(system, user):
    bad_calls["n"] += 1
    return "这不是JSON" if bad_calls["n"] == 1 else SCOUT_JSON


pipe2 = EditorialPipeline(flaky_llm, lambda k, d=None: cfg.get(k, d))
events2 = asyncio.run(pipe2.scout_day("2026-09-28", ["10:00 a：hi"]))
assert events2 and bad_calls["n"] >= 2
low = dict(events2[0])
low["significance"] = 3
assert pipe.select_events([low]) == []
# 文体补充配置生效
cfg2 = dict(cfg, style_prompt="更冷面笑匠一些")
pipe3 = EditorialPipeline(fake_llm, lambda k, d=None: cfg2.get(k, d))
_ = asyncio.run(pipe3.write_new_entry("2026-09-29", picked[0], []))
print("pipeline OK")

from group_history.exporter import export_book
h, p = export_book([e2], tmp, "群 123")
assert h.exists() and h.stat().st_size > 3000 and p is None
print("exporter OK,", h.name, h.stat().st_size, "bytes")

from group_history.fetcher import (parse_chain_content, chunk_lines, subsample_lines,
                                   is_group_umo, parse_umo)
assert parse_chain_content('{"type":"user","message":[{"type":"plain","text":"哈喽"},'
                           '{"type":"at","qq":"123","name":"凯子"},{"type":"image","url":"x"}]}') \
       == "哈喽 @凯子 [图]"
assert is_group_umo("yuehua:GroupMessage:123") and not is_group_umo("webchat:FriendMessage:1")
assert parse_umo("yuehua:GroupMessage:123") == ("yuehua", "GroupMessage", "123")
big = [f"line {i} " + "x" * 50 for i in range(100)]
chunks = chunk_lines(big, 2000)
assert sum(len(c) for c in chunks) == len(big)
sub, sampled = subsample_lines(big, 1000)
assert sampled and len(sub) < len(big)
print("fetcher OK")
print("ALL_LOCAL_TESTS_PASSED")
