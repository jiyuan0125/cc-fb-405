#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sync_tkt2.py — 三源工单事件同步进 SQLite，并维护水位文件。

源与游标：
  src_web  追加式 jsonl，游标 = 已处理行数
  src_app  批号目录（目录名自增），游标 = 已处理批目录名列表
  src_ivr  csv 增量（按事件时间），游标 = 最后处理到的事件时间

幂等设计：
  - events 表以 事件号 为主键，重投事件 INSERT OR IGNORE，不会出两回；
  - tickets 表记录工单最新状态，仅当新事件时间更晚才更新，状态不会卷回旧值；
  - 每个源先在事务内提交 DB，再原子改写水位文件；崩溃后重跑由主键去重兜底。
"""

import csv
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tkt2.db")
WATERMARK_PATH = os.path.join(BASE_DIR, "watermark.json")

SRC_WEB = os.path.join(BASE_DIR, "src_web", "events.jsonl")
SRC_APP = os.path.join(BASE_DIR, "src_app")
SRC_IVR = os.path.join(BASE_DIR, "src_ivr", "events.csv")

FIELDS = ("事件号", "工单号", "状态", "时间")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    事件号 TEXT PRIMARY KEY,
    工单号 TEXT NOT NULL,
    状态   TEXT NOT NULL,
    时间   TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tickets (
    工单号 TEXT PRIMARY KEY,
    状态   TEXT NOT NULL,
    时间   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    source       TEXT NOT NULL,
    ingested     INTEGER NOT NULL,
    skipped      INTEGER NOT NULL,
    cursor_after TEXT NOT NULL,
    logged_at    TEXT NOT NULL
);
"""

UPSERT_TICKET = """
INSERT INTO tickets (工单号, 状态, 时间) VALUES (?, ?, ?)
ON CONFLICT(工单号) DO UPDATE SET 状态 = excluded.状态, 时间 = excluded.时间
WHERE excluded.时间 > tickets.时间
"""


def load_watermark():
    if os.path.exists(WATERMARK_PATH):
        with open(WATERMARK_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"src_web": {"lines": 0}, "src_app": {"batches": []}, "src_ivr": {"last_time": ""}}


def save_watermark(wm):
    # 原子写：先写临时文件再 rename，避免崩溃留下半个文件
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(WATERMARK_PATH), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(wm, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, WATERMARK_PATH)
    except BaseException:
        os.unlink(tmp)
        raise


def ingest_event(conn, source, ev):
    """入账一条事件。返回 True=新入账，False=重投/重复跳过。"""
    cur = conn.execute(
        "INSERT OR IGNORE INTO events (事件号, 工单号, 状态, 时间, source) VALUES (?,?,?,?,?)",
        (ev["事件号"], ev["工单号"], ev["状态"], ev["时间"], source),
    )
    if cur.rowcount == 0:
        return False
    # 仅当该事件更新时才推进工单最新状态，防止旧事件把状态卷回
    conn.execute(UPSERT_TICKET, (ev["工单号"], ev["状态"], ev["时间"]))
    return True


def valid_event(ev):
    return all(ev.get(k) for k in FIELDS)


def log_sync(conn, run_id, source, ingested, skipped, cursor_after):
    conn.execute(
        "INSERT INTO sync_log (run_id, source, ingested, skipped, cursor_after, logged_at) "
        "VALUES (?,?,?,?,?,?)",
        (run_id, source, ingested, skipped, cursor_after,
         datetime.now(timezone.utc).isoformat(timespec="seconds")),
    )


def sync_web(conn, wm, run_id):
    start = wm["src_web"]["lines"]
    ingested = skipped = 0
    lines_done = start
    if os.path.exists(SRC_WEB):
        with open(SRC_WEB, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < start:
                    continue
                lines_done = i + 1
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    skipped += 1
                    continue
                if not valid_event(ev):
                    skipped += 1
                    continue
                if ingest_event(conn, "src_web", ev):
                    ingested += 1
                else:
                    skipped += 1
    wm["src_web"]["lines"] = lines_done
    log_sync(conn, run_id, "src_web", ingested, skipped, f"lines={lines_done}")
    return ingested, skipped


def sync_app(conn, wm, run_id):
    done = set(wm["src_app"]["batches"])
    ingested = skipped = 0
    if os.path.isdir(SRC_APP):
        for name in sorted(os.listdir(SRC_APP)):
            path = os.path.join(SRC_APP, name)
            if not os.path.isdir(path) or name in done:
                continue
            events_file = os.path.join(path, "events.jsonl")
            if os.path.exists(events_file):
                with open(events_file, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except json.JSONDecodeError:
                            skipped += 1
                            continue
                        if not valid_event(ev):
                            skipped += 1
                            continue
                        if ingest_event(conn, "src_app", ev):
                            ingested += 1
                        else:
                            skipped += 1
            done.add(name)
    wm["src_app"]["batches"] = sorted(done)
    log_sync(conn, run_id, "src_app", ingested, skipped,
             "batches=" + ",".join(wm["src_app"]["batches"]))
    return ingested, skipped


def sync_ivr(conn, wm, run_id):
    last_time = wm["src_ivr"]["last_time"]
    max_time = last_time
    ingested = skipped = 0
    if os.path.exists(SRC_IVR):
        with open(SRC_IVR, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ev = {k: (row.get(k) or "").strip() for k in FIELDS}
                if not valid_event(ev):
                    skipped += 1
                    continue
                # 取 >= 游标的行：边界同时间戳的行靠主键去重，不会漏也不会重
                if last_time and ev["时间"] < last_time:
                    continue
                if ingest_event(conn, "src_ivr", ev):
                    ingested += 1
                else:
                    skipped += 1
                if ev["时间"] > max_time:
                    max_time = ev["时间"]
    wm["src_ivr"]["last_time"] = max_time
    log_sync(conn, run_id, "src_ivr", ingested, skipped, f"last_time={max_time}")
    return ingested, skipped


def main():
    run_id = datetime.now(timezone.utc).isoformat(timespec="seconds")
    wm = load_watermark()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        for sync_fn in (sync_web, sync_app, sync_ivr):
            with conn:  # 每个源一个事务：先落库，再写水位
                ingested, skipped = sync_fn(conn, wm, run_id)
            save_watermark(wm)
            print(f"[{run_id}] {sync_fn.__name__}: 入账 {ingested}, 跳过 {skipped}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
