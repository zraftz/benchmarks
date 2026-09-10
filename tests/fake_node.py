#!/usr/bin/env python3
"""TEST DOUBLE ONLY. Shared SQLite is NOT Raft, replication or benchmark evidence.

Three such processes exercise orchestration, wire framing, retries, process
cleanup, and result handling. Public CLI allowlists exclude this backend.
"""
import argparse
import json
from pathlib import Path
import socketserver
import sqlite3
import struct
import threading
import time

p = argparse.ArgumentParser()
p.add_argument("--config", required=True)
a = p.parse_args()
c = json.loads(Path(a.config).read_text())
path = Path(c["data_dir"]).parent / "TEST-DOUBLE-SHARED.sqlite"
path.parent.mkdir(parents=True, exist_ok=True)

def connect():
    db = sqlite3.connect(path, timeout=10)
    db.execute("PRAGMA busy_timeout=10000")
    return db

for attempt in range(20):
    try:
        with connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS nodes(id INTEGER PRIMARY KEY,t REAL)")
            db.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY,v TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS sessions(c TEXT PRIMARY KEY,s INTEGER,cmd TEXT,outcome TEXT)")
        break
    except sqlite3.OperationalError:
        time.sleep(.05)
else:
    raise RuntimeError("fixture database initialization failed")

def heartbeat():
    while True:
        try:
            with connect() as db:
                db.execute("INSERT OR REPLACE INTO nodes VALUES (?,?)", (c["id"], time.time()))
        except sqlite3.OperationalError:
            pass
        time.sleep(.1)
threading.Thread(target=heartbeat, daemon=True).start()

def current(db):
    rows = db.execute("SELECT id FROM nodes WHERE t>? ORDER BY id", (time.time() - .75,)).fetchall()
    return rows[0][0] if len(rows) >= 2 else None

def dispatch(q):
    with connect() as db:
        lead = current(db)
        if q["op"] == "status":
            return {"status": "ok", "leader_id": lead, "info": {"implementation": "test-fixture", "node_id": c["id"], "leader": lead == c["id"], "contract": "durable-log+durable-application-v1/logged-reads"}}
        if q["op"] == "dump":
            return {"status": "ok", "info": {"values": dict(db.execute("SELECT k,v FROM kv"))}}
        if q["op"] != "execute":
            return {"status": "error", "detail": "unsupported fixture operation"}
        if lead != c["id"]:
            return {"status": "not_leader", "leader_id": lead}
        cmd = q["command"]
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT s,cmd,outcome FROM sessions WHERE c=?", (cmd["client"],)).fetchone()
        serialized = json.dumps(cmd, sort_keys=True)
        if old and cmd["sequence"] <= old[0]:
            if cmd["sequence"] == old[0] and old[1] == serialized:
                return {"status": "ok", "result": json.loads(old[2])}
            return {"status": "ok", "result": {"value": None, "swapped": None, "error": "identity_conflict"}}
        row = db.execute("SELECT v FROM kv WHERE k=?", (cmd["key"],)).fetchone()
        value = row[0] if row else None
        swapped = None
        if cmd["kind"] == "put":
            value = cmd["value"]
        elif cmd["kind"] == "cas":
            swapped = value == cmd.get("expected")
            if swapped:
                value = cmd["value"]
        if value is not None:
            db.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (cmd["key"], value))
        outcome = {"value": value, "swapped": swapped, "error": None}
        db.execute("INSERT OR REPLACE INTO sessions VALUES (?,?,?,?)", (cmd["client"], cmd["sequence"], serialized, json.dumps(outcome)))
        return {"status": "ok", "result": outcome}

class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            header = self.rfile.read(4)
            if len(header) != 4:
                return
            size, = struct.unpack(">I", header)
            if size > 8 * 1024 * 1024:
                return
            data = self.rfile.read(size)
            if len(data) != size:
                return
            result = json.dumps(dispatch(json.loads(data)), separators=(",", ":")).encode()
            try:
                self.wfile.write(struct.pack(">I", len(result)) + result)
                self.wfile.flush()
            except OSError:
                return
class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
host, port = c["client"].rsplit(":", 1)
with Server((host, int(port)), Handler) as server:
    server.serve_forever()
