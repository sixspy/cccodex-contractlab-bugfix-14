
import hashlib, json, secrets, sqlite3, threading
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 对账标齐串行闸门：两名值班同时抢点时后到者排队，只记争用，不重复封存。
SEAL_LOCK = threading.Lock()
SEAL_WAIT_SECONDS = 10

FIX_NOTE = ("假绿修复说明：对齐标记只认同源快照，页面不得本地聚合冒充对齐。"
            "甲径=写对齐标记前在同一事务内重算四类计数，不一致即拒绝标齐；"
            "乙径=先落对账草稿，核对一致后再原子挂正式对齐标记。"
            "路径当天选定后不许更换；禁止修改入站业务结果。")

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=SEAL_WAIT_SECONDS)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def today() -> str:
    return datetime.now().date().isoformat()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reconcile_snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          day TEXT NOT NULL,
          path TEXT NOT NULL,
          status TEXT NOT NULL,
          aligned INTEGER NOT NULL,
          ingress_total INTEGER NOT NULL,
          passed INTEGER NOT NULL,
          buffered INTEGER NOT NULL,
          rejected INTEGER NOT NULL,
          buffer_rows INTEGER NOT NULL,
          detail_match INTEGER NOT NULL,
          fingerprint TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reconcile_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fix_notes(
          day TEXT PRIMARY KEY,
          path TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        now = datetime.now().isoformat(timespec="seconds")
        if missing:
            if ctr["mode"] == "兼容":
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b LEFT JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# 对账快照写入层
#
# 页面只读 /api/reconcile 的快照结论，不在前端聚合计数；对齐标记只能由
# 本层在单个事务内重算四类计数（通过/缓冲/拒收/缓冲行）并逐笔核对缓冲明细后写入。
# ---------------------------------------------------------------------------

def _snapshot_counts(conn) -> dict:
    """在同一事务内重算四类计数，并逐笔比对缓冲明细。"""
    total = conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
    passed = conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
    buffered = conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
    rejected = conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
    buffer_rows = conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    buffer_batch_ids = sorted(r["batch_id"] for r in conn.execute("SELECT batch_id FROM buffers").fetchall())
    buffered_batch_ids = sorted(r["id"] for r in conn.execute("SELECT id FROM ingress_batches WHERE result='缓冲'").fetchall())
    detail_match = buffer_batch_ids == buffered_batch_ids
    fingerprint = hashlib.sha256(json.dumps({
        "total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
        "buffer_rows": buffer_rows, "buffer_batch_ids": buffer_batch_ids,
        "buffered_batch_ids": buffered_batch_ids,
    }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "ingress_total": total, "passed": passed, "buffered": buffered,
        "rejected": rejected, "buffer_rows": buffer_rows,
        "buffer_rows_match": buffered == buffer_rows,
        "detail_match": detail_match,
        "counts_match": total == passed + buffered + rejected and buffered == buffer_rows,
        "fingerprint": fingerprint,
    }

def _consistent(counts: dict) -> bool:
    return counts["counts_match"] and counts["detail_match"]

def _consistency_note(counts: dict) -> str:
    if counts["detail_match"]:
        return "四类计数不一致：缓冲 %d 笔 / 缓冲行 %d 笔 / 入站合计 %d" % (
            counts["buffered"], counts["buffer_rows"], counts["ingress_total"])
    return "缓冲明细不一致：缓冲批次与缓冲行未逐笔对应，拒绝标齐"

def _log(conn, kind: str, detail: str):
    conn.execute("INSERT INTO reconcile_events(kind,detail,created_at) VALUES (?,?,?)", (kind, detail, now_iso()))

def _fix_note(conn):
    return conn.execute("SELECT * FROM fix_notes WHERE day=?", (today(),)).fetchone()

def _resolve_path(conn, requested: str) -> sqlite3.Row:
    """当天路径一经落说明不许更换；偏离即拒绝并记“假绿偏了”。"""
    requested = (requested or "").strip() or "甲"
    if requested not in ("甲", "乙"):
        _log(conn, "假绿偏了", "请求路径 %s 不在假绿修复说明的甲/乙两径内" % requested)
        conn.commit()
        raise HTTPException(400, "假绿修复说明只允许甲径或乙径（请求 %s）" % requested)
    note = _fix_note(conn)
    if note is None:
        conn.execute("INSERT OR IGNORE INTO fix_notes(day,path,note,created_at) VALUES (?,?,?,?)",
                     (today(), requested, FIX_NOTE, now_iso()))
        conn.commit()  # 选定路径自己独立落库，随后的标齐事务再另起
        note = _fix_note(conn)
    if requested != note["path"]:
        _log(conn, "假绿偏了", "请求路径 %s 偏离假绿修复说明锁定路径 %s" % (requested, note["path"]))
        conn.commit()
        raise HTTPException(400, "假绿修复说明锁定路径 %s，当天不许换（请求 %s）" % (note["path"], requested))
    return note

def _snapshot_dict(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    d["aligned"] = bool(d["aligned"])
    d["detail_match"] = bool(d["detail_match"])
    d["fix_path"] = d["path"]
    return d

def _latest_aligned_snapshot(conn):
    return conn.execute("SELECT * FROM reconcile_snapshots WHERE aligned=1 ORDER BY id DESC LIMIT 1").fetchone()

def _aligned_view(conn, counts: dict | None = None) -> dict:
    """对外结论一律由同源快照给出，绝不按实时计数现算 aligned。"""
    counts = counts or _snapshot_counts(conn)
    snap = _latest_aligned_snapshot(conn)
    view = {
        **counts,
        "aligned": False,
        "aligned_source": "none",
        "status": "未标齐",
        "snapshot": _snapshot_dict(snap),
        "fix_note": None,
    }
    note = _fix_note(conn)
    if note:
        view["fix_note"] = dict(note)
    if snap is None:
        last = conn.execute(
            "SELECT kind FROM reconcile_events WHERE kind IN ('拒绝标齐','已对齐') ORDER BY id DESC LIMIT 1").fetchone()
        if last and last["kind"] == "拒绝标齐":
            view["status"] = "上次拒绝标齐"
        return view
    view["aligned_source"] = "snapshot"
    if counts["fingerprint"] == snap["fingerprint"] and counts["detail_match"]:
        view["aligned"] = True
        view["status"] = "已对齐"
        return view
    view["status"] = "快照已失效" if counts["detail_match"] else "快照与账册不符"
    return view

class SealIn(BaseModel):
    path: str | None = None

def _probe_write_lock() -> bool:
    """探测库上是否已有别的写事务（另一个进程正在标齐/写入）。"""
    if not DB_PATH.exists():
        return False
    try:
        probe = sqlite3.connect(DB_PATH, timeout=0.05)
    except sqlite3.OperationalError:
        return True
    try:
        probe.execute("BEGIN IMMEDIATE")
        probe.rollback()
        return False
    except sqlite3.OperationalError:
        return True
    finally:
        probe.close()

@app.post("/api/reconcile/seal")
def seal(body: SealIn | None = None, authorization: str | None = Header(default=None)):
    """对账标齐：重算与写标记同一事务；并发抢点排队，后到只记争用。"""
    require_user(authorization)
    requested = body.path if body else None

    # 先探测跨进程写锁，再抢本进程闸门；任一命中都说明是后到者。
    contended = _probe_write_lock()
    if not SEAL_LOCK.acquire(blocking=False):
        contended = True
        if not SEAL_LOCK.acquire(timeout=SEAL_WAIT_SECONDS):
            raise HTTPException(409, "对账标齐排队超时")
    try:
        conn = connect()
        try:
            note = _resolve_path(conn, requested)
            path = note["path"]
            conn.execute("BEGIN IMMEDIATE")
            counts = _snapshot_counts(conn)  # 与写入同一事务
            if contended:
                # 后到值班排队进入：无论结论如何都要留下争用记录
                _log(conn, "假绿争用", "后到值班排队进入，待重算与写标记")

            existing = _latest_aligned_snapshot(conn)
            if existing is not None and existing["fingerprint"] == counts["fingerprint"] and counts["detail_match"]:
                if contended:
                    conn.commit()
                else:
                    conn.rollback()
                return {
                    "aligned": True, "status": "已对齐", "path": path,
                    "snapshot": _snapshot_dict(existing),
                    "note": "沿用已封存的同源快照 #%d" % existing["id"],
                    "contended": contended,
                }

            if not _consistent(counts):
                reason = _consistency_note(counts)
                _log(conn, "拒绝标齐", reason)
                conn.commit()
                return {
                    "aligned": False, "status": "拒绝标齐", "path": path,
                    "snapshot": None,
                    "candidate": {
                        "id": None, "path": path, "fix_path": path,
                        "status": "拒绝标齐", "aligned": False,
                        "ingress_total": counts["ingress_total"], "passed": counts["passed"],
                        "buffered": counts["buffered"], "rejected": counts["rejected"],
                        "buffer_rows": counts["buffer_rows"],
                        "detail_match": counts["detail_match"],
                        "fingerprint": counts["fingerprint"],
                    },
                    "note": reason + "；未写入对齐标记",
                    "contended": contended,
                }

            if path == "乙":
                # 乙径：先落对账草稿，核对一致后在同一事务内原子挂正式标记。
                cur = conn.execute(
                    "INSERT INTO reconcile_snapshots(day,path,status,aligned,ingress_total,passed,buffered,rejected,buffer_rows,detail_match,fingerprint,note,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (today(), path, "草稿", 0, counts["ingress_total"], counts["passed"], counts["buffered"],
                     counts["rejected"], counts["buffer_rows"], int(counts["detail_match"]), counts["fingerprint"],
                     "对账草稿待核对", now_iso()))
                draft_id = cur.lastrowid
                verify = _snapshot_counts(conn)  # 草稿落库后在同一事务内复核
                if verify["fingerprint"] != counts["fingerprint"] or not _consistent(verify):
                    _log(conn, "拒绝标齐", "乙径草稿 #%d 复核不一致，保留草稿不挂正式标记" % draft_id)
                    conn.commit()
                    return {
                        "aligned": False, "status": "拒绝标齐", "path": path,
                        "snapshot": None,
                        "draft": {"id": draft_id, "path": path, "fix_path": path, "status": "草稿",
                                  "aligned": False, "detail_match": verify["detail_match"],
                                  "ingress_total": verify["ingress_total"], "passed": verify["passed"],
                                  "buffered": verify["buffered"], "rejected": verify["rejected"],
                                  "buffer_rows": verify["buffer_rows"],
                                  "fingerprint": verify["fingerprint"]},
                        "note": "草稿核对不一致，保留对账草稿", "contended": contended,
                    }
                conn.execute("UPDATE reconcile_snapshots SET status='已对齐', aligned=1, note=? WHERE id=?",
                             ("乙径草稿核对一致后原子挂正式标记", draft_id))
                snap_id = draft_id
            else:
                # 甲径：同一事务内重算四类计数并直接写正式标记。
                cur = conn.execute(
                    "INSERT INTO reconcile_snapshots(day,path,status,aligned,ingress_total,passed,buffered,rejected,buffer_rows,detail_match,fingerprint,note,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (today(), path, "已对齐", 1, counts["ingress_total"], counts["passed"], counts["buffered"],
                     counts["rejected"], counts["buffer_rows"], int(counts["detail_match"]), counts["fingerprint"],
                     "甲径：同一事务重算四类计数一致后标齐", now_iso()))
                snap_id = cur.lastrowid
            _log(conn, "已对齐", "%s径封存快照 #%d" % (path, snap_id))
            conn.commit()
            snap = conn.execute("SELECT * FROM reconcile_snapshots WHERE id=?", (snap_id,)).fetchone()
            return {
                "aligned": True, "status": "已对齐", "path": path,
                "snapshot": _snapshot_dict(snap),
                "note": "%s径：四类计数与缓冲明细一致，已原子挂正式对齐标记" % path,
                "contended": contended,
            }
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        raise HTTPException(409, "账册写入被占用，未标齐：%s" % exc)
    finally:
        SEAL_LOCK.release()

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        view = _aligned_view(c)
    return view

@app.get("/api/reconcile/ledger")
def reconcile_ledger(authorization: str | None = Header(default=None)):
    """对账账册视图：批次、缓冲明细、修复说明与事件，仅供展示。"""
    require_user(authorization)
    with connect() as c:
        batches = [dict(r) for r in c.execute(
            """SELECT i.id, i.subject, i.result, i.detail, COUNT(b.id) buffer_rows
               FROM ingress_batches i LEFT JOIN buffers b ON b.batch_id=i.id
               GROUP BY i.id ORDER BY i.id DESC""").fetchall()]
        orphan = [dict(r) for r in c.execute(
            """SELECT b.id, b.batch_id, b.reason FROM buffers b
               LEFT JOIN ingress_batches i ON i.id=b.batch_id
               WHERE i.id IS NULL OR i.result<>'缓冲' ORDER BY b.id DESC""").fetchall()]
        events = [dict(r) for r in c.execute(
            "SELECT id,kind,detail,created_at FROM reconcile_events ORDER BY id DESC LIMIT 20").fetchall()]
        note = _fix_note(c)
        snap = _latest_aligned_snapshot(c)
        view = _aligned_view(c)
    return {
        "batches": batches, "orphan_buffers": orphan, "events": events,
        "fix_note": dict(note) if note else None,
        "snapshot": _snapshot_dict(snap),
        "alignment": {
            "aligned": view["aligned"], "status": view["status"],
            "aligned_source": view["aligned_source"],
            "detail_match": view["detail_match"], "counts_match": view["counts_match"],
        },
    }

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
