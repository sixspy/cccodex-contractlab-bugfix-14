"""假绿修复回归测：对齐标记必须来自同源快照。

顺序要求：先复现“缓冲行与批次缓冲明细不一致、仅计数相等却标齐”的假绿，
再验证快照写入层的拒绝标齐 / 同源重算 / 并发争用记录。
"""
import threading

import pytest
from fastapi.testclient import TestClient

import app as app_mod
from app import app, connect, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    if hasattr(app_mod, "SEAL_LOCK"):
        monkeypatch.setattr(app_mod, "SEAL_LOCK", threading.Lock())
    init_db()
    yield


def _login(client):
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def _insert_batch(conn, result, detail="测试批次"):
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) "
        "VALUES (?,?,?,?,?,?)",
        ("订单入站", "order_id", "v3", result, detail, "2026-11-18T00:00:00"),
    )
    return cur.lastrowid


def test_equal_counts_with_mismatched_buffer_detail_is_not_aligned():
    """一条“缓冲”批次没有缓冲行，另一条“通过”批次却被挂了缓冲行。

    旧实现只比 buffered(1) == buffer_rows(1)，会标成对齐（假绿）。
    """
    with connect() as conn:
        buffered_batch = _insert_batch(conn, "缓冲", "缺字段进双写缓冲")
        passed_batch = _insert_batch(conn, "通过", "字段齐全")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (passed_batch, "错挂在通过批次上"))

    client = TestClient(app)
    h = _login(client)
    body = client.get("/api/reconcile", headers=h).json()

    assert body["buffered"] == 1 and body["buffer_rows"] == 1  # 计数相等，正是假绿条件
    assert body["buffer_rows_match"] is True
    assert body["detail_match"] is False
    assert body["aligned"] is False
    assert body["aligned_source"] == "none"
    assert body["snapshot"] is None

    seal = client.post("/api/reconcile/seal", headers=h, json={}).json()
    assert seal["aligned"] is False
    assert seal["status"] == "拒绝标齐"
    assert seal["snapshot"] is None  # 拒绝时不产生任何对齐快照
    assert seal["candidate"]["detail_match"] is False
    assert "缓冲" in seal["note"]

    again = client.get("/api/reconcile", headers=h).json()
    assert again["aligned"] is False
    assert again["status"] == "上次拒绝标齐"


def test_consistent_ledger_seals_snapshot_and_display_reads_it():
    with connect() as conn:
        buffered_batch = _insert_batch(conn, "缓冲", "缺字段进双写缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (buffered_batch, "缺字段进双写缓冲"))
        _insert_batch(conn, "通过")
        _insert_batch(conn, "拒收")

    client = TestClient(app)
    h = _login(client)

    seal = client.post("/api/reconcile/seal", headers=h, json={"path": "甲"}).json()
    assert seal["aligned"] is True and seal["status"] == "已对齐"
    snap = seal["snapshot"]
    assert (snap["passed"], snap["buffered"], snap["rejected"], snap["buffer_rows"]) == (1, 1, 1, 1)
    assert snap["fix_path"] == "甲"

    shown = client.get("/api/reconcile", headers=h).json()
    assert shown["aligned"] is True
    assert shown["aligned_source"] == "snapshot"
    assert shown["status"] == "已对齐"
    assert shown["snapshot"]["id"] == snap["id"]

    # 绕过接口直接改库也必须让旧快照失效，否则又是假绿
    with connect() as conn:
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (1, "脏数据"))
    stale = client.get("/api/reconcile", headers=h).json()
    assert stale["aligned"] is False
    assert stale["status"] == "快照与账册不符"


def test_ingress_results_are_never_changed_by_reconcile():
    with connect() as conn:
        conn.execute(
            "INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
            ("v9-compat", "订单入站", "兼容", "正式", "启用", "order_id,amount"),
        )

    client = TestClient(app)
    h = _login(client)
    client.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id"})
    client.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id,amount"})

    with connect() as conn:
        before = [tuple(r) for r in conn.execute(
            "SELECT id,subject,payload_fields,result,detail FROM ingress_batches ORDER BY id").fetchall()]

    client.get("/api/reconcile", headers=h)
    client.post("/api/reconcile/seal", headers=h, json={})
    client.get("/api/reconcile", headers=h)

    with connect() as conn:
        after = [tuple(r) for r in conn.execute(
            "SELECT id,subject,payload_fields,result,detail FROM ingress_batches ORDER BY id").fetchall()]
    assert before == after
    assert [r[3] for r in after] == ["缓冲", "通过"]


def test_any_ingress_write_invalidates_sealed_alignment():
    with connect() as conn:
        b = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (b, "缺字段进双写缓冲"))
    client = TestClient(app)
    h = _login(client)
    assert client.post("/api/reconcile/seal", headers=h, json={}).json()["aligned"] is True

    client.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})

    body = client.get("/api/reconcile", headers=h).json()
    assert body["aligned"] is False
    assert body["status"] == "快照已失效"


def test_concurrent_seal_queues_and_later_one_only_records_contention(monkeypatch):
    with connect() as conn:
        b = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (b, "缺字段进双写缓冲"))
        _insert_batch(conn, "通过")

    entered = threading.Event()
    release = threading.Event()
    original = app_mod._snapshot_counts
    calls = {"n": 0}

    def slow_counts(conn):
        data = original(conn)
        calls["n"] += 1
        if calls["n"] == 1:
            entered.set()
            release.wait(5)
        return data

    monkeypatch.setattr(app_mod, "_snapshot_counts", slow_counts)

    results = {}

    def worker(name):
        client = TestClient(app)
        h = _login(client)
        r = client.post("/api/reconcile/seal", headers=h, json={"path": "甲"})
        results[name] = r.json()

    t1 = threading.Thread(target=worker, args=("first",))
    t1.start()
    assert entered.wait(5)
    t2 = threading.Thread(target=worker, args=("second",))
    t2.start()
    threading.Event().wait(0.2)  # 让后到者先卡在排队上
    release.set()
    t1.join(5)
    t2.join(5)

    assert results["first"]["contended"] is False
    assert results["first"]["aligned"] is True
    assert results["second"]["contended"] is True
    assert results["second"]["aligned"] is True  # 返回既有快照，不重复封存
    assert results["second"]["snapshot"]["id"] == results["first"]["snapshot"]["id"]

    with connect() as conn:
        snapshots = conn.execute("SELECT COUNT(*) c FROM reconcile_snapshots").fetchone()["c"]
        events = [r["kind"] for r in conn.execute("SELECT kind FROM reconcile_events ORDER BY id").fetchall()]
    assert snapshots == 1
    assert "假绿争用" in events


def test_deviating_from_fix_note_is_rejected_and_logged():
    client = TestClient(app)
    h = _login(client)
    # 首次标齐选定甲径并落入当天修复说明
    first = client.post("/api/reconcile/seal", headers=h, json={"path": "甲"}).json()
    assert first["path"] == "甲"

    r = client.post("/api/reconcile/seal", headers=h, json={"path": "乙"})
    assert r.status_code == 400
    assert "甲" in r.json()["detail"]

    with connect() as conn:
        kinds = [row["kind"] for row in conn.execute("SELECT kind FROM reconcile_events ORDER BY id")]
        note = conn.execute("SELECT path FROM fix_notes WHERE day=?", (app_mod.today(),)).fetchone()
    assert "假绿偏了" in kinds
    assert note["path"] == "甲"
    assert client.get("/api/reconcile", headers=h).json()["fix_note"]["path"] == "甲"


def test_draft_path_beta_only_publishes_after_recheck():
    """乙径：先落草稿，同一事务内复核一致后才挂正式标记；不一致则保留草稿。"""
    with connect() as conn:
        buffered_batch = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (buffered_batch, "缺字段进双写缓冲"))

    client = TestClient(app)
    h = _login(client)
    seal = client.post("/api/reconcile/seal", headers=h, json={"path": "乙"}).json()
    assert seal["aligned"] is True and seal["path"] == "乙"
    assert seal["snapshot"]["status"] == "已对齐"
    assert seal["snapshot"]["fix_path"] == "乙"

    with connect() as conn:
        rows = conn.execute("SELECT status, aligned FROM reconcile_snapshots ORDER BY id").fetchall()
    assert [(r["status"], r["aligned"]) for r in rows] == [("已对齐", 1)]


def test_consistent_ledger_without_seal_is_still_not_aligned():
    """计数一致不等于对齐：没有同源快照时页面必须显示未对齐。"""
    with connect() as conn:
        b = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (b, "缺字段进双写缓冲"))
    client = TestClient(app)
    h = _login(client)
    body = client.get("/api/reconcile", headers=h).json()
    assert body["counts_match"] is True and body["detail_match"] is True
    assert body["aligned"] is False
    assert body["aligned_source"] == "none"
    assert body["status"] == "未标齐"


def test_ledger_lists_orphan_buffer_rows():
    with connect() as conn:
        passed_batch = _insert_batch(conn, "通过")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (passed_batch, "错挂在通过批次上"))
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (9999, "批次不存在"))
    client = TestClient(app)
    h = _login(client)
    ledger = client.get("/api/reconcile/ledger", headers=h).json()
    ids = sorted(r["batch_id"] for r in ledger["orphan_buffers"])
    assert ids == [passed_batch, 9999]
    assert ledger["fix_note"] is None  # 尚未选定修复路径


def test_unknown_fix_path_is_rejected_and_logged():
    client = TestClient(app)
    h = _login(client)
    r = client.post("/api/reconcile/seal", headers=h, json={"path": "丙"})
    assert r.status_code == 400
    with connect() as conn:
        kinds = [row["kind"] for row in conn.execute("SELECT kind FROM reconcile_events ORDER BY id")]
        assert conn.execute("SELECT COUNT(*) c FROM fix_notes").fetchone()["c"] == 0
    assert "假绿偏了" in kinds


def test_contention_is_logged_even_when_later_arrival_is_rejected(monkeypatch):
    """后到值班即便遇到不一致被拒，也要留“假绿争用”，且不覆盖拒绝结论。"""
    with connect() as conn:
        buffered_batch = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (buffered_batch, "缺字段进双写缓冲"))

    entered = threading.Event()
    release = threading.Event()
    original = app_mod._snapshot_counts
    calls = {"n": 0}

    def flaky_counts(conn):
        data = original(conn)
        calls["n"] += 1
        if calls["n"] == 1:
            entered.set()
            release.wait(5)
            return data
        # 后到者重算时账册已出现明细不一致（模拟先到者封存后有人错挂缓冲行）
        return {**data, "detail_match": False, "counts_match": False}

    monkeypatch.setattr(app_mod, "_snapshot_counts", flaky_counts)
    h = _login(TestClient(app))  # 先登录，保证后到者立刻撞上标齐闸门
    results = {}

    def worker(name):
        client = TestClient(app)
        results[name] = client.post("/api/reconcile/seal", headers=h, json={"path": "甲"}).json()

    t1 = threading.Thread(target=worker, args=("first",))
    t1.start()
    assert entered.wait(5)
    t2 = threading.Thread(target=worker, args=("second",))
    t2.start()
    threading.Event().wait(0.3)  # 让后到者先卡在排队上
    release.set()
    t1.join(5)
    t2.join(5)

    with connect() as conn:
        kinds = [row["kind"] for row in conn.execute("SELECT kind FROM reconcile_events ORDER BY id")]
        snapshots = conn.execute("SELECT COUNT(*) c FROM reconcile_snapshots").fetchone()["c"]
    assert "假绿争用" in kinds
    assert "拒绝标齐" in kinds
    assert snapshots == 1  # 只有先到者封存的快照
    assert results["second"]["status"] == "拒绝标齐"
    assert results["second"]["snapshot"] is None
    assert results["second"]["contended"] is True


def test_cross_connection_write_lock_is_recorded_as_contention():
    """另一连接持写锁时，标齐要排队等锁后记录争用，而不是静默标齐。"""
    import sqlite3

    with connect() as conn:
        b = _insert_batch(conn, "缓冲")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (b, "缺字段进双写缓冲"))

    blocker = sqlite3.connect(str(app_mod.DB_PATH), timeout=5, check_same_thread=False)
    blocker.execute("BEGIN IMMEDIATE")
    timer = threading.Timer(0.4, lambda: (blocker.rollback(), blocker.close()))
    timer.start()
    try:
        client = TestClient(app)
        h = _login(client)
        r = client.post("/api/reconcile/seal", headers=h, json={"path": "甲"}).json()
    finally:
        timer.join(5)
    assert r["contended"] is True
    assert r["aligned"] is True
    with connect() as conn:
        kinds = [row["kind"] for row in conn.execute("SELECT kind FROM reconcile_events ORDER BY id")]
    assert kinds.count("假绿争用") == 1
