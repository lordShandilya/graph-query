from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os, tempfile, shutil

from db import init_db, get_graph_data, get_node_neighbors, run_query, get_conn, table_exists, safe_query
from llm import process_query

app = FastAPI(title="GraphQuery — SAP Data Explorer", version="2.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    print("Initializing DB...")
    init_db()


class ChatMessage(BaseModel):
    query: str
    history: Optional[list] = []

class SQLRequest(BaseModel):
    sql: str


# ── Dataset upload ────────────────────────────────────────────────────────────

@app.post("/api/upload-dataset")
async def upload_dataset(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.xlsx', '.xls', '.zip', '.csv', '.json']:
        raise HTTPException(400, "Supported: .json .xlsx .zip .csv")
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    try:
        script = os.path.join(os.path.dirname(__file__), "../scripts/ingest.py")
        ret = os.system(f'python3 "{script}" "{tmp_path}"')
        if ret != 0:
            raise Exception("Ingest failed")
        return {"status": "ok", "message": f"'{file.filename}' loaded."}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        os.unlink(tmp_path)


# ── Graph ─────────────────────────────────────────────────────────────────────

@app.get("/api/graph")
async def get_graph(limit: int = 25):
    return get_graph_data(limit_orders=limit)


@app.get("/api/graph/node/{node_id}")
async def get_node(node_id: str):
    node = get_node_neighbors(node_id)
    if not node:
        raise HTTPException(404, "Node not found")
    return node


@app.get("/api/graph/expand/{node_id}")
async def expand_node(node_id: str):
    conn = get_conn()
    nodes, edges, seen = [], [], set()

    def add_node(nid, label, ntype, props):
        nid = str(nid)
        if nid and nid not in seen:
            seen.add(nid)
            nodes.append({"id": nid, "label": str(label)[:20], "type": ntype, "properties": props})

    def add_edge(src, tgt, rel):
        src, tgt = str(src), str(tgt)
        if src and tgt and src != tgt:
            edges.append({"source": src, "target": tgt, "label": rel})

    # Detect node type by prefix/pattern and expand accordingly
    # Business partner / customer
    if table_exists(conn, "business_partners"):
        row = conn.execute('SELECT * FROM business_partners WHERE BusinessPartner=? LIMIT 1', (node_id,)).fetchone()
        if row:
            for r in safe_query(conn, 'SELECT * FROM sales_order_headers WHERE SoldToParty=? LIMIT 20', (node_id,)):
                so = str(r.get("SalesOrder",""))
                add_node(so, so, "SalesOrder", r)
                add_edge(node_id, so, "PLACED")

    # Sales Order → items + delivery
    if table_exists(conn, "sales_order_items"):
        rows = safe_query(conn, 'SELECT * FROM sales_order_items WHERE SalesOrder=? LIMIT 20', (node_id,))
        for r in rows:
            item = str(r.get("SalesOrderItem",""))
            mat  = str(r.get("Material",""))
            if item:
                nid = f"{node_id}-{item}"
                add_node(nid, f"Item {item}", "OrderItem", r)
                add_edge(node_id, nid, "HAS_ITEM")

    if table_exists(conn, "outbound_delivery_headers"):
        for r in safe_query(conn, 'SELECT * FROM outbound_delivery_headers WHERE SalesOrder=? LIMIT 10', (node_id,)):
            did = str(r.get("DeliveryDocument",""))
            if did:
                add_node(did, did, "Delivery", r)
                add_edge(node_id, did, "DELIVERED_VIA")

    # Delivery → billing
    if table_exists(conn, "billing_headers"):
        for r in safe_query(conn, 'SELECT * FROM billing_headers WHERE DeliveryDocument=? LIMIT 10', (node_id,)):
            bid = str(r.get("BillingDocument",""))
            if bid:
                add_node(bid, bid, "Invoice", r)
                add_edge(node_id, bid, "BILLED_AS")
        # Also try direct SO → billing
        for r in safe_query(conn, 'SELECT * FROM billing_headers WHERE SalesOrder=? LIMIT 10', (node_id,)):
            bid = str(r.get("BillingDocument",""))
            if bid:
                add_node(bid, bid, "Invoice", r)
                add_edge(node_id, bid, "BILLED_AS")

    # Billing → payments + journal
    if table_exists(conn, "payments_accounts_receivable"):
        for r in safe_query(conn, 'SELECT * FROM payments_accounts_receivable WHERE BillingDocument=? LIMIT 10', (node_id,)):
            pid = str(r.get("PaymentDocument") or r.get("Document",""))
            if pid:
                add_node(pid, pid, "Payment", r)
                add_edge(node_id, pid, "SETTLED_BY")

    if table_exists(conn, "journal_entry_items_accounts_receivable"):
        for r in safe_query(conn, 'SELECT * FROM journal_entry_items_accounts_receivable WHERE BillingDocument=? LIMIT 10', (node_id,)):
            jid = str(r.get("AccountingDocument",""))
            if jid:
                add_node(jid, jid, "JournalEntry", r)
                add_edge(node_id, jid, "RECORDED_AS")

    conn.close()
    return {"nodes": nodes, "edges": edges}


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    conn = get_conn()
    stats = {}

    def count(table, where=""):
        if not table_exists(conn, table):
            return 0
        try:
            sql = f'SELECT COUNT(*) FROM "{table}"' + (f" WHERE {where}" if where else "")
            return conn.execute(sql).fetchone()[0]
        except Exception:
            return 0

    def scalar(sql):
        try:
            r = conn.execute(sql).fetchone()
            return r[0] if r else 0
        except Exception:
            return 0

    stats["total_orders"]     = count("sales_order_headers")
    stats["total_deliveries"] = count("outbound_delivery_headers")
    stats["total_invoices"]   = count("billing_headers")
    stats["total_payments"]   = count("payments_accounts_receivable")
    stats["total_customers"]  = count("business_partners")
    stats["total_products"]   = count("products")

    # Revenue
    for col in ("NetAmount", "TotalNetAmount", "net_amount", "total_amount", "Amount"):
        try:
            v = scalar(f'SELECT ROUND(SUM("{col}"), 2) FROM billing_headers')
            if v:
                stats["total_revenue"] = v
                break
        except Exception:
            pass
    if "total_revenue" not in stats:
        stats["total_revenue"] = 0

    # Broken flows: sales orders with no delivery
    if table_exists(conn, "sales_order_headers") and table_exists(conn, "outbound_delivery_headers"):
        try:
            stats["broken_flows"] = scalar(
                'SELECT COUNT(*) FROM sales_order_headers '
                'WHERE SalesOrder NOT IN (SELECT DISTINCT SalesOrder FROM outbound_delivery_headers WHERE SalesOrder IS NOT NULL)'
            )
        except Exception:
            stats["broken_flows"] = 0
    else:
        stats["broken_flows"] = 0

    conn.close()
    return stats


# ── Chat & SQL ────────────────────────────────────────────────────────────────

@app.post("/api/chat")
async def chat(msg: ChatMessage):
    return await process_query(msg.query)


@app.post("/api/sql")
async def execute_sql(req: SQLRequest):
    results, error = run_query(req.sql)
    if error:
        raise HTTPException(400, error)
    return {"results": results}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/tables")
async def list_tables():
    """Debug: list all tables and row counts."""
    conn = get_conn()
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    result = {}
    for (t,) in tables:
        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()]
            result[t] = {"rows": n, "columns": cols}
        except Exception:
            pass
    conn.close()
    return result


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)