"""
main.py - FastAPI backend for the Graph Query System.
"""
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import tempfile
import shutil

from db import init_db, get_graph_data, get_node_neighbors, run_query, get_conn
from llm import process_query

app = FastAPI(title="Graph Query System", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    print("Initializing database...")
    init_db()
    print("Ready.")


class ChatMessage(BaseModel):
    query: str
    history: Optional[list] = []

class SQLRequest(BaseModel):
    sql: str


@app.post("/api/upload-dataset")
async def upload_dataset(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.xlsx', '.xls', '.zip', '.csv']:
        raise HTTPException(status_code=400, detail="Use .xlsx, .zip, or .csv")
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    try:
        script = os.path.join(os.path.dirname(__file__), "../scripts/ingest.py")
        ret = os.system(f"python3 {script} {tmp_path}")
        if ret != 0:
            raise Exception("Ingest script failed")
        return {"status": "ok", "message": f"'{file.filename}' loaded successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)


@app.get("/api/graph")
async def get_graph(limit: int = 25):
    return get_graph_data(limit_orders=limit)


@app.get("/api/graph/node/{node_id}")
async def get_node(node_id: str):
    node = get_node_neighbors(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.get("/api/graph/expand/{node_id}")
async def expand_node(node_id: str):
    conn = get_conn()
    nodes, edges, seen = [], [], set()

    def add_node(nid, label, ntype, props):
        if nid not in seen:
            seen.add(nid)
            nodes.append({"id": nid, "label": label, "type": ntype, "properties": props})

    def add_edge(src, tgt, rel):
        edges.append({"source": src, "target": tgt, "label": rel})

    if node_id.startswith("C"):
        for r in conn.execute("SELECT * FROM sales_orders WHERE customer_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["sales_order_id"], r["sales_order_id"], "SalesOrder", r)
            add_edge(node_id, r["sales_order_id"], "PLACED")
    elif node_id.startswith("SO"):
        for r in conn.execute("SELECT soi.*, p.description FROM sales_order_items soi JOIN products p ON soi.material_id=p.material_id WHERE sales_order_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["item_id"], f"Item {r['line_number']}", "OrderItem", r)
            add_edge(node_id, r["item_id"], "HAS_ITEM")
        for r in conn.execute("SELECT * FROM deliveries WHERE sales_order_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["delivery_id"], r["delivery_id"], "Delivery", r)
            add_edge(node_id, r["delivery_id"], "DELIVERED_VIA")
    elif node_id.startswith("DEL"):
        for r in conn.execute("SELECT * FROM invoices WHERE delivery_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["invoice_id"], r["invoice_id"], "Invoice", r)
            add_edge(node_id, r["invoice_id"], "BILLED_AS")
    elif node_id.startswith("INV"):
        for r in conn.execute("SELECT * FROM payments WHERE invoice_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["payment_id"], r["payment_id"], "Payment", r)
            add_edge(node_id, r["payment_id"], "SETTLED_BY")
        for r in conn.execute("SELECT * FROM journal_entries WHERE invoice_id=?", (node_id,)).fetchall():
            r = dict(r)
            add_node(r["journal_entry_id"], r["journal_entry_id"], "JournalEntry", r)
            add_edge(node_id, r["journal_entry_id"], "RECORDED_AS")

    conn.close()
    return {"nodes": nodes, "edges": edges}


@app.get("/api/stats")
async def get_stats():
    queries = {
        "total_orders": "SELECT COUNT(*) as n FROM sales_orders",
        "total_deliveries": "SELECT COUNT(*) as n FROM deliveries",
        "total_invoices": "SELECT COUNT(*) as n FROM invoices",
        "total_payments": "SELECT COUNT(*) as n FROM payments",
        "total_customers": "SELECT COUNT(*) as n FROM customers",
        "total_revenue": "SELECT ROUND(SUM(total_amount),2) as n FROM invoices",
        "unpaid_invoices": "SELECT COUNT(*) as n FROM invoices WHERE status IN ('Unpaid','Overdue')",
        "broken_flows": "SELECT COUNT(*) as n FROM sales_orders WHERE sales_order_id NOT IN (SELECT sales_order_id FROM deliveries)",
    }
    stats = {}
    for key, sql in queries.items():
        rows, _ = run_query(sql)
        stats[key] = rows[0]["n"] if rows else 0
    return stats


@app.post("/api/chat")
async def chat(msg: ChatMessage):
    return await process_query(msg.query)


@app.post("/api/sql")
async def execute_sql(req: SQLRequest):
    results, error = run_query(req.sql)
    if error:
        raise HTTPException(status_code=400, detail=error)
    return {"results": results}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
