"""
db.py - Initializes SQLite database from CSV files and provides graph/query helpers.
"""
import sqlite3
import pandas as pd
import os
import json

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/graph.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "../data")


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Load all CSVs into SQLite tables."""
    conn = get_conn()
    csv_tables = [
        ("customers", "customers.csv"),
        ("products", "products.csv"),
        ("plants", "plants.csv"),
        ("addresses", "addresses.csv"),
        ("sales_orders", "sales_orders.csv"),
        ("sales_order_items", "sales_order_items.csv"),
        ("deliveries", "deliveries.csv"),
        ("invoices", "invoices.csv"),
        ("payments", "payments.csv"),
        ("journal_entries", "journal_entries.csv"),
    ]
    for table, fname in csv_tables:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df.to_sql(table, conn, if_exists="replace", index=False)
            print(f"  ✓ Loaded {table}: {len(df)} rows")
    conn.commit()
    conn.close()
    print("DB initialized.")


def get_schema_description():
    """Returns a text description of the schema for LLM prompts."""
    return """
DATABASE SCHEMA (SQLite):

Tables and columns:
- customers(customer_id, name, city, country, region)
- products(material_id, description, category, unit, price)
- plants(plant_id, name, city, country)
- addresses(address_id, street, city, state, zip, country)
- sales_orders(sales_order_id, customer_id, order_date, status, currency, payment_terms)
- sales_order_items(item_id, sales_order_id, material_id, quantity, unit_price, net_value, line_number)
- deliveries(delivery_id, sales_order_id, plant_id, delivery_date, address_id, status, carrier, tracking_number)
- invoices(invoice_id, delivery_id, sales_order_id, invoice_date, due_date, net_amount, tax_amount, total_amount, currency, status)
- payments(payment_id, invoice_id, payment_date, amount, method, reference, status)
- journal_entries(journal_entry_id, invoice_id, posting_date, description, debit_account, credit_account, amount, currency)

KEY RELATIONSHIPS:
- sales_orders.customer_id → customers.customer_id
- sales_order_items.sales_order_id → sales_orders.sales_order_id
- sales_order_items.material_id → products.material_id
- deliveries.sales_order_id → sales_orders.sales_order_id
- deliveries.plant_id → plants.plant_id
- deliveries.address_id → addresses.address_id
- invoices.delivery_id → deliveries.delivery_id
- invoices.sales_order_id → sales_orders.sales_order_id
- payments.invoice_id → invoices.invoice_id
- journal_entries.invoice_id → invoices.invoice_id

BUSINESS FLOW: Sales Order → Delivery → Invoice (Billing Document) → Payment → Journal Entry
"""


def run_query(sql: str):
    """Execute a SQL query and return results as list of dicts."""
    conn = get_conn()
    try:
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return rows, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()


def get_graph_data(limit_orders=30):
    """Build graph nodes and edges from the database."""
    conn = get_conn()
    nodes = []
    edges = []
    seen_nodes = set()

    def add_node(nid, label, node_type, props=None):
        if nid not in seen_nodes:
            seen_nodes.add(nid)
            nodes.append({
                "id": nid,
                "label": label,
                "type": node_type,
                "properties": props or {}
            })

    def add_edge(source, target, rel):
        edges.append({"source": source, "target": target, "label": rel})

    # Customers
    for row in conn.execute("SELECT * FROM customers").fetchall():
        r = dict(row)
        add_node(r["customer_id"], r["name"], "Customer", r)

    # Products (top 12)
    for row in conn.execute("SELECT * FROM products LIMIT 12").fetchall():
        r = dict(row)
        add_node(r["material_id"], r["description"][:20], "Product", r)

    # Plants
    for row in conn.execute("SELECT * FROM plants").fetchall():
        r = dict(row)
        add_node(r["plant_id"], r["name"], "Plant", r)

    # Sales Orders (limited)
    for row in conn.execute(f"SELECT * FROM sales_orders LIMIT {limit_orders}").fetchall():
        r = dict(row)
        add_node(r["sales_order_id"], r["sales_order_id"], "SalesOrder", r)
        add_edge(r["customer_id"], r["sales_order_id"], "PLACED")

    # Sales Order Items
    for row in conn.execute(
        f"SELECT soi.* FROM sales_order_items soi "
        f"JOIN sales_orders so ON soi.sales_order_id = so.sales_order_id LIMIT {limit_orders*4}"
    ).fetchall():
        r = dict(row)
        add_node(r["item_id"], f"Item {r['line_number']}", "OrderItem", r)
        add_edge(r["sales_order_id"], r["item_id"], "HAS_ITEM")
        add_edge(r["item_id"], r["material_id"], "IS_PRODUCT")

    # Deliveries
    for row in conn.execute(
        f"SELECT d.* FROM deliveries d JOIN sales_orders so ON d.sales_order_id = so.sales_order_id LIMIT {limit_orders}"
    ).fetchall():
        r = dict(row)
        add_node(r["delivery_id"], r["delivery_id"], "Delivery", r)
        add_edge(r["sales_order_id"], r["delivery_id"], "DELIVERED_VIA")
        if r["plant_id"] in seen_nodes:
            add_edge(r["delivery_id"], r["plant_id"], "FROM_PLANT")

    # Invoices
    for row in conn.execute(
        f"SELECT i.* FROM invoices i JOIN deliveries d ON i.delivery_id = d.delivery_id "
        f"JOIN sales_orders so ON so.sales_order_id = d.sales_order_id LIMIT {limit_orders}"
    ).fetchall():
        r = dict(row)
        add_node(r["invoice_id"], r["invoice_id"], "Invoice", r)
        add_edge(r["delivery_id"], r["invoice_id"], "BILLED_AS")

    # Payments
    for row in conn.execute(
        f"SELECT p.*, i.delivery_id FROM payments p JOIN invoices i ON p.invoice_id = i.invoice_id LIMIT {limit_orders}"
    ).fetchall():
        r = dict(row)
        add_node(r["payment_id"], r["payment_id"], "Payment", r)
        add_edge(r["invoice_id"], r["payment_id"], "SETTLED_BY")

    # Journal Entries
    for row in conn.execute(
        f"SELECT je.* FROM journal_entries je LIMIT {limit_orders}"
    ).fetchall():
        r = dict(row)
        add_node(r["journal_entry_id"], r["journal_entry_id"], "JournalEntry", r)
        add_edge(r["invoice_id"], r["journal_entry_id"], "RECORDED_AS")

    conn.close()
    return {"nodes": nodes, "edges": edges}


def get_node_neighbors(node_id: str):
    """Get a specific node and all its direct neighbors."""
    conn = get_conn()
    
    # Determine node type
    node_type = None
    node_data = {}
    
    for table, id_col, ntype in [
        ("customers", "customer_id", "Customer"),
        ("products", "material_id", "Product"),
        ("plants", "plant_id", "Plant"),
        ("sales_orders", "sales_order_id", "SalesOrder"),
        ("deliveries", "delivery_id", "Delivery"),
        ("invoices", "invoice_id", "Invoice"),
        ("payments", "payment_id", "Payment"),
        ("journal_entries", "journal_entry_id", "JournalEntry"),
    ]:
        row = conn.execute(f"SELECT * FROM {table} WHERE {id_col} = ?", (node_id,)).fetchone()
        if row:
            node_type = ntype
            node_data = dict(row)
            break
    
    conn.close()
    if not node_type:
        return None
    return {"id": node_id, "type": node_type, "properties": node_data}


if __name__ == "__main__":
    init_db()
