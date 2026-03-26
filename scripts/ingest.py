"""
ingest.py - Load the real SAP dataset into SQLite.

Your data structure:
  data/
    sales_order_headers/        ← each folder contains JSON file(s)
    sales_order_items/
    outbound_delivery_headers/
    ... etc

Usage:
  python3 scripts/ingest.py path/to/data/folder
  python3 scripts/ingest.py .        (if you run from project root, looks in ./data)
"""

import sys, os, json, glob
import pandas as pd
import sqlite3

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data")
DB_PATH  = os.path.join(DATA_DIR, "graph.db")
os.makedirs(DATA_DIR, exist_ok=True)

# Exact folder names → clean SQLite table names
FOLDER_TO_TABLE = {
    "billing_document_cancellations":          "billing_cancellations",
    "billing_document_headers":                "billing_headers",
    "billing_document_items":                  "billing_items",
    "business_partners":                       "business_partners",
    "business_partner_addresses":              "business_partner_addresses",
    "customer_company_assignments":            "customer_company_assignments",
    "customer_sales_area_assignments":         "customer_sales_area_assignments",
    "journal_entry_items_accounts_receivable": "journal_entries",
    "journal_entry_items_accounts_recei":      "journal_entries",   # truncated name
    "outbound_delivery_headers":               "delivery_headers",
    "outbound_delivery_items":                 "delivery_items",
    "payments_accounts_receivable":            "payments",
    "plants":                                  "plants",
    "product_descriptions":                    "product_descriptions",
    "product_plants":                          "product_plants",
    "product_storage_locations":               "product_storage_locations",
    "products":                                "products",
    "sales_order_headers":                     "sales_order_headers",
    "sales_order_items":                       "sales_order_items",
    "sales_order_schedule_lines":              "sales_order_schedule_lines",
}


def load_json_file(path):
    """Load a JSON file, handling array or object with 'results' or 'value' key."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return pd.DataFrame(raw)
    if isinstance(raw, dict):
        # OData: { "value": [...] } or { "d": { "results": [...] } }
        for key in ("value", "results"):
            if key in raw and isinstance(raw[key], list):
                return pd.DataFrame(raw[key])
        if "d" in raw:
            inner = raw["d"]
            if isinstance(inner, list):
                return pd.DataFrame(inner)
            if isinstance(inner, dict) and "results" in inner:
                return pd.DataFrame(inner["results"])
        # Single record wrapped in object
        return pd.DataFrame([raw])
    return pd.DataFrame()


def ingest_folder(root_path, conn):
    """Walk each subfolder, load all JSON files, insert into SQLite."""
    loaded = []

    for folder_name in sorted(os.listdir(root_path)):
        folder_path = os.path.join(root_path, folder_name)
        if not os.path.isdir(folder_path):
            continue

        # Match folder name to table (try exact, then prefix match)
        table = FOLDER_TO_TABLE.get(folder_name)
        if not table:
            # Try prefix match for truncated names
            for k, v in FOLDER_TO_TABLE.items():
                if folder_name.startswith(k[:20]):
                    table = v
                    break
        if not table:
            table = folder_name  # fallback: use folder name as-is

        # Find all JSON files inside this folder
        json_files = glob.glob(os.path.join(folder_path, "*.json"))
        if not json_files:
            print(f"  ⚠  {folder_name}/: no JSON files found")
            continue

        # Combine all JSON files in folder into one table
        frames = []
        for jf in sorted(json_files):
            try:
                df = load_json_file(jf)
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                print(f"  ⚠  {jf}: {e}")

        if not frames:
            print(f"  ⚠  {folder_name}/: all files empty")
            continue

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.dropna(how="all")

        # Write to SQLite
        combined.to_sql(table, conn, if_exists="replace", index=False)
        print(f"  ✓  {folder_name:<45} → {table:<35} ({len(combined)} rows)")
        loaded.append(table)

    return loaded


def show_schema(conn):
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"\n📊 Database: {len(tables)} tables")
    for (t,) in tables:
        n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()]
        print(f"   {t:<35} {n:>6} rows   sample cols: {cols[:5]}")


if __name__ == "__main__":
    # Default: look for subfolders inside ./data
    root = sys.argv[1] if len(sys.argv) > 1 else DATA_DIR

    # If user passed the project root instead of data folder, try data/ inside it
    if not any(os.path.isdir(os.path.join(root, d)) for d in FOLDER_TO_TABLE):
        candidate = os.path.join(root, "data")
        if os.path.isdir(candidate):
            root = candidate

    print(f"\n🔄  Reading from: {os.path.abspath(root)}\n")
    conn = sqlite3.connect(DB_PATH)
    loaded = ingest_folder(root, conn)
    conn.commit()
    show_schema(conn)
    conn.close()
    print(f"\n✅  Done! {len(loaded)} tables saved to: {DB_PATH}\n")
    print("   Now restart the backend and refresh the browser.\n")