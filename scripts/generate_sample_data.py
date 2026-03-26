"""
Generate sample dataset mimicking the real SAP-like business dataset.
Run this if you don't have the real dataset yet.
"""
import csv
import random
from datetime import datetime, timedelta
import os

random.seed(42)

OUT = os.path.join(os.path.dirname(__file__), "../data")
os.makedirs(OUT, exist_ok=True)

# --- Masters ---
customers = [
    {"customer_id": f"C{i:04d}", "name": n, "city": c, "country": co, "region": r}
    for i, (n, c, co, r) in enumerate([
        ("Acme Corp", "Mumbai", "India", "APAC"),
        ("TechVision GmbH", "Berlin", "Germany", "EMEA"),
        ("BlueWave LLC", "New York", "USA", "AMER"),
        ("SunRise Traders", "Dubai", "UAE", "EMEA"),
        ("Pacific Imports", "Tokyo", "Japan", "APAC"),
        ("Nordic Supply AB", "Stockholm", "Sweden", "EMEA"),
        ("Desert Logistics", "Riyadh", "Saudi Arabia", "EMEA"),
        ("Maple Trading Co", "Toronto", "Canada", "AMER"),
        ("Southern Cross Ltd", "Sydney", "Australia", "APAC"),
        ("Andean Partners", "Bogotá", "Colombia", "AMER"),
    ], 1)
]

products = [
    {"material_id": f"M{i:04d}", "description": d, "category": cat, "unit": u, "price": p}
    for i, (d, cat, u, p) in enumerate([
        ("Industrial Pump A100", "Machinery", "EA", 1200.00),
        ("Control Panel CP200", "Electronics", "EA", 850.00),
        ("Steel Pipe 2in x 6ft", "Raw Material", "PC", 45.00),
        ("Hydraulic Valve HV50", "Components", "EA", 320.00),
        ("Electric Motor 5HP", "Machinery", "EA", 780.00),
        ("Bearing Kit BK100", "Components", "SET", 95.00),
        ("PVC Tubing 1in x 10ft", "Raw Material", "PC", 18.00),
        ("Pressure Gauge PG20", "Instruments", "EA", 65.00),
        ("Gasket Set GS300", "Components", "SET", 42.00),
        ("Filter Assembly FA10", "Components", "EA", 135.00),
        ("Compressor Unit CU15", "Machinery", "EA", 2100.00),
        ("Digital Sensor DS5", "Electronics", "EA", 290.00),
    ], 1)
]

plants = [
    {"plant_id": f"P{i:03d}", "name": n, "city": c, "country": co}
    for i, (n, c, co) in enumerate([
        ("Main Warehouse", "Mumbai", "India"),
        ("EU Distribution Hub", "Frankfurt", "Germany"),
        ("North America DC", "Chicago", "USA"),
        ("APAC Fulfillment", "Singapore", "Singapore"),
    ], 1)
]

addresses = [
    {"address_id": f"A{i:04d}", "street": s, "city": c, "state": st, "zip": z, "country": co}
    for i, (s, c, st, z, co) in enumerate([
        ("123 Industrial Ave", "Mumbai", "MH", "400001", "India"),
        ("45 Hauptstrasse", "Berlin", "BE", "10115", "Germany"),
        ("789 Commerce Blvd", "New York", "NY", "10001", "USA"),
        ("22 Sheikh Zayed Rd", "Dubai", "DU", "00000", "UAE"),
        ("1-1 Shinjuku", "Tokyo", "TK", "160-0001", "Japan"),
        ("Kungsgatan 5", "Stockholm", "ST", "11122", "Sweden"),
        ("King Fahd Rd", "Riyadh", "RI", "11564", "Saudi Arabia"),
        ("100 King St W", "Toronto", "ON", "M5X1A9", "Canada"),
        ("1 Martin Pl", "Sydney", "NSW", "2000", "Australia"),
        ("Calle 72 #10-07", "Bogotá", "DC", "110221", "Colombia"),
    ], 1)
]

def rand_date(start_year=2023, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def fmt(d): return d.strftime("%Y-%m-%d")

# --- Sales Orders ---
sales_orders = []
so_items = []
item_id = 1
for i in range(1, 61):
    so_id = f"SO{i:06d}"
    cust = random.choice(customers)
    so_date = rand_date()
    status = random.choice(["Open", "Completed", "In Progress", "Cancelled"])
    sales_orders.append({
        "sales_order_id": so_id,
        "customer_id": cust["customer_id"],
        "order_date": fmt(so_date),
        "status": status,
        "currency": random.choice(["USD", "EUR", "INR", "AED"]),
        "payment_terms": random.choice(["NET30", "NET60", "IMMEDIATE"]),
    })
    num_items = random.randint(1, 4)
    for j in range(1, num_items + 1):
        prod = random.choice(products)
        qty = random.randint(1, 20)
        so_items.append({
            "item_id": f"SOI{item_id:06d}",
            "sales_order_id": so_id,
            "material_id": prod["material_id"],
            "quantity": qty,
            "unit_price": prod["price"],
            "net_value": round(qty * prod["price"], 2),
            "line_number": j,
        })
        item_id += 1

# --- Deliveries ---
deliveries = []
for so in sales_orders[:50]:  # 50 of 60 have deliveries (10 have no delivery = broken flow)
    so_date = datetime.strptime(so["order_date"], "%Y-%m-%d")
    del_date = so_date + timedelta(days=random.randint(3, 15))
    plant = random.choice(plants)
    addr_idx = int(so["customer_id"][1:]) - 1
    deliveries.append({
        "delivery_id": f"DEL{so['sales_order_id'][2:]}",
        "sales_order_id": so["sales_order_id"],
        "plant_id": plant["plant_id"],
        "delivery_date": fmt(del_date),
        "address_id": addresses[addr_idx % len(addresses)]["address_id"],
        "status": random.choice(["Delivered", "In Transit", "Pending"]),
        "carrier": random.choice(["DHL", "FedEx", "UPS", "BlueDart"]),
        "tracking_number": f"TRK{random.randint(100000,999999)}",
    })

# --- Billing / Invoices ---
invoices = []
# Only 42 of the 50 delivered orders get invoiced (8 delivered but not billed = broken)
for deliv in deliveries[:42]:
    so_date = datetime.strptime(
        next(s["order_date"] for s in sales_orders if s["sales_order_id"] == deliv["sales_order_id"]),
        "%Y-%m-%d"
    )
    inv_date = datetime.strptime(deliv["delivery_date"], "%Y-%m-%d") + timedelta(days=random.randint(1, 7))
    items_for_so = [it for it in so_items if it["sales_order_id"] == deliv["sales_order_id"]]
    total = sum(it["net_value"] for it in items_for_so)
    tax = round(total * 0.18, 2)
    invoices.append({
        "invoice_id": f"INV{deliv['delivery_id'][3:]}",
        "delivery_id": deliv["delivery_id"],
        "sales_order_id": deliv["sales_order_id"],
        "invoice_date": fmt(inv_date),
        "due_date": fmt(inv_date + timedelta(days=30)),
        "net_amount": round(total, 2),
        "tax_amount": tax,
        "total_amount": round(total + tax, 2),
        "currency": "USD",
        "status": random.choice(["Paid", "Unpaid", "Overdue", "Partially Paid"]),
    })

# --- Payments ---
payments = []
for inv in invoices:
    if inv["status"] in ("Paid", "Partially Paid"):
        pay_date = datetime.strptime(inv["invoice_date"], "%Y-%m-%d") + timedelta(days=random.randint(1, 25))
        amount = inv["total_amount"] if inv["status"] == "Paid" else round(inv["total_amount"] * random.uniform(0.3, 0.7), 2)
        payments.append({
            "payment_id": f"PAY{inv['invoice_id'][3:]}",
            "invoice_id": inv["invoice_id"],
            "payment_date": fmt(pay_date),
            "amount": amount,
            "method": random.choice(["Wire Transfer", "Check", "Credit Card", "ACH"]),
            "reference": f"REF{random.randint(10000,99999)}",
            "status": "Cleared",
        })

# --- Journal Entries ---
journal_entries = []
for inv in invoices:
    je_date = inv["invoice_date"]
    journal_entries.append({
        "journal_entry_id": f"JE{inv['invoice_id'][3:]}",
        "invoice_id": inv["invoice_id"],
        "posting_date": je_date,
        "description": f"Revenue recognition for {inv['invoice_id']}",
        "debit_account": "1200-Accounts Receivable",
        "credit_account": "4000-Revenue",
        "amount": inv["net_amount"],
        "currency": inv["currency"],
    })

# --- Write CSVs ---
def write_csv(filename, rows):
    if not rows:
        return
    path = os.path.join(OUT, filename)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"✓ {filename}: {len(rows)} rows")

write_csv("customers.csv", customers)
write_csv("products.csv", products)
write_csv("plants.csv", plants)
write_csv("addresses.csv", addresses)
write_csv("sales_orders.csv", sales_orders)
write_csv("sales_order_items.csv", so_items)
write_csv("deliveries.csv", deliveries)
write_csv("invoices.csv", invoices)
write_csv("payments.csv", payments)
write_csv("journal_entries.csv", journal_entries)

print("\nSample dataset generated successfully in /data/")
print(f"Sales Orders: {len(sales_orders)}, Deliveries: {len(deliveries)}, Invoices: {len(invoices)}, Payments: {len(payments)}")
print(f"Broken flows: {len(sales_orders)-len(deliveries)} orders without delivery, {len(deliveries)-len(invoices)} delivered but not billed")
