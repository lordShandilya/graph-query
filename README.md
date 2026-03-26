# GraphQuery — Business Data Explorer

A graph-based data modeling and natural language query system built for the Forward Deployed Engineer assignment.

**Live demo:** `https://your-app.vercel.app` ← replace after deployment

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        React UI                         │
│   ┌─────────────────────┐  ┌──────────────────────────┐ │
│   │   Cytoscape.js      │  │   Chat Interface         │ │
│   │   Graph Viz         │  │   + Results Table        │ │
│   └─────────────────────┘  └──────────────────────────┘ │
└────────────────────┬────────────────────────────────────┘
                     │ REST API (axios)
┌────────────────────▼────────────────────────────────────┐
│                  FastAPI Backend                         │
│   ┌──────────┐  ┌──────────────┐  ┌──────────────────┐  │
│   │  /graph  │  │    /chat     │  │ /upload-dataset  │  │
│   │ endpoints│  │  (NL→SQL)    │  │ (ingest real CSV)│  │
│   └──────────┘  └──────┬───────┘  └──────────────────┘  │
└────────────────────────┼────────────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │         SQLite DB           │
          │  (10 tables, loaded from    │
          │   CSV/Excel on startup)     │
          └─────────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │       Google Gemini API     │
          │   NL → SQL generation       │
          │   SQL results → NL answer   │
          └─────────────────────────────┘
```

---

## Database / Storage Choice

**SQLite** was chosen over Neo4j or PostgreSQL for these reasons:

- **Zero-ops**: No server to provision. Single `.db` file. Renders (cloud host) supports it natively.
- **SQL is the right query language here**: The data is inherently relational (orders → deliveries → invoices). Graph DBs like Neo4j excel at multi-hop traversal queries, but most business questions here are 2-3 hop joins — well-served by SQL.
- **LLM SQL generation is mature**: Gemini can generate reliable SQLite syntax. Cypher (Neo4j) generation is less reliable from LLMs.
- **The graph is virtual**: The visual graph is constructed at query time from the relational tables. You get graph UX without graph DB complexity.

The graph is stored relationally and **rendered as a graph** — this is the right architectural tradeoff.

---

## Graph Modeling

### Nodes
| Type | ID Format | Source Table |
|------|-----------|-------------|
| Customer | C0001 | customers |
| SalesOrder | SO000001 | sales_orders |
| OrderItem | SOI000001 | sales_order_items |
| Product | M0001 | products |
| Delivery | DEL000001 | deliveries |
| Plant | P001 | plants |
| Invoice | INV000001 | invoices |
| Payment | PAY000001 | payments |
| JournalEntry | JE000001 | journal_entries |

### Edges (Relationships)
```
Customer ──PLACED──► SalesOrder
SalesOrder ──HAS_ITEM──► OrderItem
OrderItem ──IS_PRODUCT──► Product
SalesOrder ──DELIVERED_VIA──► Delivery
Delivery ──FROM_PLANT──► Plant
Delivery ──BILLED_AS──► Invoice
Invoice ──SETTLED_BY──► Payment
Invoice ──RECORDED_AS──► JournalEntry
```

### Design Decision: Lazy Expansion
The graph loads a summary view (20 orders) on startup to keep it fast. Double-clicking any node expands its neighbors on demand. This avoids rendering hundreds of nodes at once.

---

## LLM Prompting Strategy

### Two-stage pipeline

**Stage 1: NL → SQL**
```
System: You are a SQL expert. Here is the full schema with all table/column names 
        and FK relationships. Convert the user question to SQLite SQL.
        Rules: no markdown, LIMIT 50, handle broken flows with NOT IN subqueries.
User: "Which customers have the most unpaid invoices?"
```

**Stage 2: SQL Results → NL Answer**
```
System: You are a business analyst. Answer the question based on these query results.
        Rules: cite specific values, format numbers nicely, stay under 200 words.
User: [original question] + [SQL used] + [JSON results]
```

### Self-correction
If the generated SQL fails, the error + original SQL are fed back to Gemini to produce a corrected version. This handles edge cases (wrong column name, SQLite syntax differences).

### Why Gemini 1.5 Flash?
- Free tier: 15 requests/minute, 1M tokens/day — more than enough
- Fast (< 2s typical) 
- Strong at SQL generation from schema

---

## Guardrails

The system blocks off-topic queries at two levels:

**1. Pre-LLM keyword filter** (in `llm.py`):
- Checks for off-topic patterns: `poem`, `weather`, `who is`, `write me`, etc.
- Checks for domain keywords: `order`, `invoice`, `delivery`, `payment`, etc.
- Analytical queries (`which`, `how many`, `list`) pass through even without domain keywords

**2. LLM-level grounding**:
- The system prompt never gives the LLM permission to answer general questions
- Results are always drawn from real SQL query results, not hallucinated

Blocked response: *"🚫 This system is designed to answer questions related to the provided business dataset only."*

---

## Deployment Guide

### Prerequisites
- Python 3.10+
- Node.js 18+
- Gemini API key (free at https://ai.google.dev)
- GitHub account
- Render account (render.com)
- Vercel account (vercel.com)

---

### Step 1: Load Your Real Dataset

```bash
# If you have the dataset from Google Drive:
python3 scripts/ingest.py /path/to/your/dataset.xlsx

# Or use the UI — click "Load Real Dataset" button after deployment
```

---

### Step 2: Deploy Backend to Render

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → New → Web Service
3. Connect your GitHub repo
4. Settings:
   - **Build Command:** `pip install -r backend/requirements.txt && python3 scripts/generate_sample_data.py`
   - **Start Command:** `cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Root Directory:** (leave blank)
5. Add Environment Variable:
   - `GEMINI_API_KEY` = your key
6. Deploy → copy your Render URL (e.g. `https://graphquery-api.onrender.com`)

---

### Step 3: Deploy Frontend to Vercel

1. Go to [vercel.com](https://vercel.com) → New Project → Import your GitHub repo
2. Settings:
   - **Framework:** Create React App
   - **Root Directory:** `frontend`
   - **Build Command:** `npm run build`
   - **Output Directory:** `build`
3. Add Environment Variable:
   - `REACT_APP_API_URL` = your Render backend URL
4. Deploy → your app is live!

---

### Step 4: Load Real Dataset via UI

Once both are deployed:
1. Open your Vercel URL
2. Click **"📂 Load Real Dataset"** in the top right
3. Upload your `.xlsx` or `.zip` file
4. The graph and stats will refresh automatically

---

### Running Locally

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/graph-query-system
cd graph-query-system

# Copy env file
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY

# Start everything
chmod +x start.sh
./start.sh

# App: http://localhost:3000
# API: http://localhost:8000/docs
```

---

## Project Structure

```
graph-query-system/
├── backend/
│   ├── main.py          # FastAPI routes
│   ├── db.py            # SQLite + graph construction
│   ├── llm.py           # Gemini integration + guardrails
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.js       # Main React component
│   │   └── App.css      # Styles
│   └── package.json
├── scripts/
│   ├── generate_sample_data.py   # Creates realistic sample data
│   └── ingest.py                 # Loads real dataset (xlsx/zip/csv)
├── data/                         # Auto-created: CSVs + graph.db
├── start.sh                      # One-command local launcher
├── render.yaml                   # Render deployment config
└── README.md
```

---

## Example Queries the System Handles

| Query | What It Does |
|-------|-------------|
| "Which products have the most billing documents?" | JOINs products → order_items → invoices, COUNT + GROUP BY |
| "Show broken flows" | Finds sales_orders NOT IN deliveries subquery |
| "Trace invoice INV000001" | Follows full chain: SO → DEL → INV → PAY → JE |
| "Total revenue by category" | JOINs products → order_items, SUM by category |
| "Overdue invoices with customer names" | 3-table JOIN with WHERE status filter |

---

## Bonus Features Implemented

- ✅ **Natural language → SQL translation** (Gemini)
- ✅ **Node highlighting** when query results reference graph entities
- ✅ **SQL transparency** — expandable "Generated SQL" panel in chat
- ✅ **Self-correcting queries** — retries on SQL errors
- ✅ **Dataset uploader** — load real data via UI without redeploying
- ✅ **Lazy graph expansion** — double-click to expand neighbors on demand
