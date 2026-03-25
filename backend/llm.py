"""
llm.py - Gemini integration: NL → SQL → answer, with guardrails.
"""
import httpx, json, re, os
from db import get_schema_description, run_query

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

DOMAIN_KEYWORDS = [
    "order", "delivery", "invoice", "payment", "customer", "product", "material",
    "sales", "billing", "journal", "shipment", "plant", "item", "vendor", "partner",
    "amount", "status", "date", "flow", "track", "bill", "supply", "schedule",
    "revenue", "account", "carrier", "stock", "quantity", "price", "region",
    "overdue", "unpaid", "paid", "completed", "cancelled", "pending", "open",
    "so", "del", "inv", "pay", "je", "po", "erp", "sap", "document", "business",
    "outbound", "inbound", "dispatch", "fulfil", "fulfillment",
]

OFF_TOPIC_RE = re.compile(
    r"\b(poem|story|joke|recipe|weather|sport|movie|music|news|"
    r"write me|compose|creative|capital of|who is|history of|"
    r"explain quantum|tell me about yourself|what are you)\b",
    re.I
)


def is_domain_query(q: str) -> bool:
    if OFF_TOPIC_RE.search(q):
        return False
    ql = q.lower()
    if any(kw in ql for kw in DOMAIN_KEYWORDS):
        return True
    # Allow analytical phrasing even without explicit domain words
    analytical = ["which", "how many", "list", "show", "find", "trace",
                  "identify", "top", "most", "least", "average", "total",
                  "count", "all", "compare", "broken", "incomplete"]
    return any(w in ql for w in analytical)


async def call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY not set in .env"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1024}
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        if resp.status_code != 200:
            raise Exception(f"Gemini error {resp.status_code}: {resp.text[:200]}")
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]


def clean_sql(raw: str) -> str:
    return raw.strip().replace("```sql", "").replace("```", "").strip()


async def generate_sql(user_query: str) -> str:
    schema = get_schema_description()
    prompt = f"""You are a SQLite expert for a SAP-style business dataset.

{schema}

RULES:
1. Return ONLY valid SQLite SQL — no markdown, no explanation, no backticks
2. Always use double-quotes around table and column names with spaces or special chars
3. LIMIT to 50 rows unless user asks for more
4. For "broken flows": use LEFT JOIN ... WHERE right.col IS NULL, or NOT IN subqueries
5. For billing/invoice questions, use billing_headers table
6. For delivery questions, use delivery_headers table
7. For customer questions, use business_partners table
8. When joining, prefer explicit column matches over guessing

User question: {user_query}

SQL:"""
    raw = await call_gemini(prompt)
    return clean_sql(raw)


async def generate_answer(user_query: str, sql: str, results: list) -> str:
    preview = json.dumps(results[:20], indent=2) if results else "[]"
    prompt = f"""You are a business analyst. Answer the user's question based on real query results.

Question: {user_query}
SQL used: {sql}
Results ({len(results)} rows, showing up to 20):
{preview}

Rules:
- Answer directly and concisely based only on the data shown
- Mention specific IDs, names, counts from the results
- Format numbers with commas; prefix currency values with $
- If results are empty, say so clearly
- Max 150 words

Answer:"""
    return await call_gemini(prompt)


async def process_query(user_query: str) -> dict:
    # Guardrail
    if not is_domain_query(user_query):
        return {
            "answer": "🚫 This system only answers questions about the business dataset (orders, deliveries, invoices, payments, customers, products). Please ask something related to the data.",
            "sql": None, "results": [], "is_blocked": True
        }

    try:
        sql = await generate_sql(user_query)

        results, error = run_query(sql)

        # Self-correct on error
        if error:
            schema = get_schema_description()
            fix_prompt = f"""This SQL failed on SQLite:

SQL: {sql}
Error: {error}

Schema:
{schema}

Write a corrected SQL query. Return ONLY the SQL, no explanation:"""
            sql = clean_sql(await call_gemini(fix_prompt))
            results, error2 = run_query(sql)
            if error2:
                return {"answer": f"Could not generate a valid query. Error: {error2}",
                        "sql": sql, "results": [], "is_blocked": False}

        answer = await generate_answer(user_query, sql, results or [])
        return {"answer": answer, "sql": sql, "results": results or [], "is_blocked": False}

    except Exception as e:
        return {"answer": f"Error: {str(e)}", "sql": None, "results": [], "is_blocked": False}
