"""
llm.py - LLM integration for natural language → SQL → answer pipeline.
Uses Google Gemini free tier by default.
"""
import httpx
import json
import re
import os
from db import get_schema_description, run_query

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

DOMAIN_KEYWORDS = [
    "order", "delivery", "invoice", "payment", "customer", "product", "material",
    "sales", "billing", "journal", "shipment", "plant", "item", "vendor",
    "amount", "status", "date", "flow", "track", "invoice", "bill", "supply",
    "revenue", "account", "carrier", "stock", "quantity", "price", "region",
    "overdue", "unpaid", "paid", "completed", "cancelled", "pending",
    "so", "del", "inv", "pay", "je", "po", "erp", "sap",
]

OFF_TOPIC_PATTERNS = [
    r"\b(poem|story|joke|recipe|weather|sport|movie|music|news|code|python|javascript|html|css)\b",
    r"\b(who is|what is the capital|history of|explain quantum|tell me about yourself)\b",
    r"\b(write me|compose|generate a story|creative)\b",
]


def is_domain_query(query: str) -> bool:
    """Check if the query is related to the business dataset."""
    q_lower = query.lower()
    
    # Check off-topic patterns
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, q_lower):
            return False
    
    # Check if it contains domain-relevant terms
    for kw in DOMAIN_KEYWORDS:
        if kw in q_lower:
            return True
    
    # Allow short analytical queries that might not contain keywords
    analytical_words = ["which", "how many", "list", "show", "find", "trace", "identify", 
                        "compare", "top", "most", "least", "average", "total", "count", "all"]
    has_analytical = any(w in q_lower for w in analytical_words)
    
    return has_analytical


async def generate_sql(user_query: str) -> dict:
    """Call Gemini to convert natural language query into SQL."""
    schema = get_schema_description()
    
    prompt = f"""You are a SQL expert for a business data system. Convert the user's natural language question into a SQLite SQL query.

{schema}

RULES:
1. Return ONLY valid SQLite SQL - no markdown, no explanation, no backticks
2. Use proper JOINs across tables
3. Limit results to 50 rows maximum unless asked for more
4. For "broken flows": orders without delivery = sales_orders not in deliveries; delivered not billed = deliveries not in invoices
5. Use aliases for clarity

User question: {user_query}

SQL query:"""

    response = await call_gemini(prompt)
    sql = response.strip().replace("```sql", "").replace("```", "").strip()
    
    # Remove any leading/trailing non-SQL text
    lines = sql.split('\n')
    sql_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--') or stripped.upper().startswith('SELECT') or sql_lines:
            sql_lines.append(line)
    
    return {"sql": '\n'.join(sql_lines).strip()}


async def generate_answer(user_query: str, sql: str, results: list) -> str:
    """Call Gemini to convert SQL results into a natural language answer."""
    
    results_preview = json.dumps(results[:20], indent=2) if results else "[]"
    total_count = len(results) if results else 0
    
    prompt = f"""You are a business analyst assistant. Answer the user's question based on the SQL query results.

User question: {user_query}

SQL executed: {sql}

Results ({total_count} rows total, showing up to 20):
{results_preview}

RULES:
1. Answer concisely and directly based on the data
2. If results are empty, say so clearly
3. Mention specific values, counts, and names from the data
4. Format numbers nicely (use $ for amounts, commas for large numbers)
5. Keep response under 200 words
6. Do NOT make up data not in the results

Answer:"""

    return await call_gemini(prompt)


async def call_gemini(prompt: str) -> str:
    """Make a request to the Gemini API."""
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY not set. Please add it to your .env file."
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1024,
        }
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        if resp.status_code != 200:
            raise Exception(f"Gemini API error: {resp.status_code} - {resp.text}")
        
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]


async def process_query(user_query: str) -> dict:
    """Full pipeline: NL query → SQL → execute → NL answer."""
    
    # Guardrail check
    if not is_domain_query(user_query):
        return {
            "answer": "🚫 This system is designed to answer questions related to the provided business dataset only. Please ask about orders, deliveries, invoices, payments, customers, or products.",
            "sql": None,
            "results": [],
            "is_blocked": True
        }
    
    try:
        # Generate SQL
        sql_result = await generate_sql(user_query)
        sql = sql_result["sql"]
        
        # Execute SQL
        results, error = run_query(sql)
        
        if error:
            # Try to self-correct
            correction_prompt = f"""The SQL query failed with error: {error}

Original query: {user_query}
Failed SQL: {sql}

Schema: {get_schema_description()}

Write a corrected SQLite SQL query (no markdown, no explanation):"""
            corrected_sql = await call_gemini(correction_prompt)
            corrected_sql = corrected_sql.strip().replace("```sql", "").replace("```", "").strip()
            results, error2 = run_query(corrected_sql)
            if error2:
                return {
                    "answer": f"I couldn't generate a valid query for your question. Error: {error2}",
                    "sql": corrected_sql,
                    "results": [],
                    "is_blocked": False
                }
            sql = corrected_sql
        
        # Generate natural language answer
        answer = await generate_answer(user_query, sql, results or [])
        
        return {
            "answer": answer,
            "sql": sql,
            "results": results or [],
            "is_blocked": False
        }
    
    except Exception as e:
        return {
            "answer": f"An error occurred while processing your query: {str(e)}",
            "sql": None,
            "results": [],
            "is_blocked": False
        }
