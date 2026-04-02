import os
import re
import time
import hashlib
import logging
import asyncio
from typing import Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
from openai import OpenAI, APITimeoutError

load_dotenv()

# ---------------- LOGGING CONFIG ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()

# ---------------- TIMEOUT MIDDLEWARE ---------------- #
@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    try:
        return await asyncio.wait_for(call_next(request), timeout=8.0)
    except asyncio.TimeoutError:
        logger.error("⏱ Request timed out")
        return JSONResponse(
            status_code=504,
            content={"detail": "Request timeout"}
        )

# ---------------- DB CONFIG ---------------- #
DB_URL = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
         f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={
        "connect_timeout": 5,
        "read_timeout": 3,
        "write_timeout": 3,
    }
)

try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ Database connected successfully")
except Exception as e:
    logger.error(f"Database connection failed: {str(e)}")
    raise e

# ---------------- OPENAI CLIENT ---------------- #
client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    timeout=6.0,   
    max_retries=1,
)

# ---------------- CACHE ---------------- #
_sql_cache: dict[str, tuple[str, float]] = {}
CACHE_TTL_SECONDS = 300
MAX_CACHE_SIZE = 500

def _get_cache_key(question: str) -> str:
    normalized = question.lower().strip()
    return hashlib.md5(normalized.encode()).hexdigest()

def get_cached_sql(question: str) -> Optional[str]:
    key = _get_cache_key(question)
    if key in _sql_cache:
        sql, ts = _sql_cache[key]
        if (time.time() - ts) < CACHE_TTL_SECONDS:
            logger.info("✅ Cache HIT")
            return sql
        del _sql_cache[key]
    return None

def cache_sql(question: str, sql: str):
    if len(_sql_cache) >= MAX_CACHE_SIZE:
        oldest_keys = sorted(_sql_cache.keys(), key=lambda k: _sql_cache[k][1])[:50]
        for k in oldest_keys:
            del _sql_cache[k]
    _sql_cache[_get_cache_key(question)] = (sql, time.time())
    logger.info(" Cache MISS - stored")

# ---------------- REQUEST MODEL ---------------- #
class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)

# ---------------- PROMPT ---------------- #
SCHEMA_CONTEXT = """You are a SQL expert. Generate ONLY a SELECT query for MySQL.

Tables:
- meta_campaigns(id, name, objective, status)
- meta_adsets(id, campaign_id, name, status)
- meta_ads(id, adset_id, name, status)
- meta_insight(ad_id, impressions, clicks, spend, conversions, date)
- meta_insight_age_gender(ad_id, age, gender, impressions, clicks)
- meta_insight_region(ad_id, region, impressions, clicks)

Rules:
- SELECT only, LIMIT 100 if unspecified
- Output raw SQL only, no explanation, no backticks
- Use proper JOINs"""

_RE_CLEAN = re.compile(r"```sql|```", re.IGNORECASE)
_RE_SELECT = re.compile(r"select", re.IGNORECASE)
_RE_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b",
    re.IGNORECASE
)

# ---------------- SQL GENERATION ---------------- #
def generate_sql(question: str) -> str:
    cached = get_cached_sql(question)
    if cached:
        return cached

    try:
        start_time = time.time()

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SCHEMA_CONTEXT},
                {"role": "user", "content": question}
            ],
            temperature=0,
            max_tokens=200,
        )

        raw_sql = response.choices[0].message.content.strip()

        sql = _RE_CLEAN.sub("", raw_sql).strip()

        match = _RE_SELECT.search(sql)
        if match:
            sql = sql[match.start():]
        else:
            raise ValueError(f"Invalid SQL: {raw_sql}")

        sql = sql.rstrip(";")

        if _RE_FORBIDDEN.search(sql):
            raise ValueError("Forbidden SQL operation")

        elapsed = round((time.time() - start_time) * 1000, 2)
        logger.info(f" SQL Generated in {elapsed} ms")

        cache_sql(question, sql)

        return sql

    except APITimeoutError:
        logger.error("OpenAI timeout")
        raise Exception("LLM request timed out")
    except Exception as e:
        logger.error(f"SQL Generation Failed: {str(e)}")
        raise Exception(f"SQL Generation Failed: {str(e)}")

# ---------------- SQL EXECUTION ---------------- #
def execute_sql(sql: str) -> list[dict]:
    try:
        start_time = time.time()

        with engine.connect() as conn:
            conn.execute(text("SET SESSION MAX_EXECUTION_TIME=3000"))
            result = conn.execute(text(sql))
            rows = [dict(row._mapping) for row in result]

        elapsed = round((time.time() - start_time) * 1000, 2)
        logger.info(f"Query executed in {elapsed} ms | Rows: {len(rows)}")

        return rows

    except Exception as e:
        logger.error(f"SQL Execution Failed: {str(e)}")
        raise Exception(f"SQL Execution Failed: {str(e)}")

# ---------------- ENDPOINT ---------------- #
@app.post("/query")
def query_db(req: QueryRequest):
    try:
        logger.info(f"Question: {req.question[:100]}")

        if len(req.question) > 300:
            raise HTTPException(status_code=400, detail="Query too long")

        total_start = time.time()

        sql = generate_sql(req.question)
        data = execute_sql(sql)

        total_time = round((time.time() - total_start) * 1000, 2)

        return {
            "question": req.question,
            "sql": sql,
            "count": len(data),
            "latency_ms": total_time,
            "result": data
        }

    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))