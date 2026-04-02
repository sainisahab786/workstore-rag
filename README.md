This project is a **Natural Language to SQL (NLQ) API** built using FastAPI.  
It converts user questions into SQL queries using OpenAI, executes them on a MySQL database, and returns structured results.

---

## 📌 Features

- 🔹 Convert natural language → SQL using OpenAI
- 🔹 MySQL query execution via SQLAlchemy
- 🔹 In-memory caching for faster repeated queries
- 🔹 Request timeout handling (8 seconds)
- 🔹 OpenAI timeout + retry protection
- 🔹 SQL safety checks (prevents destructive queries)
- 🔹 Optimized DB connection pooling
- 🔹 Detailed logging for performance tracking



---------------Setup--------------------------

git clone <url>

pip install -r requirements.txt

make .env file m below is the template 
-------------------##-----------------------------------------------------
DB_HOST=holistique-middleware.c9wdjmzy25ra.ap-south-1.rds.amazonaws.com
DB_PORT=3306
DB_USER=TEST_USER
DB_PASSWORD=TEST_USER
DB_NAME=TEST

# OpenAI API Key
OPENAI_API_KEY=your_openai_api_key_here
ENV=development
LOG_LEVEL=info
-------------------##------------------------------------------------------


# run command 

uvicron main:app
