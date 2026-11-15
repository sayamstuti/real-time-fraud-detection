import os, re, json, time, requests, pandas as pd
from openai import OpenAI
from sqlalchemy import create_engine, text

eng = create_engine("postgresql://fraud:fraud@localhost:5432/fraud")
client = OpenAI(api_key=os.environ["GEMINI_API_KEY"],
                base_url="https://generativelanguage.googleapis.com/v1beta/openai/")
MODEL, N = "gemini-3.1-flash-lite", 3      # N = how many flagged txns to triage per run

with eng.begin() as c:
    c.execute(text("""CREATE TABLE IF NOT EXISTS alerts (txn_id BIGINT PRIMARY KEY, risk_score INT,
    rationale TEXT, action TEXT, status TEXT DEFAULT 'pending_review', created_ts TIMESTAMP DEFAULT now())"""))

rows = pd.read_sql(text("""SELECT * FROM scored_txns s WHERE flagged=1
    AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.txn_id=s.txn_id)
    ORDER BY fraud_score DESC LIMIT :n"""), eng, params={"n": N})

SYS = ("You are a fraud analyst assistant. Explain why this flagged transaction looks suspicious using ONLY the "
       "type, amount and balance fields given. Do not speculate about causes such as account takeover or "
       "money laundering. Reply with ONLY a JSON object with keys: risk_score (integer 0-100), "
       "rationale (max 2 sentences citing the numbers), "
       "action (one of: recommend_block, hold_for_review, contact_customer).")
NUM = ["amount", "oldbalanceorg", "newbalanceorig", "oldbalancedest", "newbalancedest"]

for r in rows.itertuples():
    facts = {"type": r.type, **{k: float(getattr(r, k)) for k in NUM}}   # isfraud label is NOT sent
    try:
        out = client.chat.completions.create(model=MODEL, messages=[
            {"role": "system", "content": SYS},
            {"role": "user", "content": json.dumps(facts)}]).choices[0].message.content
        a = json.loads(re.search(r"\{.*\}", out, re.S).group())
        with eng.begin() as c:
            c.execute(text("INSERT INTO alerts (txn_id,risk_score,rationale,action) "
                           "VALUES (:t,:s,:r,:a) ON CONFLICT DO NOTHING"),
                      dict(t=int(r.txn_id), s=int(a["risk_score"]), r=a["rationale"], a=a["action"]))
        if os.getenv("SLACK_WEBHOOK"):
            requests.post(os.environ["SLACK_WEBHOOK"], json={"text":
                f"🚨 Txn {r.txn_id} | {r.type} {r.amount:,.0f} | risk {a['risk_score']} | {a['action']}\n{a['rationale']}"})
        print(r.txn_id, a)
    except Exception as e:
        print("skipped", r.txn_id, "->", e, getattr(e, "body", ""))
        time.sleep(30)                      # likely a rate limit, wait and move on
        continue
    time.sleep(15)                           # stay under the free-tier request limit