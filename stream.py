import sys, os, time, joblib, numpy as np, pandas as pd
import pyspark
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from sklearn.metrics import average_precision_score, precision_score, recall_score
from xgboost import XGBClassifier

F = ["amount_log", "amt_ratio", "errO", "errD", "is_transfer", "is_cashout", "orig_empty"]
COLS = ["txn_id", "step", "type", "amount", "nameorig", "oldbalanceorg", "newbalanceorig", "namedest",
        "oldbalancedest", "newbalancedest", "isfraud", "event_ts", "fraud_score", "flagged",
        "scored_ts", "latency_s"]
DDL = """CREATE TABLE IF NOT EXISTS scored_txns (
 txn_id BIGINT PRIMARY KEY, step INT, type TEXT, amount DOUBLE PRECISION, nameorig TEXT,
 oldbalanceorg DOUBLE PRECISION, newbalanceorig DOUBLE PRECISION, namedest TEXT,
 oldbalancedest DOUBLE PRECISION, newbalancedest DOUBLE PRECISION, isfraud INT,
 event_ts DOUBLE PRECISION, fraud_score DOUBLE PRECISION, flagged INT,
 scored_ts DOUBLE PRECISION, latency_s DOUBLE PRECISION)"""

def feats(d):
    d = d.copy()
    d["amount_log"] = np.log1p(d.amount)
    d["amt_ratio"] = d.amount / (d.oldbalanceOrg + 1)
    d["errO"] = d.newbalanceOrig + d.amount - d.oldbalanceOrg
    d["errD"] = d.newbalanceDest - d.oldbalanceDest - d.amount
    d["is_transfer"] = (d.type == "TRANSFER").astype(int)
    d["is_cashout"] = (d.type == "CASH_OUT").astype(int)
    d["orig_empty"] = (d.newbalanceOrig == 0).astype(int)
    return d

def train():
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import ConfusionMatrixDisplay, PrecisionRecallDisplay
    d = feats(pd.read_csv("paysim.csv"))
    tr, te = d[d.step <= 600], d[d.step > 600]
    m = XGBClassifier(n_estimators=200, max_depth=6, tree_method="hist",
                      scale_pos_weight=(tr.isFraud == 0).sum() / tr.isFraud.sum())
    m.fit(tr[F], tr.isFraud)
    p = m.predict_proba(te[F])[:, 1]
    print(f"PR-AUC {average_precision_score(te.isFraud, p):.3f} | "
          f"recall {recall_score(te.isFraud, p > .5):.3f} | "
          f"precision {precision_score(te.isFraud, p > .5):.3f}")
    r = ((te.orig_empty == 1) & (te.is_transfer + te.is_cashout > 0)).astype(int)
    print(f"rule baseline: precision {precision_score(te.isFraud, r):.3f} | recall {recall_score(te.isFraud, r):.3f}")
    fig, ax = plt.subplots(1, 3, figsize=(16, 4))
    ConfusionMatrixDisplay.from_predictions(te.isFraud, p > .5, ax=ax[0], colorbar=False, values_format="d")
    PrecisionRecallDisplay.from_predictions(te.isFraud, p, ax=ax[1])
    pd.Series(m.feature_importances_, F).sort_values().plot.barh(ax=ax[2], title="Feature importance")
    plt.tight_layout(); plt.savefig("results.png", dpi=150)
    joblib.dump(m, "model.pkl")

def run():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col
    from sqlalchemy import create_engine, text
    from sqlalchemy.dialects.postgresql import insert
    model = joblib.load("model.pkl")
    eng = create_engine("postgresql://fraud:fraud@localhost:5432/fraud")
    with eng.begin() as c: c.execute(text(DDL))

    def ins(table, conn, keys, rows):  # skip rows whose txn_id already exists
        conn.execute(insert(table.table).values([dict(zip(keys, r)) for r in rows]).on_conflict_do_nothing())

    spark = (SparkSession.builder.appName("fraud")
         .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{'2.13' if pyspark.__version__.startswith('4') else '2.12'}:{pyspark.__version__}")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    schema = ("txn_id long, step int, type string, amount double, nameOrig string, oldbalanceOrg double, "
              "newbalanceOrig double, nameDest string, oldbalanceDest double, newbalanceDest double, "
              "isFraud int, event_ts double")
    df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "transactions").option("startingOffsets", "latest").load()
          .select(from_json(col("value").cast("string"), schema).alias("j")).select("j.*"))

    def batch(b, _):
        t0 = time.time()
        p = b.toPandas()
        if p.empty: return
        p = feats(p)
        p["fraud_score"] = model.predict_proba(p[F])[:, 1]   # isFraud is NOT a model input
        p["flagged"] = (p.fraud_score > 0.5).astype(int)
        p["scored_ts"] = time.time()
        p["latency_s"] = p.scored_ts - p.event_ts
        p.columns = p.columns.str.lower()
        p[COLS].to_sql("scored_txns", eng, if_exists="append", index=False, method=ins)
        print(f"{len(p)} txns | {len(p)/(time.time()-t0):.0f} txn/s | flagged {p.flagged.sum()} "
              f"| avg latency {p.latency_s.mean():.1f}s")

    (df.writeStream.foreachBatch(batch).option("checkpointLocation", "chk")
       .trigger(processingTime="5 seconds").start().awaitTermination())

{"train": train, "run": run}[sys.argv[1]]()