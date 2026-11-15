import sys, json, time, pandas as pd
from confluent_kafka import Producer, Consumer

TOPIC, BROKER = "transactions", "localhost:9092"

def produce(n=8000, rate=200):
    df = (pd.read_csv("paysim.csv").query("step > 600").sample(n)
          .sort_values("step").reset_index().rename(columns={"index": "txn_id"}))
    p = Producer({"bootstrap.servers": BROKER})
    for r in df.to_dict("records"):
        r["event_ts"] = time.time()
        p.produce(TOPIC, key=r["nameOrig"], value=json.dumps(r))
        p.poll(0)
        time.sleep(1 / rate)
    p.flush()

def consume():
    c = Consumer({"bootstrap.servers": BROKER, "group.id": "check",
                  "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    while True:
        m = c.poll(1.0)
        if m and not m.error():
            print(m.value().decode())

{"produce": produce, "consume": consume}[sys.argv[1]]()