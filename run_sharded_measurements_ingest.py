import subprocess, sys, os, time
from dotenv import load_dotenv

load_dotenv()

COUNTRY = os.getenv("COUNTRY_CODE", "PT")
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "4"))

ENV = os.environ.copy()

# Optional: warn instead of hard fail
required = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
missing = [v for v in required if v not in ENV]
if missing:
    print(f"⚠️ Missing recommended env vars: {missing}. Exiting.")
    raise SystemExit(1)

# SPARK_REMOTE / DATABRICKS_CLUSTER_ID only if you rely on them elsewhere
print("Launching shard processes...")

procs = []
for i in range(SHARD_COUNT):
    cmd = [
        sys.executable,
        "ingest_bronze_measurements.py",
        "--country", COUNTRY,
        "--shard-index", str(i),
        "--shard-count", str(SHARD_COUNT)
    ]
    p = subprocess.Popen(cmd, env=ENV)
    procs.append(p)
    time.sleep(2)  # stagger

for p in procs:
    p.wait()

print("Done.")