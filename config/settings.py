import os
import sys
import json

# ---------------------------------------------------
# Detect Databricks environment
# ---------------------------------------------------
try:
    from pyspark.dbutils import DBUtils
    import pyspark
    _spark = pyspark.sql.SparkSession.builder.getOrCreate()
    dbutils = DBUtils(_spark)
    IS_DATABRICKS = True
except Exception:
    dbutils = None
    IS_DATABRICKS = False

# ---------------------------------------------------
# Optional CLI override for workspace config path
#   --config_workspace_path /Workspace/Users/you/config.xxx.json
# ---------------------------------------------------
def _cli_config_path():
    for i, tok in enumerate(sys.argv):
        if tok in ("--config_workspace_path", "--config-path") and i + 1 < len(sys.argv):
            return sys.argv[i + 1].strip()
        if tok.startswith("--config_workspace_path="):
            return tok.split("=", 1)[1].strip()
        if tok.startswith("--config-path="):
            return tok.split("=", 1)[1].strip()
    return None

# ---------------------------------------------------
# Load workspace JSON (semi‑sensitive, not in repo)
# Precedence for path: CLI arg > ENV var CONFIG_WORKSPACE_PATH > default path
# ---------------------------------------------------
def load_workspace_config():
    if not IS_DATABRICKS:
        return {}
    override = _cli_config_path() or os.getenv("CONFIG_WORKSPACE_PATH")
    path = override or "/Workspace/Users/andrecaiado@gmail.com/config.data-air-quality-monitor.json"
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

WORKSPACE_CONFIG = load_workspace_config()

# ---------------------------------------------------
# Load local .env (only outside Databricks)
# ---------------------------------------------------
if not IS_DATABRICKS:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

# ---------------------------------------------------
# Unified config getter
# Precedence:
# 1. Secret scope (if provided)
# 2. Workspace JSON (Databricks only)
# 3. Environment variables (.env or exported)
# 4. Explicit default
# ---------------------------------------------------
def get_config(key, default=None, secret_scope=None):
    if IS_DATABRICKS and secret_scope:
        try:
            return dbutils.secrets.get(scope=secret_scope, key=key)
        except Exception:
            pass
    if IS_DATABRICKS and key in WORKSPACE_CONFIG:
        return WORKSPACE_CONFIG[key]
    if key in os.environ:
        return os.environ[key]
    return default