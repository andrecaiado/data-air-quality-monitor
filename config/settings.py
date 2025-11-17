import json
import os
from pathlib import Path

# ---------------------------------------------------
# Detect Databricks environment
# ---------------------------------------------------
try:
    # dbutils exists only inside Databricks
    from pyspark.dbutils import DBUtils
    import pyspark
    dbutils = DBUtils(pyspark.sql.SparkSession.builder.getOrCreate())
    IS_DATABRICKS = True
except Exception:
    dbutils = None
    IS_DATABRICKS = False

# ---------------------------------------------------
# Load non-sensitive defaults from repo
# ---------------------------------------------------
def load_default_config():
    config_file = Path(__file__).parent / "config.json"
    with open(config_file) as f:
        return json.load(f)

DEFAULT_CONFIG = load_default_config()

# ---------------------------------------------------
# Load slightly-sensitive config from Databricks Workspace
# ---------------------------------------------------
def load_workspace_config():
    if not IS_DATABRICKS:
        return {}

    # IMPORTANT: change this to your actual Databricks path
    workspace_path = "/Workspace/Users/andrecaiado@gmail.com/config.data-air-quality-monitor.json"

    try:
        with open(workspace_path, "r") as f:
            return json.load(f)
    except:
        return {}

WORKSPACE_CONFIG = load_workspace_config()

# ---------------------------------------------------
# Load local .env only outside Databricks
# ---------------------------------------------------
if not IS_DATABRICKS:
    from dotenv import load_dotenv
    load_dotenv()

# ---------------------------------------------------
# Unified config getter
# ---------------------------------------------------
def get_config(key, default=None, secret_scope=None):
    """
    Order of precedence:
    1. Databricks Secret Scope (sensitive values)
    2. Workspace File (semi-sensitive, not in repo)
    3. Environment variables (.env locally)
    4. Repo defaults from config.json
    5. Explicit default value
    """

    # 1 — Databricks secret (ONLY for sensitive values)
    if IS_DATABRICKS and secret_scope:
        try:
            return dbutils.secrets.get(scope=secret_scope, key=key)
        except:
            pass

    # 2 — Databricks workspace file
    if IS_DATABRICKS and key in WORKSPACE_CONFIG:
        return WORKSPACE_CONFIG[key]

    # 3 — Environment variable (local .env or manual env vars)
    if key in os.environ:
        return os.environ[key]

    # 4 — Repo non-sensitive defaults
    if key in DEFAULT_CONFIG:
        return DEFAULT_CONFIG[key]

    # 5 — fallback default
    return default
