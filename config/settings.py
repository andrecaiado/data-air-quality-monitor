import os
import sys
import importlib.util

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
# CLI override for config module path
#   --config_file_path /Workspace/Users/<user>/data-air-quality-monitor/config_dev.py
# ---------------------------------------------------
def _cli_config_path():
    for i, tok in enumerate(sys.argv):
        if tok == "--config_file_path" and i + 1 < len(sys.argv):
            return sys.argv[i + 1].strip()
        if tok.startswith("--config_file_path="):
            return tok.split("=", 1)[1].strip()
    return None

CONFIG_MODULE_PATH = _cli_config_path()

# ---------------------------------------------------
# Load workspace Python module (expects CONFIG dict)
# ---------------------------------------------------
def load_workspace_config():
    if not IS_DATABRICKS:
        return {}
    path = CONFIG_MODULE_PATH
    if not path:
        return {}
    if not os.path.exists(path):
        return {}
    try:
        spec = importlib.util.spec_from_file_location("external_config", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, "CONFIG", {})
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