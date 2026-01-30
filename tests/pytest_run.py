# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import os

# COMMAND ----------

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTEST_ADDOPTS"] = "--assert=plain"
os.environ["PYTEST_CACHE_DIR"] = "/tmp/pytest_cache"

# COMMAND ----------

pytest.main([
    "-q",
    "--disable-warnings",
    "pytest",
])

