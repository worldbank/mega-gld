# Databricks notebook source
# MAGIC %pip install pytest
# MAGIC %pip install pyreadstat

# COMMAND ----------

import pytest
import os
import subprocess, sys


# COMMAND ----------

subprocess.run([sys.executable, "-m", "pytest", "-q", "--disable-warnings", "pytest"], check=False)

