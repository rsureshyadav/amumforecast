# Databricks notebook source
pip install nsetools

# COMMAND ----------

from nsetools import Nse
import json

import pandas as pd    

nse = Nse()
all_stock_codes = nse.get_stock_codes()
top_gainers = nse.get_top_gainers()
df = pd.DataFrame(top_gainers)
df