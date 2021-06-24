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

# COMMAND ----------

pip install moneycontrol

# COMMAND ----------

pip install requests

# COMMAND ----------

pip install beautifulsoup4

# COMMAND ----------

pip install html5lib

# COMMAND ----------

pip install bs4


# COMMAND ----------

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd  

url = 'https://www.moneycontrol.com/india/stockpricequote/'
reqs = requests.get(url)
soup = BeautifulSoup(reqs.text, 'html.parser')
tbl_lst = []
for table_tag in soup.find_all('table'):
  for each_row in table_tag.find_all('tr'):
    links = each_row.find('a', href=True)
    if links:
      title = links.get('title')
      http_url = links.get('href')
      item=title +","+ http_url
      items = item.split(',')
      tbl_lst.append([items[0], items[1]])
df1 = pd.DataFrame(tbl_lst)
df1.columns =['Name', 'Url']
df1

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

url = "https://www.moneycontrol.com/india/stockpricequote/"
r = requests.get(url)
soup = BeautifulSoup(r.content, 'html5lib')
print(soup.prettify())