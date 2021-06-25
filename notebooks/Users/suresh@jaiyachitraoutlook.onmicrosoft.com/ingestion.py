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

pip install beautifulsoup4

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

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd  

def getStkOverview(soup):
  Price = soup.find("td", text="VWAP").find_next_sibling("td").text
  Volume = soup.find("td", text="Volume").find_next_sibling("td").text
  AvgVolume = soup.find("td", text="20D Avg Volume").find_next_sibling("td").text
  AvgDelivery = soup.find("td", text="20D Avg Delivery").find_next_sibling("td").text
  MktCap = soup.find("td", text="Mkt Cap (Rs. Cr.)").find_next_sibling("td").text
  eps = soup.find("td", text="TTM EPS").find_next_sibling("td").text
  pe = soup.find("td", text="TTM PE").find_next_sibling("td").text
  Revenue = soup.find("td", text="Revenue").find_next_sibling("td").text
  NetProfit = soup.find("td", text="NetProfit").find_next_sibling("td").text
  OperatingProfit = soup.find("td", text="OperatingProfit").find_next_sibling("td").text
  #Moving_Averages = soup.find("td", text="Moving Averages").find_next_sibling("td").text
  #Technical_Indicators = soup.find("td", text=" Technical Indicators").find_next_sibling("td").text
  #Moving_Averages_Crossovers = soup.find("td", text="Moving Averages Crossovers").find_next_sibling("td").text
  
  
  #stk_concat=Price+";"+Volume+";"+","+AvgVolume+";"+AvgDelivery+";"+MktCap+";"+eps+";"+pe+";"+Revenue+";"+NetProfit+";"+OperatingProfit+";"+Moving_Averages+";"+Technical_Indicators+";"+Moving_Averages_Crossovers
  stk_concat=Price+";"+Volume+";"+","+AvgVolume+";"+AvgDelivery+";"+MktCap+";"+eps+";"+pe+";"+Revenue+";"+NetProfit+";"+OperatingProfit
  stk_concat=stk_concat.replace(',','')
  stk_concat=stk_concat.replace(';',',')
  return stk_concat

  
url = 'https://www.moneycontrol.com/stocks/marketstats/nsehigh/index.php'

reqs = requests.get(url)
soup = BeautifulSoup(reqs.content, 'html.parser')
tbl_lst=[]
for i in soup.find_all('span',{'class':'gld13 disin'}):
  link = i.find('a',href=True)
  title = link.get('title')
  if link is None:
    continue
  item=link['title'] +","+ link['href']
  items = item.split(',')
  tbl_lst.append([items[0], items[1]])
res = []
[res.append(x) for x in tbl_lst if x not in res]
df2 = pd.DataFrame(res)
df2.columns =['Name', 'Url']

stk_lst=[]
for index, row in df2.iterrows():
  print(str(index)+">>>"+row['Url'])
  stk_reqs = requests.get(row['Url'])
  stk_soup = BeautifulSoup(stk_reqs.text, 'html.parser')
  stk_item=getStkOverview(stk_soup)
  stk_items = stk_item.split(',')
  stk_lst.append([row['Name'],stk_items[0], stk_items[1], stk_items[2], stk_items[3], stk_items[4], stk_items[5], stk_items[6], stk_items[7], stk_items[8], stk_items[9],row['Url']])
df3 = pd.DataFrame(stk_lst)
df3.columns =['Name','Price', 'Volume', '20DayAvgVolume','20DayAvgDelivery','MktCap', 'eps', 'pe', 'Revenue', 'NetProfit', 'OperatingProfit','Url']
sdf=spark.createDataFrame(df3)
display(sdf)

# COMMAND ----------

df3['Price'] = pd.to_numeric(df3['Price'])
df3['Volume'] = pd.to_numeric(df3['Volume'])
df3['20DayAvgVolume'] = pd.to_numeric(df3['20DayAvgVolume'])
df3['MktCap'] = pd.to_numeric(df3['MktCap'])
df3.loc[(df3.eps == '--'),'eps']='0'
df3['eps'] = pd.to_numeric(df3['eps'])
df3.loc[(df3.pe == '--'),'pe']='0'
df3['pe'] = pd.to_numeric(df3['pe'])
df3.loc[(df3['20DayAvgDelivery'] == '--'),'20DayAvgDelivery']='0'
df3['20DayAvgDelivery'] = pd.to_numeric(df3['20DayAvgDelivery'])
df4 = df3[df3['Price'] > 20 ] 
df4 = df4[df4['Price'] < 200]
df4 = df4[df4['Volume'] > 100000 ] 
df4 = df4[df4['20DayAvgVolume'] > 100000 ] 
df4 = df4[df4['MktCap'] > 100 ] 
df4 = df4[df4['eps'] > 0 ] 
df4


# COMMAND ----------

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd  

url = 'https://www.moneycontrol.com/india/stockpricequote/power-transmissionequipment/a2zinfraengineering/AME02'

reqs = requests.get(url)
soup = BeautifulSoup(reqs.text, 'html.parser')
print(soup.find_all("td"))
#tbl_lst = []
#Price = soup.find("td", text="VWAP").find_next_sibling("td").text
#print("Price : "+Price)
#
#Volume = soup.find("td", text="Volume").find_next_sibling("td").text
#print("Today Volume : "+Volume)
#
#AvgVolume = soup.find("td", text="20D Avg Volume").find_next_sibling("td").text
#print("20D Avg Volume : "+AvgVolume)
#
#eps = soup.find("td", text="TTM EPS").find_next_sibling("td").text
#print("TTM EPS : "+eps)
#
#pe = soup.find("td", text="TTM PE").find_next_sibling("td").text
#print("TTM PE : "+pe)
#
#Revenue = soup.find("td", text="Revenue").find_next_sibling("td").text
#print("Revenue : "+Revenue)
#
#NetProfit = soup.find("td", text="NetProfit").find_next_sibling("td").text
#print("NetProfit : "+NetProfit)
#
#OperatingProfit = soup.find("td", text="OperatingProfit").find_next_sibling("td").text
#print("OperatingProfit : "+OperatingProfit)
#
#Moving_Averages = soup.find("td", text="Moving Averages").find_next_sibling("td").text
#print("Moving Averages : "+Moving_Averages)
#
#Technical_Indicators = soup.find("td", text=" Technical Indicators").find_next_sibling("td").text
#print("Technical Indicators : "+Technical_Indicators)
#
#Moving_Averages_Crossovers = soup.find("td", text="Moving Averages Crossovers").find_next_sibling("td").text
#print("Moving Averages Crossovers : "+Moving_Averages_Crossovers)

