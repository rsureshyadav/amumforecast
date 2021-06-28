# Databricks notebook source
pip install requests

# COMMAND ----------

pip install beautifulsoup4

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
index = df2.index
number_of_rows = len(index)
print("Total Count : "+str(number_of_rows))
stk_lst=[]
for index, row in df2.iterrows():
  print(str(index)+">>>"+row['Url'])
  stk_reqs = requests.get(row['Url'])
  stk_soup = BeautifulSoup(stk_reqs.text, 'html.parser')
  if stk_soup is None:
    continue
  stk_item=getStkOverview(stk_soup)
  stk_items = stk_item.split(',')
  stk_lst.append([row['Name'],stk_items[0], stk_items[1], stk_items[2], stk_items[3], stk_items[4], stk_items[5], stk_items[6], stk_items[7], stk_items[8], stk_items[9],row['Url']])
df3 = pd.DataFrame(stk_lst)
df3.columns =['Name','Price', 'Volume', '20DayAvgVolume','20DayAvgDelivery','MktCap', 'eps', 'pe', 'Revenue', 'NetProfit', 'OperatingProfit','Url']
#####APPLY RULES#######
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
###SWOT####
swot_metric_lst=[]
for index, row in df4.iterrows():
  url_lst = row['Url'].split("/")
  fnl_url = 'https://www.moneycontrol.com/swot-analysis/'+url_lst[len(url_lst)-2]+'/'+url_lst[len(url_lst)-1]+'/strength'
  reqs = requests.get(fnl_url)
  soup = BeautifulSoup(reqs.content, 'html.parser')
  swot_lst=[]
  for i in soup.find_all('div',{'class':'swot_count'}):
    swot_lst.append([i.text])
  for item in swot_lst:
    str_item = str(item)
    str_item = str_item.replace('\\n', '')
    str_item = [character for character in str_item if character.isalnum()]
    str_item = "".join(str_item)
    if "Strengths" in str_item:
      Strengths = str_item.replace("Strengths",'')
    elif "Weaknesses" in str_item:
      Weaknesses = str_item.replace("Weaknesses",'')
    elif "Opportunities" in str_item:
      Opportunities = str_item.replace("Opportunities",'')
    elif "Threats" in str_item:
      Threats = str_item.replace("Threats",'')
  swot_metric_lst.append([row['Name'],row['Price'],row['MktCap'],Strengths,Weaknesses,Opportunities,Threats,row['Url']])	
df5 = pd.DataFrame(swot_metric_lst)
df5.columns =['Name','Price','MktCap','Strengths','Weaknesses','Opportunities','Threats','Url']
sdf=spark.createDataFrame(df5)
display(sdf)
