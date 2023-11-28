import requests as rq
import pandas as pd
import sqlalchemy
import pymssql
import datetime as dt
from sqlalchemy import types 
import configparser
print('Scheduler Started at' + str(dt.datetime.now()))

ID = [
    "T10Y3M",   
    "NFCI",
    "WALCL",
    "T10Y2Y",
    "ANFCI",
    "USALOLITONOSTSAM",
    "USPHCI",
    "FEDFUNDS"
]

today = dt.datetime.now().strftime('%Y-%m-%d')


for id in ID :
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    url = config.get('development','mysql_connection_string')
    engine = sqlalchemy.create_engine(url)
    
    df = pd.read_csv('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=958&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id='+id+'&scale=left&cosd=1953-01-01&coed='+today+'&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date='+today+'&revision_date='+today+'&nd=1953-01-01')
    # df.to_csv('Scraped Data/Fred/'+id+'.csv',index=False)
    df.rename(columns={id:'PRICE'},inplace=True)
    columns_type = {'DATE': types.DATE,'PRICE':types.VARCHAR(50)}
    # print(id)
    # print(df)
    df.to_sql(name=id,con=engine,if_exists='replace',index=False,dtype=columns_type,method='multi',chunksize=1000)
    engine.dispose()
    print('----------')
    # break

print('Scheduler Completed at ' + str(dt.datetime.now()))  

############## Selenium Code ###################
# from selenium import webdriver  
# import time  
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.by import By  
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import Select
# import random,os,shutil

# URL = ["https://fred.stlouisfed.org/series/NFCI",
#        "https://fred.stlouisfed.org/series/WALCL",
#        "https://fred.stlouisfed.org/series/T10Y3M",
#        "https://fred.stlouisfed.org/series/T10Y2Y"
# ]

# for url in URL :

#     driver = webdriver.Chrome()
#     driver.maximize_window()  

#     driver.get(url)  

#     print('Step ---- 1 Start Download')
#     time.sleep(random.randint(0, 10))
#     driver.find_element(By.ID,"download-button").click()
#     print('Step ---- 1 Complete Download')

#     print('Step ---- 2 Start CSV Download')
#     time.sleep(random.randint(3, 10))
#     driver.find_element(By.ID,"download-data-csv").click()
#     print('Step ---- 2 Complete CSV Download')

#     time.sleep(5)
#     # shutil.move('YOUR DOWNLOAD DEFUALT PATH','WALCL.csv')
#     shutil.move('C:/Users/Yash/Downloads/'+url.split('/')[-1]+'.csv','Scraped Data/Fred/'+url.split('/')[-1]+'.csv')

#     driver.close()

############## Selenium Code ###################