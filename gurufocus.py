from bs4 import BeautifulSoup
import datetime
import cloudscraper
import pandas as pd
import sqlalchemy
import configparser

SYMBOLS = [ 
        "stock-market-valuations"
]

print('Scheduler Started at ' + str(datetime.datetime.now()))
config = configparser.ConfigParser()
config.read('config.ini')
url = config.get('development','mysql_connection_string')
engine = sqlalchemy.create_engine(url)

for symbol in SYMBOLS :
    url = "https://www.gurufocus.com/"+symbol+'.php'
    
    scraper = cloudscraper.create_scraper(browser={'browser': 'firefox','platform': 'windows','mobile': False})
    html = scraper.get(url).content
    soup = BeautifulSoup(html, 'html5lib').find("div", {"class": "left with-margin"})
    
    df = pd.DataFrame()
    for table in soup :
        df['Date'] = [datetime.datetime.now()]
        df['News'] = [table.text.split('Bookmark')[0]]     
        break
    
    columns_type = {'Date': sqlalchemy.types.DATE,'News': sqlalchemy.types.TEXT}
    
    df.to_sql(name=symbol,con=engine,if_exists='append',index=False,dtype=columns_type,method='multi',chunksize=1000)

print('Scheduler Completed at ' + str(datetime.datetime.now()))