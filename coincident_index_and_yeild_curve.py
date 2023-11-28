import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from sqlalchemy import types 
import sqlalchemy
import datetime
import configparser

URL = [ "https://tradingeconomics.com/united-states/composite-leading-indicator"
    ]

config = configparser.ConfigParser()
config.read('config.ini')
url = config.get('development','mysql_connection_string')
engine = sqlalchemy.create_engine(url)

for url in URL :
    scraper = cloudscraper.create_scraper(browser={'browser': 'firefox','platform': 'windows','mobile': False})

    html = scraper.get(url).content

    soup = BeautifulSoup(html, 'html5lib').find("div", {"id": "ctl00_ContentPlaceHolder1_ctl00_ctl01_Panel1"}).find('tbody')

    list = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td') 
        for td in tds:
            if td.text != '' :
                list.append((td.text).strip())

    table_data = pd.DataFrame([[datetime.datetime.now().strftime('%b-%Y'),float(list[0]),float(list[1]),float(list[2]),float(list[3]),list[4],list[5],list[6],datetime.datetime.now().strftime('%Y-%m-%d')]],columns=['Month','Actual','Previous','Highest','Lowest','Dates','Unit','Frequency','Updated_On'])

    # table_data.to_csv('Scraped Data/Trading Economics/'+url.rsplit('/')[-1]+'.csv',index=False)
    
    columns_type = {'Month': types.VARCHAR(50),'Actual': types.FLOAT,'Previous':types.FLOAT,'Highest':types.FLOAT,'Lowest':types.FLOAT,'Dates':types.VARCHAR(50),'Unit':types.VARCHAR(50),'Frequency':types.VARCHAR(50),'Updated_On':types.Date}
    
    data_present = pd.read_sql(sql='SELECT * FROM `stock-rw-db`.`Coincident Index & Yield Curve` order by Updated_On desc limit 1',con=engine)

    if (table_data['Actual'][0] != data_present['Actual'][0] ):
        table_data.to_sql(name='Coincident Index & Yield Curve',con=engine,if_exists='append',index=False,dtype=columns_type,method='multi',chunksize=1000)
        
    engine.dispose()
    
    print(table_data)