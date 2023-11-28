import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
import datetime
import time
import sqlalchemy
from sqlalchemy import types
import datetime as dt
import configparser


SYMBOLS = [ 
           "ishares-short-treasury-bond-mx",
            "vanguard-ftse-all-world-x-us",
            "proshares-vix-short-term-futures",
            "ishares-h-y-corporate-bond",
            "ishares-inv-g-bond",
            "ishares-lehman-20-year-treas",
            "spdr-gold-trust"
        ]


print('Scheduler Started at' + str(datetime.datetime.now()))

for symbol in SYMBOLS :
    config = configparser.ConfigParser()
    config.read('config.ini')
    url = config.get('development','mysql_connection_string')
    engine = sqlalchemy.create_engine(url)

    start_date = "01/01/1972"
    end_date = dt.datetime.now().strftime('%d/%m/%Y')
    
    start_date = datetime.datetime.strptime(start_date,"%d/%m/%Y")
    end_date = datetime.datetime.strptime(end_date,"%d/%m/%Y")
    
    start_date = start_date.timetuple()
    end_date = end_date.timetuple()

    start_timestamp = time.mktime(start_date)
    end_timestamp = time.mktime(end_date)

    url = "https://in.investing.com/etfs/"+symbol+"-historical-data?end_date="+str(end_timestamp)+"&interval_sec=weekly&st_date="+str(start_timestamp)
    print(url)

    scraper = cloudscraper.create_scraper(browser={'browser': 'firefox','platform': 'windows','mobile': False})

    html = scraper.get(url).content

    soup = BeautifulSoup(html, 'html5lib').find("table", {"class": "common-table medium js-table"})

    # print(soup)
    table_data = pd.DataFrame()

    for table in soup.find_all('tr',{'class' : 'common-table-item'}):

        temp_df = pd.DataFrame()
        td = table.find_all('td')
        date = td[0].find('span').text
        price = td[1].find('span').text
        open = td[2].find('span').text
        high = td[3].find('span').text
        low = td[4].find('span').text
        volume = td[5].find('span').text
        change_per = td[6].find('span').text
        temp_df['date'] = [datetime.datetime.strftime(datetime.datetime.strptime(date.strip(), '%b %d, %Y'),'%Y-%m-%d')]
        temp_df['price'] = [price.replace(',','')]
        temp_df['open'] = [open.replace(',','')]
        temp_df['high'] = [high.replace(',','')]
        temp_df['low'] = [low.replace(',','')]
        temp_df['volume'] = [volume.replace(',','')]
        temp_df['change_per'] = [change_per.replace(',','')]

        table_data = pd.concat([table_data,temp_df],axis=0,ignore_index=True)

    # print(table_data)

    # table_data.to_csv('Scraped Data/Investing/'+symbol+'.csv',index=False)
    columns_type = {'date': types.DATE,'price':types.FLOAT,'open':types.FLOAT,'high':types.FLOAT,'low':types.FLOAT,'volume':types.VARCHAR(50),'change_per':types.VARCHAR(50)}

    table_data.to_sql(name=symbol,con=engine,if_exists='replace',index=False,chunksize=1000,dtype=columns_type,method='multi')
    engine.dispose()
    
print('Scheduler Completed at' + str(datetime.datetime.now()))