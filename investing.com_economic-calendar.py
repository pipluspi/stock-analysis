import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
import datetime
import time
import sqlalchemy
from sqlalchemy import types
import datetime as dt
import configparser


SYMBOLS = [ "nfib-small-business-optimism-537"
        ]


print('Scheduler Started at' + str(datetime.datetime.now()))

for symbol in SYMBOLS :
    config = configparser.ConfigParser()
    config.read('config.ini')
    url = config.get('development','mysql_connection_string')
    engine = sqlalchemy.create_engine(url)

    url = "https://in.investing.com/economic-calendar/"+symbol
    scraper = cloudscraper.create_scraper(browser={'browser': 'firefox','platform': 'windows','mobile': False})

    html = scraper.get(url).content

    soup = BeautifulSoup(html, 'html5lib').find("table", {"class": "common-table medium js-table"})


    table_data = pd.DataFrame()

    for table in soup.find_all('tr',{'class' : 'common-table-item'}):

        temp_df = pd.DataFrame()
        td = table.find_all('td')
        date = td[0].find('span').text
        time_stamp = td[1].find('span').text
        actual = td[2].find('span').text
        forecast = td[3].find('span').text
        previous = td[4].find('span').text
        # volume = td[5].find('span').text
        # change_per = td[6].find('span').text
        temp_df['Release Date'] = [datetime.datetime.strftime(datetime.datetime.strptime(date.split('(')[0].strip(), '%b %d, %Y'),'%Y-%m-%d')]
        # temp_df['Date'] = [date]
        # temp_df['time_stamp'] = [time_stamp.replace(',','')]
        temp_df['Actual'] = [float(actual.replace(',',''))]
        temp_df['Date'] = [date.replace(')','').split('(')[1].strip() + '-' + datetime.datetime.strftime(datetime.datetime.strptime(date.split('(')[0].strip(), '%b %d, %Y'),'%Y')]
        # temp_df['forecast'] = [forecast.replace(',','')]
        # temp_df['previous'] = [previous.replace(',','')]

        table_data = pd.concat([table_data,temp_df],axis=0,ignore_index=True)

    print(table_data)

    # table_data.to_csv('Scraped Data/Investing/'+symbol+'.csv',index=False)
    columns_type = {'Release Date': types.DATE,'Date': types.VARCHAR(50),'Actual':types.FLOAT}
    
    table_data.to_sql(name='z_'+symbol.replace('-','_'),con=engine,if_exists='replace',index=False,dtype=columns_type)

    truncate_qury = "delete from z_"+symbol.replace('-','_')+" where Date in (select Date from "+symbol.replace('-','_')+")"
    rs = engine.execute(truncate_qury)
    insert_query = "insert into "+symbol.replace('-','_')+" select * from z_"+symbol.replace('-','_')+""
    rs = engine.execute(insert_query)
    rs = engine.execute("""delete from z_"""+symbol.replace('-','_'))

    engine.dispose()
    
print('Scheduler Completed at' + str(datetime.datetime.now()))