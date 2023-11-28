import requests
from urllib.parse import unquote
import pandas as pd
import sqlalchemy
from sqlalchemy import types 
import datetime
import configparser

print('Scheduler Started at' + str(datetime.datetime.now()))
config = configparser.ConfigParser()
config.read('config.ini')
url = config.get('development','mysql_connection_string')
SYMBOL_LIST = ['$MMOH','$ADVN','$DECN','$ADRN','$AVRN','$HIGN','$LOWN','$MMTH','$AVVN','$DVCN']


engine = sqlalchemy.create_engine(url)

for symbol in  SYMBOL_LIST :
    # print(symbol)
    # geturl=r'https://www.barchart.com/stocks/quotes/AAPL%7C20210423%7C126.00C/price-history/'
    geturl=r'https://www.barchart.com/stocks/quotes/'+symbol+'/price-history/historical'
    apiurl=r'https://www.barchart.com/proxies/core-api/v1/quotes/get'

    getheaders={

        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        "referer":"https://www.barchart.com/stocks/quotes/"+symbol+"%7C20210423%7C126.00C/price-history/historical",
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
        }
    s=requests.Session()
    r=s.get(geturl, headers=getheaders)

    headers={
        'accept': 'application/json',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',

        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36",
        'x-xsrf-token': unquote(unquote(s.cookies.get_dict()['XSRF-TOKEN']))
    }

    payload={

        "symbol":symbol,
        "fields":"tradeTime.format(m\/d\/Y),openPrice,highPrice,lowPrice,lastPrice,priceChange,percentChange,volume,openInterest,impliedVolatility,symbolCode,symbolType",
        "type":"eod",
        "orderBy":"tradeTime",
        "orderDir":"desc",
        "limit":65,
        #"meta":"field.shortName,field.type,field.description",
        'raw': '1'


    }

    
    r=s.get(apiurl,params=payload,headers=headers)
    j=r.json()
    df = pd.json_normalize(j['data']).filter(['tradeTime','openPrice','highPrice','lowPrice','lastPrice','priceChange','percentChange','volume'])

    df['openPrice'] = df['openPrice'].str.replace(',','')
    df['highPrice'] = df['highPrice'].str.replace(',','')
    df['lowPrice'] = df['lowPrice'].str.replace(',','')
    df['lastPrice'] = df['lastPrice'].str.replace(',','')
    df['priceChange'] = df['priceChange'].str.replace(',','').replace('unch','0')
    df['percentChange'] = df['percentChange'].str.replace(',','').replace('unch','0')
    df['volume'] = df['volume'].str.replace(',','')

    df = df.rename(columns={'tradeTime':'Time','openPrice':'Open','highPrice':'High','lowPrice':'Low','lastPrice':'Last','priceChange':'Change','percentChange':'%Chg','volume':'Volume'})
    df['Time'] = df['Time'].astype('datetime64[D]')
    print(df.head())
    

    columns_type = {'Time': types.DATE,'Open':types.FLOAT,'High':types.FLOAT,'Low':types.FLOAT,'Last':types.FLOAT,'Change':types.VARCHAR(50),'%Chg':types.VARCHAR(50),'Volume':types.VARCHAR(50)}

    df.to_sql(name='z_bar_'+symbol,con=engine,if_exists='replace',index=False,dtype=columns_type)
    
    truncate_qury = "delete from z_bar_"+symbol+" where Time in (select Time from bar_"+symbol+")"
    rs = engine.execute(truncate_qury)
    insert_query = "insert into bar_"+symbol+" select * from z_bar_"+symbol+""
    rs = engine.execute(insert_query)
    rs = engine.execute("""delete from z_bar_"""+symbol)

print('Scheduler Completed at ' + str(datetime.datetime.now()))  
    