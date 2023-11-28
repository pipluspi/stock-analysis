from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import sqlalchemy
from sqlalchemy import types 
import datetime
import configparser

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1gOFa57YtfnhEItb6-vG9Ak7clGlLBJXS_Nn7tMwRTYg'

def Sheet1(SAMPLE_RANGE_NAME):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    main_df = pd.DataFrame()
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('/workspace/token.json'):
        creds = Credentials.from_authorized_user_file('/workspace/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/workspace/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('/workspace/token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        for row in values:
            # print(row)
            temp_df = pd.DataFrame()
            temp_df['Date'] = [row[0]]
            temp_df['REC'] = [row[1]]
            temp_df['fwd6'] = [row[2]]
            temp_df['TB3'] = [row[3]]
            temp_df['ntfs'] = [row[4]]
            try :
                temp_df['ntfs_ma'] = [row[5]]
            except :
                temp_df['ntfs_ma'] = ['']
            
            main_df = pd.concat([main_df,temp_df],axis=0)

        # main_df.to_csv('Scraped Data/NTFS/NTFS_1.csv',index=False)

    except HttpError as err:
        print(err)
    
def Sheet2(SAMPLE_RANGE_NAME):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    main_df = pd.DataFrame()
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        for row in values:
            # print(row)
            temp_df = pd.DataFrame()
            temp_df['Date'] = [row[0]]
            temp_df['NTFS'] = [row[1]]
            
            # try :
            #     temp_df['NTFS (90 Day Moving Avg.)'] = [row[2]]
            # except :
            #     temp_df['NTFS (90 Day Moving Avg.)']  = ['']
            
            main_df = pd.concat([main_df,temp_df],axis=0)

        config = configparser.ConfigParser()
        config.read('config.ini')
        url = config.get('development','mysql_connection_string')
        engine = sqlalchemy.create_engine(url)
        columns_type = {'DATE': types.DATE,'NTFS':types.Float}
        main_df.to_sql(name='NTFS',con=engine,if_exists='replace',index=False,dtype=columns_type,method='multi',chunksize=1000)
        engine.dispose()
        
        # print(main_df)
        # main_df.to_csv('NTFS_2.csv',index=False)

    except HttpError as err:
        print(err)

if __name__ == '__main__':
    print('Scheduler Started at' + str(datetime.datetime.now()))
    # Sheet1('Sheet1!A2:F')
    Sheet2('Sheet2!A2:C')
    print('Scheduler Completed at ' + str(datetime.datetime.now())) 
    