import logging
import datetime
import os
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
#import PyPDF2
from requests_html import HTMLSession
import json
import datetime
from datetime import date, timedelta
import re
from commit_secrets import load_secrets


commit_table = load_secrets()

class califorinia_arb:
    
    def __init__(self, access_token):
        self.url = 'https://ww2.arb.ca.gov/resources/documents/weekly-lcfs-credit-transfer-activity-reports'
        self.access_token = access_token
    
    def get_excel_link(self):
        session = HTMLSession()
        response = session.get(self.url)
        links = response.html.absolute_links
        
        for link in links:
            if link.endswith('.xlsx'):
                self.excel_link = link
    
    def weekly_average_credit_price(self):
        self.get_excel_link()
        response = requests.get(self.excel_link)
        excel_file = pd.ExcelFile(response.content)
        sheet_names = excel_file.sheet_names

        df2 = pd.read_excel(excel_file, sheet_name=sheet_names[2])
        df2 = df2.drop(["Unnamed: 9", "Unnamed: 5"], axis=1)
        #df2 = df2[-50:].reset_index(drop=True)
        df2.columns = df2.columns.str.rstrip()

        push_data2_ua(commit_table['LCFS_CA_WeeklyMarketData'],df2, self.access_token)

    def LCFS_credit_transactions(self):
        self.get_excel_link()
        response = requests.get(self.excel_link)
        excel_file = pd.ExcelFile(response.content)
        sheet_names = excel_file.sheet_names

        df1 = pd.read_excel(excel_file, sheet_name=sheet_names[1])
        #df1 = df1[-50:].reset_index(drop=True)
        df1.columns = df1.columns.str.rstrip()

        push_data2_ua(commit_table['LCFS_CA_TradeValueSummary'],df1, self.access_token)

def get_access_token():

    # Create access token for the instance
    request_url = 'https://accounts.zoho.com/oauth/v2/token'
    query_params = {'refresh_token': os.environ.get('refresh_token'),
                    'client_id': os.environ.get('client_id'),
                    'client_secret': os.environ.get('client_secret'),
                    'redirect_uri': 'https://api-console.zoho.com',
                    'grant_type': 'refresh_token'}
    
    response = requests.post(request_url, data=query_params)
    
    if response.status_code != 200:
        raise Exception('Failed to get access token')
    
    logging.info(print(response.json()))
    access_token = response.json()['access_token']
    return access_token


def push_data2(name, data_frame, access_token):
    request_url  = f'https://analyticsapi.zoho.com/restapi/v2/workspaces/572329000038869009/views/{name}/data'
    
    def create_csv_file(data_frame):
        csv_data = data_frame.to_csv(index=False)
        csv_file = io.StringIO(csv_data)
        return csv_file

    csv_file = create_csv_file(data_frame)

    
    payload = {
    'importType':'updateadd',
    'fileType': 'csv',
    'autoIdentify': 'true',
    'onError': 'setcolumnempty',
    'matchingColumns': list(data_frame.columns)
}

    json_payload = json.dumps(payload)
    
    files = {
    'FILE': csv_file
}
    headers = {
    'ZANALYTICS-ORGID': '67178922',
    'Authorization': f'Zoho-oauthtoken {access_token}'
}
    
    response = requests.post(request_url , params={'CONFIG':  json_payload}, headers=headers, files=files)
    logging.info(f'API response: {response.text}')
    

    
def push_data2_ua(name, data_frame, access_token):
    request_url  = f'https://analyticsapi.zoho.com/restapi/v2/workspaces/572329000038869009/views/{name}/data'
    
    def create_csv_file(data_frame):
        csv_data = data_frame.to_csv(index=False)
        csv_file = io.StringIO(csv_data)
        return csv_file

    csv_file = create_csv_file(data_frame)

    
    payload = {
    'importType':'truncateadd',
    'fileType': 'csv',
    'autoIdentify': 'true',
    'onError': 'setcolumnempty'
}

    json_payload = json.dumps(payload)
    
    files = {
    'FILE': csv_file
}
    headers = {
    'ZANALYTICS-ORGID': '67178922',
    'Authorization': f'Zoho-oauthtoken {access_token}'
}
    
    response = requests.post(request_url , params={'CONFIG':  json_payload}, headers=headers, files=files)
    logging.info(f'API response: {response.text}')


def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function ran at %s', datetime.datetime.utcnow().isoformat())
    
    access_token = get_access_token()
    
    carb = califorinia_arb(access_token)
    carb.LCFS_credit_transactions()
    carb.weekly_average_credit_price()
    
    