import datetime
import logging
import os
import json
import io
from io import StringIO
import requests
import pandas as pd
from commit_secrets import load_secrets
import azure.functions as func

commit_table = load_secrets()

class NOAA:
    def __init__(self, access_token):
        self.access_token = access_token
        
        
    def temperature(self):
        
        regions = {101:"Northeast Region"}
        
        for key, value in regions.items():
            print(key)
            url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/regional/time-series/{key}/tavg/all/12/1895-2023.csv?base_prd=true&begbaseyear=1901&endbaseyear=2000'
            print(url)
            response = requests.get(url)
            csv_data = response.text
            df = pd.read_csv(StringIO(csv_data), skiprows=4)
            df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m').strftime('%b-%Y'))  
            df['region'] = value
            df['Date'] = pd.to_datetime(df['Date'], format = '%b-%Y')
            push_data2_ua(commit_table['noaa_temperature'], df,self.access_token )

    
    def redti(self):
        
        regions = {"Northeast":"climate1","Southeast":"climate4",}
        
        months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
        df_list=[]
        for key, value in regions.items():
            for month in months:   
         
                url =f'https://www.ncei.noaa.gov/access/monitoring/redti/{value}/{month}/1-month/data.csv'
                response = requests.get(url)
                csv_data = response.text
                df = pd.read_csv(StringIO(csv_data))
                headers = df.iloc[0].values
                df.columns = headers
                df.drop(index=0, axis=0, inplace=True)
                df['month'] = month
                df['region'] = key
                df['Date'] = df['month']+'-'+df['Date']
                df['Date'] = pd.to_datetime(df['Date'], format = '%b-%Y')
                df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
  
        push_data2_ua(commit_table['noaa_redti'], combined_df, self.access_token)
        

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
    noaa = NOAA(access_token)
    noaa.redti()
    noaa.temperature()
    
    