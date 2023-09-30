import datetime
import logging
import os
import requests
import io
import json
from datetime import date, timedelta
import pandas as pd
import azure.functions as func
from commit_secrets import load_secrets


commit_table = load_secrets()


    
class ICEUKA:
    def __init__(self,access_token):
        self.access_token = access_token
        
    def ice_uka_url(self):
        
        today = date.today()
        friday = today - timedelta(days=7)
        formatted_friday = friday.strftime("%Y%m%d")
        url = f'https://www.ice.com/marketdata/publicdocs/mifid/commitment_of_traders/IFEU_FUT_{formatted_friday}.csv'
        response = requests.post(url)
        df = pd.read_csv(io.StringIO(response.text), sep=',')
        df = df[:-1]
        df =df.replace('.',0)
     
        df['Date_and_time_of_the_publication'] = (pd.to_datetime(df['Date_and_time_of_the_publication'], format='%Y-%m-%dT%H:%M:%S.%fZ')).dt.strftime('%Y-%m-%d')
       
        push_data2(commit_table['UKA_positions'],df,self.access_token)

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

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    access_token = get_access_token()
    ice_uka = ICEUKA(access_token)
    ice_uka.ice_uka_url()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
