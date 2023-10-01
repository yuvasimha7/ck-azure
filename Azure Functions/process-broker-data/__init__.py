# author: yuva simha
# written for ckinetics - ccarbon broker sheets data processing
import logging
import azure.functions as func
import datetime as dt
import requests
import tempfile
import pandas as pd
import numpy as np
import os
import io
import re
import time
import datetime
from datetime import datetime
import PyPDF2
from bs4 import BeautifulSoup
import tabula 
import json
import pdfplumber
from azure.storage.blob import BlobServiceClient,BlobClient, ContainerClient
from commit_secrets import load_secrets

access_token = None
expiration_time = 0

# Azure Blob Storage connection string
connection_string = os.environ.get('ccarbonproddata_STORAGE')

# Blob container name
container_name = 'amerex-raw-files'
# File name for storing the access token
access_token_file = 'access_token.txt'

commit_table = load_secrets()

class Amerex:

    def __init__(self, df, access_token):
        self.df = df
        self.access_token = access_token
        
    def process_lcfs(self):
        # Check if file is a LCFS file
        if 'CA PHYSICAL LCFS' in str(self.df.loc[10, 2]):
            lcfs_temp = self.df.loc[11:19, 2:5]
            lcfs_temp.iloc[0:1,0:1] = 'Contract'
            lcfs_temp.columns = lcfs_temp.iloc[0]
            lcfs_temp =lcfs_temp.iloc[1:,:]
            lcfs_temp['Market'] = 'CA PHYSICAL LCFS'

        elif 'CA PHYSICAL LCFS' in str(self.df.loc[11, 2]):
            lcfs_temp = self.df.loc[12:20, 2:6]
            lcfs_temp.iloc[0:1,0:1] = 'Contract'
            lcfs_temp.columns = lcfs_temp.iloc[0]
            lcfs_temp =lcfs_temp.iloc[1:,:]
            lcfs_temp['Market'] = 'CA PHYSICAL LCFS'

        else:
            return


        lcfs_temp.insert(0, 'date', self.get_date())
        lcfs_temp['date'] =lcfs_temp['date'].dt.strftime('%d-%b-%Y')

        lcfs_temp.replace(' ', np.nan, inplace=True)
        lcfs_temp = lcfs_temp.rename(columns={col: col.strip() for col in lcfs_temp.columns})
        lcfs_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        
        lcfs_temp['Bid'] = lcfs_temp['Bid'].astype(float, errors='ignore')
        lcfs_temp['Mid'] = lcfs_temp['Mid'].astype(float, errors='ignore')
        lcfs_temp['Offer'] = lcfs_temp['Offer'].astype(float, errors='ignore')
        
        #push_data('Amerex_LCFS',lcfs_temp,self.access_token)
        push_data2(commit_table['Amerex_LCFS'],lcfs_temp,self.access_token)


    def process_offsets(self):
    
        if 'OFFSETS' in str(self.df.loc[48,8]):
            offsets_temp = self.df.loc[49:56, 8:11]
            offsets_temp.columns = ['Category', 'Non Debs', 'Debs', 'Next Year']
            offsets_temp['Market'] = 'OFFSETS'
           
            
        elif 'OFFSETS' in str(self.df.loc[42,9]):
            offsets_temp = self.df.loc[43:49, 9:12]
            offsets_temp.columns = ['Category', 'Non Debs', 'Debs', 'Next Year']
            offsets_temp['Market'] = 'OFFSETS'
          
        else:
            return
        
        offsets_temp.insert(0,'Date',self.get_date())
        offsets_temp['Date'] = offsets_temp['Date'].dt.strftime('%d-%b-%Y')

        offsets_temp.replace(' ', np.nan, inplace=True)
        offsets_temp = offsets_temp.rename(columns={col: col.strip() for col in offsets_temp.columns})

        offsets_temp.dropna(subset=['Non Debs', 'Debs', 'Next Year'], thresh=1, inplace=True)

        
        
        #push_data('Amerex_CCO',offsets_temp,self.access_token)
        push_data2(commit_table['Amerex_CCO'],offsets_temp,self.access_token)

    def process_cca(self):
        if 'ICE/NODAL CCA' in str(self.df.loc[10,8]):
         
            cca_temp = self.df.loc[11:19, 8:11]
            cca_temp.iloc[0:1,0:1] = 'Contract'
            cca_temp.columns = cca_temp.iloc[0]
            cca_temp =cca_temp.iloc[1:,:]
            cca_temp['Market'] = 'ICE/NODAL CCA'
     
        elif 'ICE/NODAL CCA' in str(self.df.iloc[11,9]):

            cca_temp = self.df.loc[12:20, 9:13]
            cca_temp.iloc[0:1,0:1] = 'Contract'
            cca_temp.columns = cca_temp.iloc[0]
            cca_temp =cca_temp.iloc[1:,:]
            cca_temp['Market'] = 'ICE/NODAL CCA'

        else:
            return
        
        
        cca_temp.insert(0,'date',self.get_date())
        cca_temp['date'] = cca_temp['date'].dt.strftime('%d-%b-%Y')

        cca_temp.replace(' ', np.nan, inplace=True)
        cca_temp = cca_temp.rename(columns={col: col.strip() for col in cca_temp.columns})

        cca_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        cca_temp['Bid'] = cca_temp['Bid'].astype(float, errors='ignore')
        cca_temp['Mid'] = cca_temp['Mid'].astype(float, errors='ignore')
        cca_temp['Offer'] = cca_temp['Offer'].astype(float, errors='ignore')
        
        push_data2(commit_table['Amerex_CCA'],cca_temp,self.access_token)
        #push_data('572329000055069667',cca_temp,self.access_token)
    

    def process_rggi(self):

        if 'ICE/NODAL RGGI' in str(self.df.loc[27,8]):
         
            rggi_temp = self.df.loc[28:34, 8:11]
            rggi_temp.iloc[0:1,0:1] = 'Contract'
            rggi_temp.columns = rggi_temp.iloc[0]
            rggi_temp =rggi_temp.iloc[1:,:]
            rggi_temp['Market'] = 'ICE/NODAL RGGI'
        elif 'ICE/NODAL RGGI' in str(self.df.iloc[28,9]):

            rggi_temp = self.df.loc[29:35, 9:13]
            rggi_temp.iloc[0:1,0:1] = 'Contract'
            rggi_temp.columns = rggi_temp.iloc[0]
            rggi_temp =rggi_temp.iloc[1:,:]
            rggi_temp['Market'] = 'ICE/NODAL RGGI'

        else:
            return
        
        
        rggi_temp.insert(0,'date',self.get_date())
        rggi_temp['date'] = rggi_temp['date'].dt.strftime('%d-%b-%Y')

        rggi_temp.replace(' ', np.nan, inplace=True)
        rggi_temp = rggi_temp.rename(columns={col: col.strip() for col in rggi_temp.columns})
        rggi_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        
        rggi_temp['Bid'] = rggi_temp['Bid'].astype(float, errors='ignore')
        rggi_temp['Mid'] = rggi_temp['Mid'].astype(float, errors='ignore')
        rggi_temp['Offer'] = rggi_temp['Offer'].astype(float, errors='ignore')
        push_data2(commit_table['Amerex_RGGI'],rggi_temp,self.access_token)
        

    def process_wca(self):
        if 'ICE/NODAL WCA' in str(self.df.loc[42,8]):
         
            wca_temp = self.df.loc[43:47, 8:11]
            wca_temp.iloc[0:1,0:1] = 'Contract'
            wca_temp.columns = wca_temp.iloc[0]
            wca_temp =wca_temp.iloc[1:,:]
            wca_temp['Market'] = 'ICE/NODAL WCA'
        else: 
            return

        wca_temp.insert(0,'date',self.get_date())
        wca_temp['date'] = wca_temp['date'].dt.strftime('%d-%b-%Y')

        wca_temp.replace(' ', np.nan, inplace=True)
        wca_temp = wca_temp.rename(columns={col: col.strip() for col in wca_temp.columns})
        wca_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)
        
        wca_temp['Bid'] = wca_temp['Bid'].astype(float, errors='ignore')
        wca_temp['Mid'] = wca_temp['Mid'].astype(float, errors='ignore')
        wca_temp['Offer'] = wca_temp['Offer'].astype(float, errors='ignore')
        push_data2(commit_table['Amerex_WCA'],wca_temp,self.access_token)
        #push_data('572329000055277247',wca_temp,self.access_token)

    def process_vc(self):
        if 'Voluntary Carbon' in str(self.df.loc[45,2]):
            
            if 'NGO' in str(self.df.loc[46,2]):
         
                vc_ngo_temp = self.df.loc[47:50, 2:5]
                vc_ngo_temp.iloc[0:1,0:1] = 'Contract'
                
                vc_ngo_temp.columns = vc_ngo_temp.iloc[0]
                vc_ngo_temp['product'] = 'NGO'
                vc_ngo_temp['market'] = 'Voluntary Carbon'
                vc_ngo_temp =vc_ngo_temp.iloc[1:,:]
                vc_temp = vc_ngo_temp
            else:
                return
                
            if 'GEO' in str(self.df.loc[51,2]):
                vc_geo_temp = self.df.loc[52:55, 2:5]
                vc_geo_temp.iloc[0:1,0:1] = 'Contract'
                
                vc_geo_temp.columns = vc_geo_temp.iloc[0]
                vc_geo_temp['product'] = 'GEO'
                vc_geo_temp['market'] = 'Voluntary Carbon'
                vc_geo_temp =vc_geo_temp.iloc[1:,:]
                vc_temp = pd.concat([vc_temp, vc_geo_temp], ignore_index=True)
            else:
                return
                
            if 'CGEO' in str(self.df.loc[56,2]):
                vc_cgeo_temp = self.df.loc[57:60, 2:5]
                vc_cgeo_temp.iloc[0:1,0:1] = 'Contract'
                
                vc_cgeo_temp.columns = vc_cgeo_temp.iloc[0]
                vc_cgeo_temp['product'] = 'CGEO'
                vc_cgeo_temp['market'] = 'Voluntary Carbon'
                vc_cgeo_temp =vc_cgeo_temp.iloc[1:,:]
                vc_temp = pd.concat([vc_temp, vc_cgeo_temp], ignore_index=True)
            else:
                return
                
        else: 
            return

        vc_temp.insert(0,'date',self.get_date())
        vc_temp['date'] = vc_temp['date'].dt.strftime('%d-%b-%Y')

        vc_temp.replace(' ', np.nan, inplace=True)
        vc_temp = vc_temp.rename(columns={col: col.strip() for col in vc_temp.columns})
        vc_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        
        vc_temp['Bid'] = vc_temp['Bid'].astype(float, errors='ignore')
        vc_temp['Mid'] = vc_temp['Mid'].astype(float, errors='ignore')
        vc_temp['Offer'] = vc_temp['Offer'].astype(float, errors='ignore')
        push_data2(commit_table['Amerex_voluntary_carbon'],vc_temp,self.access_token)
       
    
    def process_OR_LCFS(self):
        if 'OR PHYSICAL LCFS' in str(self.df.loc[33,2]):
         
            or_lcfs_temp = self.df.loc[34:35, 2:5]
            or_lcfs_temp.iloc[0:1,0:1] = 'Contract'
            or_lcfs_temp.columns = or_lcfs_temp.iloc[0]
            or_lcfs_temp['market'] = 'Amerex OR Physcal LCFS'
            or_lcfs_temp =or_lcfs_temp.iloc[1:,:]
        else: 
            return

        or_lcfs_temp.insert(0,'date',self.get_date())
        or_lcfs_temp['date'] = or_lcfs_temp['date'].dt.strftime('%d-%b-%Y')

        or_lcfs_temp.replace(' ', np.nan, inplace=True)
        or_lcfs_temp = or_lcfs_temp.rename(columns={col: col.strip() for col in or_lcfs_temp.columns})
        or_lcfs_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        
        or_lcfs_temp['Bid'] = or_lcfs_temp['Bid'].astype(float, errors='ignore')
        or_lcfs_temp['Mid'] = or_lcfs_temp['Mid'].astype(float, errors='ignore')
        or_lcfs_temp['Offer'] = or_lcfs_temp['Offer'].astype(float, errors='ignore')
        push_data2(commit_table['Amerex_OR_LCFS'],or_lcfs_temp,self.access_token)
        #push_data('572329000055559765',or_lcfs_temp,self.access_token)

    def process_D3_RIN(self):
        if 'D3 RIN' in str(self.df.loc[38,2]):
         
            d3rin_temp = self.df.loc[39:43, 2:5]
            d3rin_temp.iloc[0:1,0:1] = 'Contract'
            d3rin_temp.columns = d3rin_temp.iloc[0]
            d3rin_temp['market'] = 'Amerex D3 RIN'
            d3rin_temp =d3rin_temp.iloc[1:,:]
        else: 
            return

        d3rin_temp.insert(0,'date',self.get_date())
        d3rin_temp['date'] = d3rin_temp['date'].dt.strftime('%d-%b-%Y')

        d3rin_temp.replace(' ', np.nan, inplace=True)
        d3rin_temp = d3rin_temp.rename(columns={col: col.strip() for col in d3rin_temp.columns})
        d3rin_temp.dropna(subset=['Bid', 'Offer', 'Mid'], thresh=1, inplace=True)

        
        d3rin_temp['Bid'] = d3rin_temp['Bid'].astype(float, errors='ignore')
        d3rin_temp['Mid'] = d3rin_temp['Mid'].astype(float, errors='ignore')
        d3rin_temp['Offer'] = d3rin_temp['Offer'].astype(float, errors='ignore')
        push_data2(commit_table['Amerex_D3_RIN'],d3rin_temp,self.access_token)
      



    def get_date(self):
        # Check for date cell here
        # Add more elif statements if they keep on changing
        date = ''
        if pd.notna(self.df.loc[7,2]):
            date = self.df.loc[7,2]
        elif pd.notna(self.df.loc[8,2]):
            date = self.df.loc[8,2]
        else:
            return
        
        return date


class Nodal:
    def __init__(self, df, access_token, blob_name):
        self.df = df
        self.access_token = access_token
        self.blob_name = blob_name

    def process_futures(self):
        nodal_futures_temp = self.df.copy()
        nodal_futures_temp.insert(0, 'date', self.get_date())
        nodal_futures_temp['Market'] = 'NEX_EOD_Futures'
        self.common_processing(nodal_futures_temp)
        nodal_futures_temp['price'] = nodal_futures_temp['price'].astype(float, errors='ignore')
        push_data2(commit_table['nodal_futures'], nodal_futures_temp, self.access_token)

    def process_options(self):
        nodal_options_temp = self.df.copy()
        nodal_options_temp.insert(0, 'date', self.get_date())
        nodal_options_temp['Market'] = 'NEX_EOD_Options'
        self.common_processing(nodal_options_temp)
        nodal_options_temp['price'] = nodal_options_temp['price'].astype(float, errors='ignore')
        nodal_options_temp['strike_price'] = nodal_options_temp['strike_price'].astype(float, errors='ignore')
        push_data2(commit_table['nodal_options'], nodal_options_temp, self.access_token)

    def process_volume(self):
        nodal_volume_temp = self.df.copy()
        nodal_volume_temp.insert(0, 'date', self.get_date())
        nodal_volume_temp['Market'] = 'Nodal OI and Volume'
        nodal_volume_temp.replace(' ', np.nan, inplace=True)
        self.common_processing(nodal_volume_temp)
        nodal_volume_temp['Open Interest'] =nodal_volume_temp['Open Interest'].astype(float,errors='ignore')
        nodal_volume_temp['OI Change'] =nodal_volume_temp['OI Change'].astype(float,errors='ignore')
        nodal_volume_temp['Total Volume'] =nodal_volume_temp['Total Volume'].astype(float,errors='ignore')
        nodal_volume_temp['Block Volume'] =nodal_volume_temp['Block Volume'].astype(float,errors='ignore')
        nodal_volume_temp.dropna(subset=['Open Interest', 'OI Change', 'Total Volume','Block Volume'], thresh=1, inplace=True)
        nodal_volume_temp.dropna(subset=['Code','Product'], thresh=1, inplace=True)
        push_data2(commit_table['nodal_volume'], nodal_volume_temp, self.access_token)

    def common_processing(self, nodal_temp):
        
        nodal_temp.dropna(subset=nodal_temp.columns[2:5], thresh=1, inplace=True)
        nodal_temp.replace(' ', np.nan, inplace=True)
        nodal_temp = nodal_temp.rename(columns={col: col.strip() for col in nodal_temp.columns})

    def get_date(self):
        if 'NEX_EOD' in self.blob_name:
            date_value = re.search(r"\d{4}(\d{2})(\d{2})", self.blob_name)
            if date_value:
                date_str = date_value.group(1) + " " + date_value.group(2) + " " + date_value.group(0)[:4]
                date = dt.datetime.strptime(date_str, "%m %d %Y").strftime("%d %b %Y")
                return date
            
        elif 'Nodal OI' in self.blob_name:
            date_value = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", self.blob_name)
            if date_value:
                date_str = date_value.group(1) + " " + date_value.group(2) + " " + date_value.group(3)[:4]
                date = dt.datetime.strptime(date_str, "%m %d %Y").strftime("%d %b %Y")
                return date


class OPIS:
    def __init__(self, df, access_token, blob_name):
        self.df = df
        self.access_token = access_token
        self.blob_name = blob_name

    def process_opis_carbon_market(self):
        opis_carbon_temp = self.df.copy()
        push_data2(commit_table['OPIS_CarbonMarket_data'],opis_carbon_temp, self.access_token)
        
class ICE:
    def __init__(self, tables, access_token, blob_name):
        self.tables = tables
        self.access_token = access_token
        self.blob_name = blob_name
        
    def ice_sheets(self):
        
        file_name = self.blob_name.split('/')[-1]
        file_name_without_extension = file_name.split('.')[0]
      
        contract_name, date_parts = file_name_without_extension.split('_')[0], file_name_without_extension.split('_')[1:]
       

        date = datetime.strptime('-'.join(date_parts), '%Y-%m-%d').strftime('%d-%m-%Y')
       
        #df = pd.concat(self.tables)
        df =self.tables
        
        

        df = df[df.iloc[:, 0] == contract_name]
                                
        new_column_names = ['Contract Name', 'delivery', 'open', 'high', 'low', 'close','settle_price', 
        'settle_change', 'total_volume', 'OI', 'OI_change','EFP', 'EFS', 'block_volume', 'spread_volume'
                            ]

        df.columns = new_column_names

        df = df.drop(['Contract Name', 'settle_change', 'EFP', 'EFS'], axis=1)
        df['date'] = date
        df['date'] = pd.to_datetime(date, format='%d-%m-%Y')
        cols = list(df.columns)
        cols.insert(0, cols.pop(cols.index('date')))
        df = df[cols]
        print(df.head())
        push_data2_fpc(commit_table[contract_name],df,self.access_token)
        
        

class EVOLUTION:
    def __init__(self,url, access_token, blob_name):
        self.access_token = access_token
        self.blob_name = blob_name
        self.url = url
        
    def cco(self):
        
        match = re.search(r'\d{4}-\d{2}-\d{2}', self.url)
        date = datetime.strptime(match.group(), '%Y-%m-%d').date()
        response = requests.get(self.url)
        html_content = response.text

        soup = BeautifulSoup(html_content, 'html.parser')

        div_element = soup.find('div', class_='view view-evoid-report view-id-evoid_report view-display-id-carbon_offset view-dom-id-1')

        table = div_element.find('table', class_='MsoNormalTable')

        table_data = []
        rows = table.find_all('tr')

        for row in rows[1:-1]:
            row_data = []
            cells = row.find_all('td')
            for cell in cells:
                [sup.extract() for sup in cell.find_all('sup')]
                row_data.append(cell.text.strip())
            table_data.append(row_data)

        df = pd.DataFrame(table_data)
        df=df.rename(columns=df.iloc[0]).drop(df.index[0])
        df['date'] = date
        df.rename(columns={'PRODUCT': 'Contract', 'ASK PRICE': 'Ask','BID PRICE':'Bid' }, inplace=True)
        push_data2(commit_table['Evolution_CCO_link'], df, self.access_token)
    


def get_access_token():
    current_time = time.time()

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob client for the access token file
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=access_token_file)

    try:
        # Check if the access token file exists
        blob_properties = blob_client.get_blob_properties()
        metadata = blob_properties.metadata
        expires_at = float(metadata.get('expires_at'))
        access_token = metadata.get('access_token')

        # Check if the existing access token is still valid
        if expires_at > current_time:
            return access_token

    except Exception as e:
        logging.info(f"Failed to retrieve access token file: {e}")

    # Create a new access token
    request_url = 'https://accounts.zoho.com/oauth/v2/token'
    query_params = {
        'refresh_token': os.environ.get('refresh_token'),
        'client_id': os.environ.get('client_id'),
        'client_secret': os.environ.get('client_secret'),
        'redirect_uri': 'https://api-console.zoho.com',
        'grant_type': 'refresh_token'
    }

    response = requests.post(request_url, data=query_params)

    if response.status_code != 200:
        raise Exception('Failed to get access token')

    response_json = response.json()
    access_token = response_json['access_token']
    expires_in = response_json['expires_in']
    expires_at = current_time + expires_in

    # Save the access token to the blob storage container
    metadata = {
        'access_token': access_token,
        'expires_at': str(expires_at)
    }

    blob_client.upload_blob(data='', metadata=metadata, overwrite=True)

    return access_token



def push_data(name, data_frame, access_token):
        # Push data to comMIT
        # Try keeping table names similar so it is easier to work. if class names comes into picture add f-string for class name
        request_url = f'https://analyticsapi.zoho.com/api/phpp@ckinetics.com/CarbonMarkets/{name}'
        #request_url  = f'https://analyticsapi.zoho.com/restapi/v2/workspaces/572329000038869009/views/{name}/data'

        csv_file=''
        csv_file = io.StringIO()
        data_frame.to_csv(csv_file, index=False)
        csv_file.seek(0)

        payload ={  'ZOHO_ACTION':'IMPORT',
                    'ZOHO_OUTPUT_FORMAT':'XML',
                    'ZOHO_ERROR_FORMAT':'XML',
                    'ZOHO_API_VERSION':'1.0',
                    'ZOHO_IMPORT_TYPE':'UPDATEADD',
                    'ZOHO_AUTO_IDENTIFY':'TRUE',
                    'ZOHO_ON_IMPORT_ERROR':'SETCOLUMNEMPTY',
                    'ZOHO_CREATE_TABLE':'false',
                    'ZOHO_INSERT_NEW':'true',
                    'ZOHO_MATCHING_COLUMNS': ','.join(list(data_frame.columns[:]))
                }

        files = {
            'ZOHO_FILE':('data.csv',csv_file.getvalue(), 'text/csv')

            }


        headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
        

        # Send the HTTP POST request with headers and query parameters
        response = requests.post(request_url, params=payload, headers=headers,  files=files)

        #log output response 
        logging.info(f'API response: {response.text}')
    
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
    
def push_data2_fpc(name, data_frame, access_token):
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
    'matchingColumns': list(data_frame.columns)[:2]
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

def save_archives(connection_string, container_name, file_name, data):
    # Convert the DataFrame to a CSV string
    #csv_string = data.to_csv(index=False)
    # Create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Create or overwrite the blob with the CSV data
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(data, overwrite=True)
    
    
def main(myblob: func.InputStream):

    # Get the name of the blob that triggered the function
    blob_name = myblob.name
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name:{blob_name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # Copy the blob content to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(myblob.read())
        tmp.flush()
    
    # Get access token
    access_token = get_access_token()
    
    #get connection string of ccarbonproddata
    ice_regex_expression = r'^([A-Z]{3}_\d{4}_\d{2}_\d{2}|[A-Z]{2}[0-9]{1}_\d{4}_\d{2}_\d{2}|[A-Z]{1}_\d{4}_\d{2}_\d{2})\.pdf$'
    
    # Check broker sheet
    if "Amerex" in blob_name:
        df = pd.read_excel(tmp.name, header=None)
        amerex = Amerex(df,access_token)
        amerex.process_lcfs()
        amerex.process_offsets()
        amerex.process_cca()
        amerex.process_rggi()
        amerex.process_wca()
        amerex.process_vc()
        amerex.process_OR_LCFS()
        amerex.process_D3_RIN()
        
        #saving the file to amerex-archives folder/contaienr
        container_name = "amerex-archives"
        file_name = blob_name.split('/')[1]
        save_archives(connection_string, container_name, file_name, tmp.name)
        
        #deleting the temporary file in amerex-raw-files
        source_file_name = blob_name.split('/')[1]
        
        source_container_name = blob_name.split('/')[0]
       
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
        
        
    elif "NEX_EOD" in blob_name:
        df = pd.read_csv(tmp.name)
        nodal = Nodal(df,access_token, blob_name)
        if 'FUTURES' in blob_name:
            nodal.process_futures()
        elif 'OPTIONS' in blob_name:
            nodal.process_options()
            
        #saving the file to nodal-archives folder/contaienr
        container_name = "nodal-archives"
        file_name = blob_name.split('/')[1]
        save_archives(connection_string, container_name, file_name, tmp.name)
        
        #deleting the temporary file in amerex-raw-files
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
        

    elif "Nodal" in blob_name:
        df = pd.read_excel(tmp.name)
        nodal = Nodal(df,access_token, blob_name)
        if 'Volume' in blob_name:
            nodal.process_volume()
            
        #saving the file to nodal-archives folder/contaienr
        container_name = "nodal-archives"
        file_name = blob_name.split('/')[1]
        save_archives(connection_string, container_name, file_name, tmp.name)
        
        #deleting the temporary file in amerex-raw-files
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()

    elif "CarbonReport" in blob_name:
        df = pd.read_csv(tmp.name)
        opis = OPIS (df,access_token,blob_name)
        opis.process_opis_carbon_market()
        
        #saving the file to nodal-archives folder/container
        #addaching date to file to make the file name unique
        container_name = "opis-carbon-market-archives"
        opis_date_string  = (df.iloc[0, 1]).replace('/', '-')
        file_name = f'CarbonReport_{opis_date_string}'
        save_archives(connection_string, container_name, file_name, tmp.name)
        
        #deleting temporary file from amerex-raw-files
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
     
    elif "evolutionlink.txt" in blob_name:
        
        with open(tmp.name, "r") as file:
            url = file.read()
        
        evolution = EVOLUTION(url,access_token,blob_name)
        evolution.cco()
        
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
    
    #elif re.match(r'^[A-Z]{3}_\d{4}_\d{2}_\d{2}\.pdf$', blob_name.split('/')[-1]):
    elif re.match (r'^amerex-raw-files/([A-Z]{3}_\d{4}_\d{2}_\d{2}|[A-Z]{2}[0-9]{1}_\d{4}_\d{2}_\d{2}|[A-Z]{1}_\d{4}_\d{2}_\d{2})\.pdf$', blob_name):
    
        
    #elif "DUK" in blob_name:
        #print('TEST SUCCESSFUL')  
        '''file_name = myblob.name.split('/')[-1]
        file_name_without_extension = file_name.split('.')[0]
      
        contract_name, date_parts = file_name_without_extension.split('_')[0], file_name_without_extension.split('_')[1:]
       

        date = datetime.strptime('-'.join(date_parts), '%Y-%m-%d').strftime('%d-%m-%Y')'''
       
        #tables = tabula.read_pdf(tmp.name, lattice=True, pages='all', pandas_options={'header': None})
        
        with pdfplumber.open(tmp.name) as pdf:
            tables = []
            for page in pdf.pages:
                
                # Extract the table using pdfplumber's extract_table method
                table = page.extract_table()

                # If you want to remove the header, you can do so by excluding the first row
                if table:
                    tables.append(table[1:])
                    
        result_df = pd.DataFrame()

        # Iterate through the outermost list and create a DataFrame for each innermost list
        for inner_list in tables:
            inner_df = pd.DataFrame(inner_list)
            
            # Optionally, set column names if needed
            inner_df.columns = inner_df.iloc[0]
            
            # Reset the index to ensure proper indexing
            inner_df = inner_df.iloc[:]
            
            # Concatenate the inner DataFrame with the result DataFrame
            result_df = pd.concat([result_df, inner_df], ignore_index=True)
   
        ice = ICE(result_df, access_token, blob_name)
        ice.ice_sheets()
        
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
        
    elif 'access_token.txt' in blob_name:
        pass
        
    else:
        logging.warning("Blob name does not contain 'Amerex' or 'Nodal'")
        source_file_name = blob_name.split('/')[1]
        source_container_name = blob_name.split('/')[0]
        blob_client = BlobClient.from_connection_string(connection_string, source_container_name, source_file_name)
        blob_client.delete_blob()
