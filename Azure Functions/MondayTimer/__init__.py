
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
from datetime import datetime as dt
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
        df2 = df2[-50:].reset_index(drop=True)
        df2.columns = df2.columns.str.rstrip()

        push_data2('LCFS_CA_WeeklyMarketData',df2, self.access_token)

    def LCFS_credit_transactions(self):
        self.get_excel_link()
        response = requests.get(self.excel_link)
        excel_file = pd.ExcelFile(response.content)
        sheet_names = excel_file.sheet_names

        df1 = pd.read_excel(excel_file, sheet_name=sheet_names[1])
        df1 = df1[-50:].reset_index(drop=True)
        df1.columns = df1.columns.str.rstrip()

        push_data('LCFS_CA_TradeValueSummary',df1, self.access_token)
        
'''class ESMA:
    def __init__(self, access_token):
        self.access_token = access_token
        
    def esma_url(self):
        url = "https://registers.esma.europa.eu/publication/searchRegister/doMainSearch"
        
        today = datetime.date.today()
        date = today -datetime.timedelta(days=10)

        esma_payload = {
            "core": "esma_registers_coder58",
            "pagingSize": "50",
            "start": 0,
            "keyword": "",
            "sortField": "wp_vpcode asc",
            "criteria": [
                {
                    "name": "wp_refdt_parametric_date",
                    "value": f"{date}",
                    "type": "parametricQuery",
                    "isParent": True
                }
            ],
            "wt": "csv"
        }
        return url, esma_payload
        
    def esma_mapping(self):
        name_mapping = {
            'ifi': 'INVEST_FUNDS',
            'ifu': 'INVEST_FUNDS',
            'oth': 'OTHER_FIN_INST',
            'com': 'COMM_UNDER',
            'ope': 'OPER_WITH_COMP_OBL_UNDER_DIR'
        }

        trading_mapping = {
            'lo': 'LONG',
            'sh': 'SHORT'
        }

        position_mapping = {
            0: 'NUM_OF_POS',
            1: 'NUM_OF_POS',
            2: 'NUM_OF_POS',
            3: 'CHNG_SINCE_PREV_REPORT',
            4: 'CHNG_SINCE_PREV_REPORT',
            5: 'CHNG_SINCE_PREV_REPORT',
            6: 'PERC_TOTAL_OPN_INTER',
            7: 'PERC_TOTAL_OPN_INTER',
            8: 'PERC_TOTAL_OPN_INTER',
            9: 'NUM_PERS_HOL_POS'
        }


        risk_mapping = {
            0: 'RISK_REDUC',
            1: 'OTHER',
            2: 'TOTAL',
            3: 'RISK_REDUC',
            4: 'OTHER',
            5: 'TOTAL',
            6: 'RISK_REDUC',
            7: 'OTHER',
            8: 'TOTAL'
        }

        column_mapping = {
            "wp_refdt_0": "Date to which the Weekly Report refers",
            "wp_pubdt_0": "Date and time of publication",
            "tv_mic_0": "Name of Trading Venue",
            "wp_sts_0": "Report Status",
            "wp_vpcode_0": "Venue product code",
            "wp_cednm_0": "Name of Commodity Derivative Contract Emission Allowance or Derivative thereof",
            "wp_munit_0": "NOTATION OF THE POSITION QUANTITY"
            }
        
        return name_mapping, risk_mapping, trading_mapping,position_mapping,column_mapping'''

    
class ESMA:
        def __init__(self, access_token):
            self.access_token = access_token
            
        def esma_url(self):
            url = "https://registers.esma.europa.eu/publication/searchRegister/doMainSearch"
            
            today = date.today()
            friday = today - timedelta(days=10)
            print(friday)

            esma_payload = {
                "core": "esma_registers_coder58",
                "pagingSize": "50",
                "start": 0,
                "keyword": "",
                "sortField": "wp_vpcode asc",
                "criteria": [
                    {
                        "name": "wp_refdt_parametric_date",
                        "value": f"{friday}",
                        "type": "parametricQuery",
                        "isParent": True
                    }
                ],
                "wt": "csv"
            }
            return url, esma_payload
            
        def esma_mapping(self):
            name_mapping = {
                'ifi': 'INVST_FIRMS',
                'ifu': 'INVST_FUNDS',
                'oth': 'OTHR_FIN_INST',
                'com': 'COMM_UNDER',
                'ope': 'OPER_WITH_COMP_OBL_UNDER_DIR'
            }

            trading_mapping = {
                'lo': 'LONG',
                'sh': 'SHORT'
            }

            position_mapping = {
                0: 'NUM_OF_POS',
                1: 'NUM_OF_POS',
                2: 'NUM_OF_POS',
                3: 'CHNG_SINCE_PREV_REPORT',
                4: 'CHNG_SINCE_PREV_REPORT',
                5: 'CHNG_SINCE_PREV_REPORT',
                6: 'PERC_TOTAL_OPN_INTER',
                7: 'PERC_TOTAL_OPN_INTER',
                8: 'PERC_TOTAL_OPN_INTER',
                9: 'NUM_PERS_HOL_POS'
            }


            risk_mapping = {
                0: 'RISK_REDUC',
                1: 'OTHER',
                2: 'TOTAL',
                3: 'RISK_REDUC',
                4: 'OTHER',
                5: 'TOTAL',
                6: 'RISK_REDUC',
                7: 'OTHER',
                8: 'TOTAL'
            }

            column_mapping = {
                "wp_refdt_0": "Date to which the Weekly Report refers",
                "wp_pubdt_0": "Date and time of publication",
                "tv_name_0": "Name of Trading Venue",
                "wp_sts_0": "Report Status",
                "wp_vpcode_0": "Venue product code",
                "wp_cednm_0": "Name of Commodity Derivative Contract Emission Allowance or Derivative thereof",
                "wp_munit_0": "NOTATION OF THE POSITION QUANTITY",
                "tv_mic_0": "Trading Venue Identifier"
                }
            
            return name_mapping, risk_mapping, trading_mapping,position_mapping,column_mapping

        
        def esma_data_extraction(self):
            url, payload = self.esma_url()
            name_mapping, risk_mapping, trading_mapping,position_mapping,column_mapping = self.esma_mapping()
            response = requests.post(url, json=payload)
            df1 = pd.read_csv(io.StringIO(response.text), header=None, sep=',')

            df1.columns = df1.iloc[0]  
            df1 = df1[1:]  
            df1.reset_index(drop=True, inplace=True) 
            
            wp_pos_name_values = df1["wp_pos_name"].unique()
            wp_pos_cat_values = df1["wp_pos_cat"].unique()

            df1 = df1.drop(columns=["wp_pos_name", "wp_pos_cat"])

            for column in df1.columns:
                df1[column] = df1[column].str.split(',')
                df1 = df1.join(df1[column].apply(pd.Series).add_prefix(column + '_'))
                df1 = df1.drop(column, axis=1)

   

            columns_to_rename = [col for col in df1.columns if col.startswith('wp_') and col.count('_') == 3 and col[-1].isdigit()]


            for col in columns_to_rename:
                
                keys = col.split('_')[1:]

                name_value = name_mapping.get(keys[0], '')
                trading_value = trading_mapping.get(keys[1], '')
                position_risk_key = int(keys[2])
                position_value = position_mapping.get(position_risk_key, '')
                risk_value = risk_mapping.get(position_risk_key, '')

                new_col = f"{position_value}-{risk_value}-{name_value}-{trading_value}"
                new_col = new_col.replace("--","-")
                if position_value == "NUM_PERS_HOL_POS":
                    new_col = new_col.replace("-LONG","")
                    new_col = new_col.replace("-SHORT","")
                df1.rename(columns={col: new_col}, inplace=True)




            df1 = df1.rename(columns=column_mapping)
                

                
            columns_to_drop = ["_version__0","_root__0", "entity_type_0", "type_s_0", "timestamp_0", "time_lastUpdate_0", "id_0"]
            df1 = df1.drop(columns=columns_to_drop)

            df1.columns = df1.columns.str.strip()
            df1['Date and time of publication'] = (pd.to_datetime(df1['Date and time of publication'], format='%Y-%m-%dT%H:%M:%SZ')).dt.strftime('%Y-%m-%d')
            df1['Date to which the Weekly Report refers'] =  (pd.to_datetime(df1['Date to which the Weekly Report refers'], format='%Y-%m-%dT%H:%M:%SZ')).dt.strftime('%Y-%m-%d')
            
                    
            push_data2(commit_table['esma_commodity_positions'],df1,self.access_token)
        
    
class CFTC:
    def __init__(self,access_token):
        self.access_token = access_token
        self.url = 'https://www.cftc.gov/dea/newcot/f_disagg.txt'
        
    def scrape_data(self):
        response = requests.get(self.url)
        df = pd.read_csv(io.StringIO(response.text),header =None, sep=',')
        df.columns = ['column'] + ['column' + str(i) for i in range(1, len(df.columns))]

        df = df.iloc[1:].reset_index(drop=True)
        
        push_data2(commit_table['CFTC'],df,self.access_token)
        
def UK_guilt(access_token):
    
    today = dt.today()
    current_weekday = today.weekday()
    last_sunday = today - timedelta(days=current_weekday + 1)
    previous_sunday = last_sunday - timedelta(days=7)
    last_sunday_str = last_sunday.strftime("%m/%d/%Y")
    previous_sunday_str = previous_sunday.strftime("%m/%d/%Y")
    
    custom_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    }
    url = f"https://www.wsj.com/market-data/quotes/bond/BX/TMBMKGB-01Y/historical-prices/download?MOD_VIEW=page&num_rows=10&range_days=10&startDate={previous_sunday_str}&endDate={last_sunday_str}"

    response = requests.get(url, headers=custom_headers)
    df = pd.read_csv(io.StringIO(response.text))
    df.rename(columns=lambda x: x.strip(), inplace=True)
    
    push_data2(commit_table['UK_Guilt'],df,access_token)
    
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

def push_data(name, data_frame, access_token):
        # Push data to comMIT
        # Try keeping table names similar so it is easier to work. if class names comes into picture add f-string for class name
        request_url = f'https://analyticsapi.zoho.com/api/phpp@ckinetics.com/Learning/{name}'

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
    



def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function ran at %s', datetime.datetime.utcnow().isoformat())
    
    access_token = get_access_token()
    
    '''carb = califorinia_arb(access_token)
    carb.LCFS_credit_transactions()
    carb.weekly_average_credit_price()'''
    
    esma = ESMA(access_token)
    esma.esma_data_extraction()
    
    cftc = CFTC(access_token)
    cftc.scrape_data()
    UK_guilt(access_token)
        
