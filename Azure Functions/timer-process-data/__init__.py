
import logging
import datetime
import os
import io
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import PyPDF2
import pandas as pd
from datetime import timedelta
from datetime import datetime as dt
import time
from io import StringIO
'''from selenium import webdriver
#from selenium.webdriver.chrome.options import ChromeOptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager'''
import requests
import json
from bs4 import BeautifulSoup
'''import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from crochet import setup, wait_for
import time
from scrapy import Request, Spider'''
from commit_secrets import load_secrets


commit_table = load_secrets()


'''setup()'''

'''class WisdomTreeSpider(scrapy.Spider):
    name = "wisdomtree"
 
    start_urls = ["https://www.wisdomtree.eu/en-gb/products/ucits-etfs-unleveraged-etps/commodities/wisdomtree-carbon"]
    custom_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, headers={"User-Agent": self.custom_user_agent}, callback=self.parse)

    
    def __init__(self,access_token):
        
        self.access_token = access_token
    
    def parse(self, response):
    
        html_content = response.body
     
        soup =BeautifulSoup(html_content, 'html.parser')
       
    
        div_element = soup.find('table', class_='table table-striped-customized')
        
        data = []
        header_row = []
        for row_index, row in enumerate(div_element.find_all('tr')):
            if row_index == 0: 
                header_row = [th.text for th in row.find_all('th')]
            else:
                td_values = [td.text for td in row.find_all('td')]
                data.append(td_values)

        date = header_row[1]
        print(data)
        print(date)
        transposed_data = list(map(list, zip(*data)))
        print(transposed_data)
        header_row.insert(0, "Date")
       
        df = pd.DataFrame(transposed_data)
        print(df)
        headers = df.iloc[0]
        df.columns = headers
        df.insert(0, "Date", date.strip())
        df =df.loc[:]
        print(df)
      
        push_data('WisdomTree',df,self.access_token)'''
        

        

'''def run_spider(access_token):
   
    crawler = CrawlerRunner()
   
    d = crawler.crawl(WisdomTreeSpider, access_token)
    return d'''

class Yahoo:
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Referer": "https://www.hanetf.com/product/30/fund/sparkchange-physical-carbon-eua-etc",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0",
            "Connection": "keep-alive",
        }
        self.interval = '1d'

    def numeric_date(self, date):
        datetime_obj = dt.strptime(date, '%Y-%m-%d')
        timestamp = int(time.mktime(datetime_obj.timetuple()))
        return timestamp

    def yahoo_data(self, symbol):
        
        date1 = (dt.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        date2 = dt.now().strftime('%Y-%m-%d') 
        base_url = 'https://query1.finance.yahoo.com/v7/finance/download/'
        query_url = f'{base_url}{symbol}=X?period1={self.numeric_date(date1)}&period2={self.numeric_date(date2)}&interval={self.interval}&events=history&includeAdjustedClose=true'
        
        response = requests.get(query_url, headers=self.headers)
        data = response.text
        df = pd.read_csv(StringIO(data))
        df = df[['Date','Open']]
        df.rename(columns = {'Open':'EUR_USD'}, inplace = True)
        query_url2 = f'{base_url}GBPUSD=X?period1={self.numeric_date(date1)}&period2={self.numeric_date(date2)}&interval={self.interval}&events=history&includeAdjustedClose=true'
        response2 = requests.get(query_url2, headers=self.headers)
        data2 = response2.text
        df2 = pd.read_csv(StringIO(data2))
        df2 = df2[['Date','Open']]
        df2.rename(columns = {'Open':'GBP_USD'}, inplace = True)
        df3 = pd.merge(df, df2, on='Date', how='inner')
        push_data2(commit_table[symbol],df3, self.access_token)

class EEX:
    def __init__(self,access_token):
        self.access_token = access_token
        self.current_date = dt.now()
        
    def EUA_futures(self):
        expiration_date = (self.current_date - timedelta(days=0)).strftime("%Y/%m/%d")
        on_date = (self.current_date - timedelta(days=1)).strftime("%Y/%m/%d")
        url = f"https://webservice-eex.gvsi.com/query/json/getChain/gv.pricesymbol/gv.displaydate/gv.expirationdate/tradedatetimegmt/gv.eexdeliverystart/ontradeprice/close/onexchsingletradevolume/onexchtradevolumeeex/offexchtradevolumeeex/openinterest/?optionroot=%22%2FE.FEUA%22&expirationdate={expiration_date}&onDate={on_date}"
    
        headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Origin": "https://www.eex.com",
        "Referer": "https://www.eex.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Not/A)Brand\";v=\"99\", \"Google Chrome\";v=\"115\", \"Chromium\";v=\"115\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
    }
    
        response = requests.get(url, headers=headers)
        data = response.json()
        items_list = data['results']['items']
        df = pd.DataFrame(items_list)
        print(df.columns)
        df.drop(columns=["gv.pricesymbol","gv.displaydate","gv.eexdeliverystart"],axis=1,inplace=True)
        df.rename(columns={
                            'tradedatetimegmt':'Date',
                            'gv.expirationdate': 'Future',
                            'ontradeprice': 'Last Price',
                            'close': 'Settlement Price',
                            'onexchsingletradevolume':'Last Volume',
                            'onexchtradevolumeeex':'Volume Trade Registration',
                            'offexchtradevolumeeex':'Volume Exchange',
                            'openinterest':'Open Interest'
                        },inplace=True)
        
        push_data2(commit_table['EUA_futures'],df, self.access_token)
        
    def EUAA_futures(self):
        expiration_date = (self.current_date - timedelta(days=0)).strftime("%Y/%m/%d")
        on_date = (self.current_date - timedelta(days=1)).strftime("%Y/%m/%d")
        url = f"https://webservice-eex.gvsi.com/query/json/getChain/gv.pricesymbol/gv.displaydate/gv.expirationdate/tradedatetimegmt/gv.eexdeliverystart/ontradeprice/close/onexchsingletradevolume/onexchtradevolumeeex/offexchtradevolumeeex/openinterest/?optionroot=%22%2FE.FEAA%22&expirationdate={expiration_date}&onDate={on_date}"
        headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Origin": "https://www.eex.com",
        "Referer": "https://www.eex.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Not/A)Brand\";v=\"99\", \"Google Chrome\";v=\"115\", \"Chromium\";v=\"115\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
    }
    
        response = requests.get(url, headers=headers)
        data = response.json()
        items_list = data['results']['items']
        df = pd.DataFrame(items_list)
        print(df.columns)
        df.drop(columns=["gv.pricesymbol","gv.displaydate","gv.eexdeliverystart"],axis=1,inplace=True)
        df.rename(columns={
                            'tradedatetimegmt':'Date',
                            'gv.expirationdate': 'Future',
                            'ontradeprice': 'Last Price',
                            'close': 'Settlement Price',
                            'onexchsingletradevolume':'Last Volume',
                            'onexchtradevolumeeex':'Volume Trade Registration',
                            'offexchtradevolumeeex':'Volume Exchange',
                            'openinterest':'Open Interest'
                        },inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%m-%d-%Y')
        
        push_data2(commit_table['EUAA_futures'],df, self.access_token)
    

class NodalFutures:
    def __init__(self):
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Referer": "http://nodalexchange.com/",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        self.url = "http://nodalexchange.com/files/autogenerated/EOD_FUTURES_REPORT.PDF"
        self.connection_string = os.environ.get('ccarbonproddata_STORAGE')
        self.container_name = 'ccarbonproddata_STORAGE'
        self.blob_name = "amerex-raw-files/EOD_FUTURES_REPORT.pdf"
        

    def scrape_data(self):
        
        response = requests.get(self.url, headers=self.headers)
      
        
        cwd = os.getcwd()
       

        with open('EOD_FUTURES_REPORT.pdf', 'wb') as f:
            f.write(response.content)
        
        with open('EOD_FUTURES_REPORT.pdf', 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            first_page = pdf_reader.pages[0].extract_text()
        
        date = first_page.split('\n')[1].strip()
        
        file_name = f'EOD_FUTURES_REPORT_{date}.pdf'
        
        
        with open(file_name, 'wb') as f:
            f.write(response.content)
        
        return file_name

    def upload_to_blob_storage(self, file_name):
        connection_string = os.environ.get('ccarbonproddata_STORAGE')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = "nodal-exchange-eod-futures-report-archives"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        
        with open(file_name, "rb") as data:
            blob_client.upload_blob(data)
        
        logging.info(f"File uploaded: {file_name}")


class KraneShares:
    def __init__(self, access_token) -> None:
        self.access_token = access_token
    
    '''def scrape_data(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')

        ua = UserAgent()
        userAgent = ua.random
        chrome_options.add_argument(f'--user-agent={userAgent}')

        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        url = 'https://kraneshares.com/krbn/'
        driver.get(url)

        table_xpath = "/html/body/div[2]/div[6]/div[1]/div/table"

        table = WebDriverWait(driver, 10).until(EC.visibility_of_element_located(('xpath', table_xpath)))

        row_xpaths = "/html/body/div[2]/div[6]/div[1]/div/table/tbody/tr"
        rows = table.find_elements('xpath', row_xpaths)

        date_xpath = "/html/body/div[2]/div[6]/div[1]/small"
        date_element = driver.find_element('xpath', date_xpath)
        date = date_element.text

        header_row = rows[0]
        header_cells = header_row.find_elements(By.TAG_NAME, 'td')
        headers = [cell.text for cell in header_cells]

        data_rows = []

        # Iterate over the remaining rows
        for row in rows[1:]:
            cell_xpaths = "./td"
            cells = row.find_elements('xpath', cell_xpaths)

            if len(cells) == len(headers):
                data = [cell.text for cell in cells]
                data_rows.append(data)
            else:
                break

        driver.quit()
        df = pd.DataFrame(data_rows, columns=headers)
        df['date'] = date
        df['date'] = df['date'].str.replace('Data as of ', '')

        push_data('KraneShares',df, self.access_token)'''
        
    import requests
from datetime import datetime as dt, timedelta
import pandas as pd

class policy_rates:
    def __init__(self, access_token) -> None:
        self.access_token = access_token

    def dates(self):
        current_date = dt.now().date()
        one_month_ago = current_date - timedelta(days=30)

        current_date_str = current_date.strftime('%Y-%m-%d')
        one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
        return current_date_str, one_month_ago_str

    def data_collection(self, data):
        url = 'https://data.bis.org/api/v0/observations'

        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.post(url, headers=headers, json=data)
        data = response.json()
        df = pd.DataFrame(data['items']).apply(lambda x: x.apply(lambda y: y.get('value') if isinstance(y, dict) else y))

        df = df[['TIME_PERIOD', 'OBS_VALUE']]
        df = df.rename(columns={
            'TIME_PERIOD': 'Date',
            'OBS_VALUE': 'Rate'
        })
        return df

    def us_rates(self):
        data = {
            'timeseries': [{'df_id': 'BIS,WS_CBPOL,1.0', 'series_key': 'D.US'}],
            'filters': [{'field_id': 'TIMESPAN', 'search_term': f'{self.dates()[1]}_{self.dates()[0]}' }]
        }
        df = self.data_collection(data)
        push_data2(commit_table['US_rates'], df, self.access_token)

    def uk_rates(self):
        data = {
            'timeseries': [{'df_id': 'BIS,WS_CBPOL,1.0', 'series_key': 'D.GB'}],
            'filters': [{'field_id': 'TIMESPAN', 'search_term': f'{self.dates()[1]}_{self.dates()[0]}' }]
        }
        df = self.data_collection(data)
        push_data2(commit_table['UK_rates'], df, self.access_token)

    def eu_rates(self):
        data = {
            'timeseries': [{'df_id': 'BIS,WS_CBPOL,1.0', 'series_key': 'D.XM'}],
            'filters': [{'field_id': 'TIMESPAN', 'search_term': f'{self.dates()[1]}_{self.dates()[0]}' }]
        }
        df = self.data_collection(data)
        push_data2(commit_table['EU_rates'], df, self.access_token)

        
        
    def scrape_data(self):
        url = "https://kraneshares.com/krbn/"

        headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
                "Referer": "https://kraneshares.com/krbn/",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table with class "dark-product performance-table" and id "carbon-futures-table"
        table = soup.find('table', class_='dark-product performance-table', id='carbon-futures-table')

        if table:
            # Process the table here
            rows = table.find_all('tr')
            data = []
            row_data = []
            for row in rows:
                # Process each row as needed
                columns = row.find_all('td')
                for column in columns:
                    row_data.append(column.text.strip())
                if len(row_data) == 5:  
                    data.append(row_data)
                    row_data = []

            # Create pandas DataFrame
            df = pd.DataFrame(data)

        df.columns = df.iloc[0]
        df = df[1:]
        date_element = soup.find(class_="as_of")
        date = date_element.get_text()
        df['date'] =date
        df['date'] = df['date'].str.replace('Data as of ', '')
        
        push_data2(commit_table['KraneShares'],df, self.access_token)
        
def CAISO(access_token):
    
    today = dt.today().strftime("%Y%m%d")
    previous_day = (dt.today() - timedelta(days=1)).strftime("%Y%m%d")
    url = f"https://www.caiso.com/outlook/SP/History/{previous_day}/co2.csv?_=1688649017623"
    previous_day = dt.strptime(previous_day, "%Y%m%d")
    previous_day=previous_day.strftime("%m-%d-%Y")

    try:
        df = pd.read_csv(url)
        df = df.iloc[:, 1:]
        result = df.abs().sum() / 12
        final_df = pd.DataFrame([result], columns=result.index)
        final_df.columns = final_df.columns.str.replace(' CO2', '')
        final_df['date'] = previous_day
        print(final_df['date'])
        

    except (pd.errors.ParserError, pd.errors.HTTPError):
        pass  
    
    push_data2(commit_table['caiso'],final_df,access_token)    
      
'''def WisdomTree(access_token):
    chrome_options = Options()
    chrome_options.add_argument('--headless')

    ua = UserAgent()
    userAgent = ua.random
    chrome_options.add_argument(f'--user-agent={userAgent}')

    # Create a Chrome webdriver instance
    driver = webdriver.Chrome(options=chrome_options)

    url = 'https://www.wisdomtree.eu/en-gb/products/ucits-etfs-unleveraged-etps/commodities/wisdomtree-carbon'
    driver.get(url)

    # Perform necessary actions using Selenium
    cookie_accept = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div[2]/div[1]/a[1]').click()
    accept_conditions = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[3]/div/div[2]/div[3]/form/div[2]/div/button' ))).click()
    table_data = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[3]/div[3]/div/div/div[2]/section/div/table' ))).text

    push_data('wisdomtree',table_data,access_token)'''
    
def HanETF(access_token):
    url = "https://www.hanetf.com/product/30/fund/sparkchange-physical-carbon-eua-etc"


    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Referer": "https://www.hanetf.com/product/30/fund/sparkchange-physical-carbon-eua-etc",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0",
        "Connection": "keep-alive",
    }





    response = requests.get(url,headers=headers)

    soup = BeautifulSoup(response.content, 'html.parser')

    div_element = soup.find('div', class_='x6 cols-margin last')

    table_element = div_element.find('table')

    data = []
    header_row = []
    for row_index, row in enumerate(table_element.find_all('tr')):
        if row_index == 0: 
            header_row = [th.text for th in row.find_all('th')]
        else:
            td_values = [td.text for td in row.find_all('td')]
            data.append(td_values)

    date = header_row[1]
    date = date.replace('As of', '').strip()

    transposed_data = list(map(list, zip(*data)))
    header_row.insert(0, "Date")

    df = pd.DataFrame(transposed_data)
    headers = df.iloc[0]
    df.columns = headers  

    df.insert(0, 'Date', date)

    df =df.loc[1:]
    
   
    
    push_data2(commit_table['HanETF'],df,access_token)
    
def BarclaysIpath(access_token):
    url = 'https://ipathetn.barclays/ipath/details/369782'
    response = requests.get(url)
    data = response.text
    json_data = json.loads(data)
    
    df = pd.DataFrame({
    'date': json_data['overview']['asOfDate'],
    'ETN ticker': json_data['overview']['tickerSymbol'],
    'close':json_data['overview']['closingIndicativeNoteValue'],
    'market cap': json_data['overview']['marketCap'],
    'ETN Asset class': json_data['overview']['theme'],
    'ETN outstanding': json_data['overview']['etnsOutstanding']
    
},index=[0])
    push_data2(commit_table['BarclaysIpath'],df,access_token)


def EU_Auctions(access_token):
    url = "https://public.eex-group.com/eex/eua-auction-report/emission-spot-primary-market-auction-report-2023-data.xlsx"
    df = pd.read_excel(url, header=5)
    df = df.iloc[:, 1:]
    df = df[:1]
    push_data2_fpc(commit_table['EU Auctions'],df,access_token)


def EU_1yr_Treasury(access_token):
    url = "https://data.ecb.europa.eu/download-api/csv/wide"
    headers = {
        'Cookie': 'ecb_data_session=1',
    }
    data = {
        'serieskeys[]': 'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y',
    }

    response = requests.post(url, headers=headers, data=data)
    
    df = pd.read_csv(io.StringIO(response.text))
    df = df.drop(df.columns[1], axis=1)
    df.columns = ['Date', 'Rate']
    push_data2(commit_table['EU_1Yr_Treasury'],df,access_token)
    
    
def US_Yield_Rates(access_token):
    current_date = dt.now()
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')

    url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/all/{year}{month}?type=daily_treasury_yield_curve&field_tdr_date_value_month={year}{month}&page&_format=csv"
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text))
    push_data2(commit_table['US_Yield_Rates'],df,access_token)
    

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
        request_url = f'https://analyticsapi.zoho.com/api/phpp@ckinetics.com/CarbonMarkets/{name}'

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
    'matchingColumns': list(data_frame.columns)[:4]
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
    
    kraneshares = KraneShares(access_token)
    kraneshares.scrape_data()
    
    HanETF(access_token)
    
    BarclaysIpath(access_token)
    
    EU_Auctions(access_token)
    
    CAISO(access_token)
    
    yahoo = Yahoo(access_token)
    
    #euro_usd= yahoo.yahoo_data("EURUSD")
    
    eex_futures = EEX(access_token)
    eex_futures.EUA_futures()
    
    US_Yield_Rates(access_token)
    EU_1yr_Treasury(access_token)
    
    PR = policy_rates(access_token)
    PR.eu_rates()
    PR.us_rates()
    PR.uk_rates()
    eex_futures.EUAA_futures()
    #gbp_usd = yahoo.yahoo_data("GBPUSD")
    yahoo.yahoo_data("EURUSD")
    yahoo.yahoo_data("GBPUSD")
    
    '''run_spider(access_token)'''
    
    #nodal_futures = NodalFutures()
    #file_name = nodal_futures.scrape_data()
    #nodal_futures.upload_to_blob_storage(file_name)
    
    #WisdomTreeSpider()
    
    
    
    
    #logging.info(f"File uploaded: {file_name}")
