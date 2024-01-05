#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import time

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import cross_val_score
from sklearn.metrics import accuracy_score
from sklearn.model_selection import RandomizedSearchCV
from sklearn.ensemble import RandomForestRegressor

import xgboost as xgb
from xgboost import XGBRegressor

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_pinball_loss, mean_squared_error

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# from skopt import BayesSearchCV
# from skopt.space import Real, Integer, Categorical
import scipy.stats as stats
from sklearn.utils import resample
import openpyxl

from openpyxl.drawing.image import Image
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.chart import BarChart, Reference


# In[2]:


df = pd.read_csv(r'c:\\Users\\Rohit Sai Janga\\Downloads\\VCM ML Model\vcm_model_data.csv')


# In[3]:


df.info()


# In[4]:


df['Vintage Year'] = pd.to_datetime(df['Vintage Year'], errors='coerce')
df['Delivery_Year'] = pd.to_datetime(df['Delivery_Year'], errors='coerce')
df['Date_of_Trade'] = pd.to_datetime(df['Date_of_Trade'], errors='coerce')
df['Vintage Year'] = (df['Vintage Year'].dt.year)
df['Delivery_Year'] = (df['Delivery_Year'].dt.year)


# In[5]:


reference_date = pd.to_datetime("2005-01-01")
df['Date_of_Trade'] = (df['Date_of_Trade'] - reference_date).dt.days + 1


# In[6]:


df = df[df['Protocol_Type'] != 'TBD']
df = df[df['Verification_Standard'] != 'TBD']
df['Region_of_Origin'] = df['Region_of_Origin'].replace('TBD',np.nan)
df['Protocol_Type'] = df['Protocol_Type'].replace('TBD',np.nan)
df['Verification_Standard'] = df['Verification_Standard'].replace('TBD',np.nan)


# In[7]:


df['Price_of_Trade'] = pd.to_numeric(df['Price_of_Trade'], errors='coerce')
df = df.dropna()


# In[8]:


protocol_category_mapping = {
    "Af / Re-Forestation": "Others",
    "Land Use - Avoidance": "Others",
    "Other Tech Solutions": "Others",
    "Cookstove": "Others",
    "Biomass": "Others",
    "Other Greenhouse Gases": "Others",
    "Land Use - Removal": "Others"
}
df['Protocol_Type'] = df['Protocol_Type'].replace(protocol_category_mapping)


# In[9]:


df['Transaction_Volume'] = pd.to_numeric(df['Transaction_Volume'], errors='coerce')
df = df[df['Price_of_Trade'] <= 40]
df = df[df['Transaction_Volume'] <= 1592501]
#df = df[df['Transaction_Volume'] <= 1000001]


# In[10]:


columns_to_encode = ['Protocol_Type','Verification_Standard',
                     'Region_of_Origin','Market Segment']


# In[11]:


df_encoding =pd.get_dummies(df,columns=columns_to_encode)


# In[12]:


numeric_columns = ['Transaction_Volume','Date_of_Trade','Delivery_Year','Vintage Year']
price_column  = ['Price_of_Trade']
print(df)


# In[13]:


scaler = MinMaxScaler(feature_range=(0, 1))
df_encoding[numeric_columns] = scaler.fit_transform(df_encoding[numeric_columns])
price_scaler = MinMaxScaler(feature_range=(0,1))
df_encoding[price_column] = price_scaler.fit_transform(df_encoding[price_column])


# In[14]:


X = df_encoding.drop(['Price_of_Trade'], axis=1)
y = df_encoding['Price_of_Trade']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.03, random_state=1234, stratify=None)


# In[15]:


model = XGBRegressor(
    objective='reg:tweedie', 
    booster='gbtree',
    learning_rate=0.1,              
    max_depth=6,                    
    subsample=0.8,                  
    colsample_bytree=0.6,
    gamma=0.3,
    min_child_weight=5,
    n_estimators=500 ,
    random_state=1234
)


# In[16]:


model.fit(X_train,y_train)


# In[17]:


xgb_params = {
    'objective': 'reg:tweedie', 
    'booster': 'gbtree',
    'learning_rate': 0.1,              
    'max_depth': 6,                    
    'subsample': 0.8,                  
    'colsample_bytree': 0.6,
    'gamma': 0.3,
    'min_child_weight': 5,
    'n_estimators': 500,
    'random_state': 1234
}

num_models = 100

sampling_size = 0.93

models = []

X_train = X_train.reset_index(drop=True)
y_train = y_train.reset_index(drop=True)
for i in range(num_models):
    indices = np.random.choice(len(X_train), size=int(len(X_train) * sampling_size), replace=True)
    model = XGBRegressor(**xgb_params)
    model.fit(X_train.iloc[indices], y_train.iloc[indices])
    models.append(model)
    locals()[f"model_{i+1}"] = model


# In[18]:


importances = model.feature_importances_

feature_importances = pd.DataFrame({'Feature': X_train.columns, 'Importance': importances})
feature_importances = feature_importances.sort_values(by='Importance', ascending=False)

plt.figure(figsize=(10, 10))
sns.barplot(x='Importance', y='Feature', data=feature_importances)
feature_importance_chart = sns.barplot(x='Importance', y='Feature', data=feature_importances)
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.title('Feature Importances')
plt.show()


# In[24]:


data = pd.read_excel(r'c:\\Users\\Rohit Sai Janga\\Downloads\\VCM ML Model\\VCM_Price_Inputs.xlsx')


# In[25]:


data.head()


# In[26]:


data['Vintage Year'] = pd.to_datetime(data['Vintage Year'], errors='coerce')
data['Delivery_Year'] = pd.to_datetime(data['Delivery_Year'], errors='coerce')
data['Date_of_Trade'] = pd.to_datetime(data['Date_of_Trade'], errors='coerce')
data['Vintage Year'] = data['Vintage Year'].dt.year
data['Delivery_Year'] = data['Delivery_Year'].dt.year
data['Date_of_Trade'] = (data['Date_of_Trade'] - pd.to_datetime('2005-01-01')).dt.days

numeric_columns2 = ['Transaction_Volume','Date_of_Trade','Delivery_Year','Vintage Year']


# In[22]:


for index, row in data.iterrows():
    print(row)
    print('------------------')


# In[28]:


for index, row in data.iterrows():
    
    row_subset = row[:8]
    row_dict = row_subset.to_dict()
    
    for col in ['Protocol_Type', 'Verification_Standard', 'Region', 'Market Segment']:
        new_col_name = f"{col}_{row_dict[col]}"
        row_dict[new_col_name] = 1
    
    input_data = pd.DataFrame([row_dict])  

    input_df = pd.DataFrame(input_data)

    input_df[numeric_columns2] = scaler.transform(input_df[numeric_columns2])
    
    missing_columns = set(X_test.columns) - set(input_df.columns)

    for col in missing_columns:
        input_df[col] = 0

    X_test_columns_order = X_test.columns.tolist()
    input_df = input_df[X_test_columns_order]
    
    price = model.predict(input_df)
    price_scale_reshaped = price.reshape(1, -1)
    predicted_prices = price_scaler.inverse_transform(price_scale_reshaped)
    data.at[index, 'Price_of_Trade'] = predicted_prices[0][0]
    #print(input_df)
    print(predicted_prices)
    
    #Insert bootstrap here
    #confidence interval
    predicted_prices = []

    for i, model in enumerate(models):
        predicted_price = model.predict(input_df)
        predicted_prices.append(predicted_price)

        #print(f"Predicted price using model_{i+1}: {predicted_price}")

    predicted_prices = np.array(predicted_prices)

    min_price = np.min(predicted_prices)
    max_price = np.max(predicted_prices)

    predicted_prices = np.array(predicted_prices)
    inverse_scaled_prices = price_scaler.inverse_transform(predicted_prices)

    percentile_5 = np.percentile(inverse_scaled_prices, 5)
    percentile_95 = np.percentile(inverse_scaled_prices, 95)

    min_price = np.min(inverse_scaled_prices)
    max_price = np.max(inverse_scaled_prices)
    
    data.at[index, 'lower_interval'] = min_price
    data.at[index, 'upper_interval'] = max_price

output_sheet_name = 'Output'

    

with pd.ExcelWriter(r'c:\\Users\\Rohit Sai Janga\\Downloads\\VCM ML Model\\VCM_Price_Inputs.xlsx', engine='openpyxl', mode='a') as writer:

    if output_sheet_name in writer.book.sheetnames:

        existing_sheet = writer.book[output_sheet_name]
        start_row = existing_sheet.max_row + 1
        data.to_excel(writer, sheet_name=output_sheet_name, index=False, startrow=start_row)
    else:

        data.to_excel(writer, sheet_name=output_sheet_name, index=False)

    writer.save()
    


# In[ ]:


input_data = {
    'Transaction_Volume': [200],
    'Market Segment_Retail': [1],  

    'Region_of_Origin_North America': [1], 
     
    'Verification_Standard_ACR': [1], 
    'Protocol_Type_Raw_IFM':[1],
   
    'Vintage Year': [2021],
    'Delivery_Year': [2023],
    'Date_of_Trade': [6954]
}


# In[ ]:


numeric_columns2 = ['Transaction_Volume','Date_of_Trade','Delivery_Year','Vintage Year']
input_df = pd.DataFrame(input_data)
input_df[numeric_columns2] = scaler.transform(input_df[numeric_columns2])


# In[ ]:


missing_columns = set(X_test.columns) - set(input_df.columns)

for col in missing_columns:
    input_df[col] = 0
    
X_test_columns_order = X_test.columns.tolist()
input_df = input_df[X_test_columns_order]


# In[ ]:


price = model.predict(input_df)
price_scale_reshaped = price.reshape(1, -1)
predicted_prices = price_scaler.inverse_transform(price_scale_reshaped)
print(predicted_prices[0][0]) 


# In[ ]:

predicted_prices = []

for i, model in enumerate(models):
    predicted_price = model.predict(input_df)
    predicted_prices.append(predicted_price)
    


predicted_prices = np.array(predicted_prices)

min_price = np.min(predicted_prices)
max_price = np.max(predicted_prices)




predicted_prices = np.array(predicted_prices)
inverse_scaled_prices = price_scaler.inverse_transform(predicted_prices)


percentile_5 = np.percentile(inverse_scaled_prices, 5)
percentile_95 = np.percentile(inverse_scaled_prices, 95)

print("5th Percentile Value:", percentile_5)
print("95th Percentile Value:", percentile_95)

min_price = np.min(inverse_scaled_prices)
max_price = np.max(inverse_scaled_prices)

# %%
