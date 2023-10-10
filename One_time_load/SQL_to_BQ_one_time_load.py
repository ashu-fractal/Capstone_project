import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

#install necessary libraries
install("psycopg2")
install("pg8000")
install("pandas")
install("numpy")
install("pyarrow")  

#import modules
import google.auth
import google.auth.transport.requests

import psycopg2
import pandas as pd
from datetime import datetime as dt
import warnings
warnings.filterwarnings("ignore")
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage


#establishing the connection
conn = psycopg2.connect(
database="mydb", user='root', password='Ashu@123', host='34.172.0.224', port= '5432')

#reading the data from the sql tables into dataframes
consumer_master=pd.read_sql(f'''select * from customer_master''',conn)
product_master=pd.read_sql(f'''select * from product_master''',conn)
order_details = pd.read_sql(f'''select * from order_details''',conn)
order_items = pd.read_sql(f'''select * from order_items''',conn)
consumer_master['pincode'] = pd.to_numeric(consumer_master['pincode'])

#creating dim_order for BigQuery
def insert_dim_order(order_details):
    dim_order = order_details[["orderid","order_status_update_timestamp","order_status"]]
    return dim_order

#creating fact_daily_orders for BigQuery
def insert_fact_daily_orders_func1(consumer_master,order_details):
    #setting the columns for the dataframe
    fact_daily_temp = pd.DataFrame(columns = ['customerid', 'orderid', 'order_received_timestamp', 'order_delivery_timestamp',
                                              'pincode', 'order_amount', 'item_count', 'order_delivery_time_seconds'])
    
    #performing join on necessary tables
    temp = pd.merge(order_details, consumer_master, left_on='customerid', right_on='customerid', how='left')
    temp.sort_values(by=['orderid'])

    #set the order_id
    fact_daily_temp['orderid'] = range(1, 20001)

    #setup indexs for querying
    received_index = 0
    inprogress_index = 1
    delivered_index = 2
    index = 0
    for i in range(0, 60000, 3):

        fact_daily_temp['customerid'][index] = temp['customerid'][i]

        #finding the minimum and maximum timestamp
        a = temp['order_status_update_timestamp'][received_index]
        b = temp['order_status_update_timestamp'][delivered_index]
        c = temp['order_status_update_timestamp'][inprogress_index]
        maxi = max(a, b, c)
        mini = min(a, b, c)

        #assigning the min max values
        fact_daily_temp['order_received_timestamp'][index] = mini
        fact_daily_temp['order_delivery_timestamp'][index] = maxi

        #logic used is to find the mininum timestamp and maximum timestamp out of 3 records

        fact_daily_temp['pincode'][index] = temp['pincode'][i]
        diff = int((maxi-mini).total_seconds())
        fact_daily_temp['order_delivery_time_seconds'][index] = diff

        #incrementing the indexes
        index = index+1
        received_index+=3
        delivered_index+=3
        inprogress_index+=3
    return fact_daily_temp


def fact_daily_orders_func2(temp, product_master, order_items):

    #performing join remaning tables
    temp = pd.merge(temp, order_items, left_on='orderid', right_on='orderid', how='inner')
    temp = pd.merge(temp, product_master, left_on='productid', right_on='productid', how='inner')

    #assigning value to columns
    temp['order_amount'] = temp['rate']
    temp['item_count'] = temp['quantity']

    #dropping the tables that are not necessary
    new_temp = temp.drop(['productcode','productname','isactive','sku','productid', 'quantity', 'rate'],axis=1)
    
    new_temp['customerid'] = pd.to_numeric(new_temp['customerid'])
    new_temp['pincode'] = pd.to_numeric(new_temp['pincode'])
    new_temp['order_delivery_time_seconds'] = pd.to_numeric(new_temp['order_delivery_time_seconds'])
    
    #returning the new dataframe
    return new_temp

#creating the dim_address
def insert_dim_address(consumer_master):
    #generating the address_id
    consumer_master["address_id"] = list(range(1,1001))

    #choosing the desired columns
    dim_address = consumer_master[["address_id","address","city","state","pincode"]]
    dim_address['pincode'] = pd.to_numeric(dim_address['pincode'])
    return dim_address


#creating f_order_details
def insert_f_order_details(order_details, order_items):
    
    #setting up the dataframe
    f_order_details = pd.DataFrame(columns = ["orderid","productid","quantity"])
    f_order_details['orderid'] = order_items['orderid']
    f_order_details['productid'] = order_items['productid']
    f_order_details['quantity'] = order_items['quantity']
    temp = pd.DataFrame(columns = ['orderid', 'order_delivery_timestamp'])

    #performing required join
    temp['orderid'] = list(range(1, 20001))
    index = 0
    for i in range(2, 60000, 3):
        temp['order_delivery_timestamp'][index] = order_details['order_status_update_timestamp'][i]
        index+=1
    f_order_details = pd.merge(f_order_details, temp, left_on='orderid', right_on='orderid', how='left')
    return f_order_details

#creating dim_product
def insert_dim_product(product_master):
    dim_product = product_master

    #product start_date taken as null
    dim_product["start_date"]=np.nan
    dim_product["start_date"]=pd.to_datetime(dim_product["start_date"])

    #product end_date taken as null
    dim_product["end_date"]=np.nan
    dim_product["end_date"]=pd.to_datetime(dim_product["end_date"])
    return dim_product

#crearing the dim_customer
def insert_dim_customer(consumer_master):

    #generating the address_id
    consumer_master["address_id"] = list(range(1,1001))

    #setting up the columns
    dim_customer = consumer_master[["customerid","name","address_id"]]
    dim_customer["start_date"]= (consumer_master['update_timestamp'].dt.date).astype('datetime64[ns]')

    #end_date taken as null
    dim_customer["end_date"] = np.nan
    dim_customer["end_date"]=pd.to_datetime(dim_customer["end_date"])
    return dim_customer



#calling the functions for respective tables
dim_order = insert_dim_order(order_details)
temp = insert_fact_daily_orders_func1(consumer_master,order_details)
fact_daily_orders = fact_daily_orders_func2(temp, product_master, order_items)
dim_customer = insert_dim_customer(consumer_master)
dim_address = insert_dim_address(consumer_master)
dim_product = insert_dim_product(product_master)
f_order_details = insert_f_order_details(order_details, order_items)





# bucket_name = 'ashu_bucket'
# blob_path = 'fractalb-160c7214bb4c.json'
# storage_client = storage.Client()
# bucket = storage_client.get_bucket(bucket_name)
# blob = bucket.blob(blob_path)
# print(type(blob))

# import json

# credentials = service_account.Credentials.from_service_account_file(key)

# client = bigquery.Client(credentials=credentials)
# bucket_name = 'ashu_bucket'
# BUCKET = storage_client.get_bucket(bucket_name)
# client = bigquery.Client.from_service_account_json('ashu_bucket/fractalb-160c7214bb4c.json')


#assigning credentials for the current project 
credentials, project_id = google.auth.default(scopes=['https://storage.cloud.google.com/ashu_bucket/fractalb-160c7214bb4c.json'])
authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
location = 'asia-south2' 

#setting up connection to bigquery client
client = bigquery.Client()

#referring to the tables that we already created in bigquery
tableRef1 = 'fractalb.star_schema.dim_address'
tableRef2 = 'fractalb.star_schema.dim_customer'
tableRef3 = 'fractalb.star_schema.dim_order'
tableRef4 = 'fractalb.star_schema.dim_product'
tableRef5 = 'fractalb.star_schema.f_order_details'
tableRef6 = 'fractalb.star_schema.fact_daily_orders'


#loading the data from dataframe to respective tables
client.load_table_from_dataframe(dim_address,tableRef1)
client.load_table_from_dataframe(dim_customer,tableRef2)
client.load_table_from_dataframe(dim_order,tableRef3)
client.load_table_from_dataframe(dim_product,tableRef4)
client.load_table_from_dataframe(f_order_details,tableRef5)
client.load_table_from_dataframe(fact_daily_orders, tableRef6)