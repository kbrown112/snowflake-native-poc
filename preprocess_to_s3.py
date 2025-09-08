import boto3 
import pandas as pd
from io import StringIO
from datetime import datetime
import numpy as np
import re
import glob 
import os
import time

start_time = time.time()

TARGET_COLUMNS = [
'vendorid',
'tpep_pickup_datetime',
'tpep_dropoff_datetime',
'passenger_count',
'trip_distance',
'pickup_longitude',
'pickup_latitude',
'ratecodeid',
'store_and_fwd_flag',
'dropoff_longitude',
'dropoff_latitude',
'payment_type',
'fare_amount',
'extra',
'mta_tax',
'tip_amount',
'toll_amount',
'improvement_surcharge',
'total_amount',
'congestion_surcharge'
]
COL_MAPPING = {
"VendorID": "vendorid",
"tpep_pickup_datetime": "tpep_pickup_datetime",
"tpep_dropoff_datetime": "tpep_dropoff_datetime",
"passenger_count": "passenger_count",
"trip_distance": "trip_distance",
"RatecodeID": "ratecodeid",
"store_and_fwd_flag": "store_and_fwd_flag",
"payment_type": "payment_type",
"fare_amount": "fare_amount",
"extra": "extra",
"mta_tax": "mta_tax",
"tip_amount": "tip_amount",
"tolls_amount": "toll_amount",
"improvement_surcharge": "improvement_surcharge",
"total_amount": "total_amount",
"congestion_surcharge": "congestion_surcharge"
}
MONTH_MAPPING = {
'01': 'January','02': 'February','03': 'March','04': 'April','05': 'May','06': 'June','07': 'July','08': 'August',
'09': 'September','10': 'October','11': 'November','12': 'December'}

DIRECTORY = 'yellow_tripdata_all/' #replace to correct year
SEARCH = pattern = os.path.join(DIRECTORY, '*.csv')

print('creating connection to s3...')
BUCKET = 'nyc-taxi-data-ctv' 
s3 = boto3.client('s3') 


for local_file_path in glob.glob(SEARCH): # Extract the file name from the full path filename = os.path.basename(filepath) print(filename)

    print(f'reading data... {local_file_path}')

    if ('2020' in local_file_path): #replace to correct year

        df = pd.read_csv(local_file_path)

        PATTERN = r"^.*_(\d{4})-(\d{2}).csv$"
        match = re.match(PATTERN, local_file_path) 
        year = '2020' # replace to match.group(1)
        month_num = match.group(2)
        month = MONTH_MAPPING.get(month_num)

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df.sort_values(by='tpep_dropoff_datetime', inplace=True)
        df.reset_index(drop=True, inplace=True)

        last = datetime.now()
        print(f"simmulating batch upload for {month}/{year}")
        for date in range (1,32): #For every possible date
            print(f"Processing date: {date} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            filtered_df = df[df['tpep_dropoff_datetime'].dt.day == date]
            if filtered_df.empty:
                print(f"No data for date: {date}, skipping...") #data quality check
                continue
            for hour in range(24):
                hour_df = filtered_df[filtered_df['tpep_dropoff_datetime'].dt.hour == hour] #for every possible hour
                if not hour_df.empty:
                    csv_buffer=StringIO() #set up a blank string conversion
                    aws_filepath = f'raw-data/batched-Native-12/{year}_again2/{month}/{date}/{hour}:00_again2.csv' #create the filepath for the s3 bucket
                    hour_df.to_csv(csv_buffer, index = False) #write the hour_df to a csv buffer
                    content = csv_buffer.getvalue() #get relevant content from the csv buffer
                    s3.put_object(Bucket= BUCKET, Body = content, Key = aws_filepath) #upload the content to the s3 bucket
                else:
                    print(f"No data for Day: {date}, Hour: {hour}, skipping...")
            print('Day ' + str(date) + ' took ' + str(datetime.now() - last) + ' to run')
            last = datetime.now()
        print(f"Finished processing {month}/{year} data, and this many rows were uploaded: {len(filtered_df)}")

print("elapsed_time: ", time.time() - start_time)