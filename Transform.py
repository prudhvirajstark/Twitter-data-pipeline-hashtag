import os
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
from datetime import datetime,timezone,timedelta,date
import hashlib
import os
from logs import log_error,log_info


todays_date = datetime.now(timezone.utc).astimezone()
today_date_time = todays_date.strftime("%Y%m%d-%H%M%S")

# Define the date range to process
days_ago_range = 1
today = date.today()

try:
    files = []
    for i in range(0,days_ago_range+1):
        date = todays_date - timedelta(days=i)
        date = date.strftime("%Y%m%d")
        files.append(f'ChargeNow_{date}')
except Exception as e:
    log_error('Error Creating list of files to process\n'+str(e),today_date_time)
    raise Exception(e)

try:
    stage_directory = '/home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task/stage'
    df = pd.DataFrame()
    for file in os.listdir(stage_directory):
        for i in files:
            if file.startswith(i):
                new = pd.read_parquet(stage_directory + "/"+file)
                df = pd.concat([df,new],ignore_index=True)
                log_info(f"File {i} loaded to process!",today_date_time)
            else:
                log_info(f"File {i} was not available to process!",today_date_time)
                pass
except Exception as e:
        log_error('Error Creating list of files to process\n'+str(e),today_date_time)
        raise Exception(e)

try:
    # Anonymising the column author_id
    count_row = df.shape[0]
    for i in range (0, count_row):
        idEncoded = str(df['author_id'][i]).encode("utf-8")
        encoded_message = hashlib.md5(idEncoded)
        df['author_id'][i] = encoded_message.digest()

    # Missing Location Null Values
    df['author_location'].fillna("No Location", inplace = True)

    # Renaming the retweets column for appropriate name
    df.rename(columns = {'retweets' : 'is_retweet'}, inplace = True)

    df["is_retweet"] = np.where(df["is_retweet"] == 0, 'NO', 'YES')

    # Creating column with a list of hashtags in the tweet
    df['hash'] = None
    for i in range (0, count_row):
        inner_list = []
        for dic in range(len(df['hashtags'][i])):
            inner_list.append(df['hashtags'][i][dic]['tag'])
        df['hash'][i] = inner_list

    # Creating a new dataframe to explode each hashtag related to a twitter
    df_Hashtags = df[['tweet_id','hash']]
    df_Hashtags = df_Hashtags.explode('hash')

    # # Dopping unecessary columns from df
    df = df.drop(columns=['username','hashtags','hash'])

    df  = df.drop_duplicates(subset=["tweet_id"])
    
    df_Hashtags = df_Hashtags.drop_duplicates()
    # current_directory = os.path.abspath(os.getcwd())
    current_directory = '/home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task'
    charge_now_file_path = current_directory + f'/consumer/ChargeNowData/ChargeNow_{today_date_time}.csv'
    hash_file_path = current_directory + f'/consumer/HashtagsData/Hashtag_{today_date_time}.csv'

    # writting to csv files
    df.to_csv(charge_now_file_path , encoding = 'utf-8-sig')
    df_Hashtags.to_csv(hash_file_path , encoding = 'utf-8')

    log_info(f"Files loaded were sucessfully processed!",today_date_time)
except Exception as e:
    log_error('Files loaded were not processed today \n'+str(e),today_date_time)
    raise Exception(e)