import os

from dotenv import load_dotenv
from logs import log_error,log_info

import tweepy   as tw
import pandas   as pd
from datetime import datetime, timezone, timedelta
import time
import fastparquet

load_dotenv()
BEARER_TOKEN    = os.getenv('BEARER_TOKEN')
client = tw.Client(BEARER_TOKEN, wait_on_rate_limit=True)

# Dates for search query (start and end date)
todays_date = datetime.now(timezone.utc).astimezone()
today_date_time = todays_date.strftime("%Y%m%d-%H%M%S")
start_days = 60
end_days = 0
date_since = todays_date - timedelta(days=start_days)
date_until = todays_date + timedelta(days=end_days) - timedelta(seconds=20)

# Calling the isoformat() function over the start and end's date and time
DATE_SINCE_ISO = date_since.isoformat()
DATE_UNTIL_ISO = date_until.isoformat()
# Connecting with API to do a search based on date and query
try:
    charge_now_tweets = []
    for response in tw.Paginator(client.search_all_tweets,
                                 query = '#ChargeNow',
                                 user_fields = ['id','username','location', 'public_metrics', 'description'],
                                 tweet_fields = ['created_at', 'geo', 'public_metrics', 'entities'],
                                 expansions = 'author_id',
                                 start_time = DATE_SINCE_ISO,
                                 end_time = DATE_UNTIL_ISO,
                                 max_results=50):
        time.sleep(1)
        charge_now_tweets.append(response)
    log_info(f"Hashtag search was sucessfull from {date_since} to {date_until} !",today_date_time)
except Exception as e:
    log_error(e,today_date_time)


try:
    if(response.data != None):
        result = []
        user_dict = {}
        # Loop through each response object
        for temp_data in charge_now_tweets:
            # Take all of the users, and put them into a dictionary of dictionaries with the info we want to keep
            for user in temp_data.includes['users']:
                user_dict[user.id] = {'username': user.username,
                                    'followers': user.public_metrics['followers_count'],
                                    'tweets': user.public_metrics['tweet_count'],
                                    'location': user.location
                                    }
            for tweet in temp_data.data:
                # For each tweet, find the author's information
                author_info = user_dict[tweet.author_id]
                # Put all of the information we want to keep in a single dictionary for each tweet
                result.append({'author_id': tweet.author_id,
                            'tweet_id': tweet.id,
                            'username': author_info['username'],
                            'author_location': author_info['location'],
                            'author_followers': author_info['followers'],
                            'created_at': tweet.created_at,
                            'author_tweets': author_info['tweets'],
                            'hashtags': tweet.entities['hashtags'],
                            'retweets': tweet.public_metrics['retweet_count']
                            })

        # Change this list of dictionaries into a dataframe
        df = pd.DataFrame(result)

        current_directory = '/home/prudhvirajstark/Documents/Repos/DCS_Data_engineer_task'
        file_path = current_directory + f'/stage/ChargeNow_{today_date_time}.parquet.gzip'

        df.to_parquet(file_path, compression='gzip')

        log_info(f"Generated file ChargeNow#{today_date_time} from {date_since} to {date_until} !",today_date_time)
    else:
        log_info(f"No data with #ChargeNow from {date_since} to {date_until} !",today_date_time)
except Exception as exp:
    log_error(exp,today_date_time)