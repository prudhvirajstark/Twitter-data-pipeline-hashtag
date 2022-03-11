# DATA PIPELINE 

This is a basic end to end etl pipeline to search data from a Twitter API v2 end point using python and store it in local folder (Parquet Format), transform this data, then load the output (CSV file) into a other location for Reporting (Power BI) the insights.


### Requirements

-   Python 3
-   Twitter account and API credentials (Access Token from a Twitter app)
-   Apache 
-   Slack Channel
-   Power BI

### Prerequisites

The folder and files structures:
```
│   Dashboard_Report.pbix
│   Extract.py
│   logs.py
│   Presentation.pptx
│   README.md
│   requirements.txt
│   Transform.py
│
├───consumer
│   ├───ChargeNowData
│   │       ChargeNow_20220310-003556.csv
│   │
│   └───HashtagsData
│           Hashtag_20220310-003556.csv
│
├───dags
│   │   twitter_pipeline.py
│   │
│   └───dags_utils
│           slack.py
│
├───Images
│       Airflow_dags.png
│       Dag.png
│       Dashboard_image.png
│       ETL_Architecture.png
│       Extract_1.png
│       Extract_2.png
│       logs.png
│       pipeline_1.png
│       Pipeline_2.png
│       Slack_message_success.png
│       Slack_script.png
│       transform_1.png
│       transform_2.png
│
├───logs
│       log_20220310-003556.txt
│
└───stage
        ChargeNow_20220310-003553.parquet.gzip
```

## Dags
1. Check for the API Availability
2. Search for the tweets with a particular hashtag and extract the data from them and save to local path
3. Perform Transformations and save it into local file
4. Monitor the dags through notifying the SLACK Channel

![alt text](https://github.com/prudhvirajstark/Twitter-data-pipeline-hashtag/blob/master/Images/Airflow_dags.png?raw=true)


## References

1. https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
2. https://docs.tweepy.org/en/stable/client.html


## Author

* **Prudhviraj Panisetti**
