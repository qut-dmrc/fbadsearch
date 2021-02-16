import os
import time
from random import randint

from selenium import webdriver
import google.auth
from google.cloud import storage, bigquery
import re
import pandas as pd
import logging

logger = logging.getLogger('fbadsearch')

def bq_get_clients(project_id=None):
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Make clients.
    bqclient = bigquery.Client(
        credentials=credentials,
        project=your_project_id,
    )

    gcs_client = storage.Client(project=project_id)
    return bqclient, gcs_client

def getScreenshots(ad_ids_urls, save_path, gcs_save_path, bucket):
    logger = logging.getLogger(__name__)
    options = webdriver.ChromeOptions()
    options.add_argument('start-maximized')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome('chromedriver', options=options)
    driver.maximize_window()
    driver.set_window_size(1920, 1080)
    for id, url in ad_ids_urls:

        file_name = f"{save_path}/{id}.png"
        if not os.path.exists(file_name):
            driver.get(url)
            driver.get_screenshot_as_file(file_name)

            blob = bucket.blob(f"{gcs_save_path}/{id}.png")
            blob.upload_from_filename(file_name)

            logger.debug(f'Successfully saved to: {blob.path}')
            time.sleep(randint(0,1))



def getAdIDs(sql_query, bqclient):
    df = bqclient.query(sql_query).result().to_dataframe()
    results = list(df.itertuples(index=False))
    logger.info(f"Got {len(results)} results.")
    return results

def getQldPolAds():
    GC_PROJECT = 'platform-analysis'  # @param {type: 'string'}
    GCS_BUCKET = 'election_analysis'  # @param {type: 'string'}
    GCS_DIR = f'election_analysis/datasets/ads'  # @param {type: 'string'}
    BQ_TABLE = 'platform-analysis.electionads.qld2020'  # @param {type: 'string'}

    import datetime
    timestamp = datetime.datetime.utcnow().isoformat()
    gcs_save_path = f'{GCS_DIR}/images/{timestamp}'
    save_path = f"images/{timestamp}"
    os.makedirs(save_path)

    bqclient, gcsclient = bq_get_clients(project_id=GC_PROJECT)
    sql_query = """SELECT id, ad_snapshot_url 
            FROM `platform-analysis.electionads.qld2020`
            GROUP BY id, ad_snapshot_url"""

    adIDs = getAdIDs(sql_query, bqclient)

    bucket = gcsclient.get_bucket(GCS_BUCKET)
    getScreenshots(adIDs, save_path, gcs_save_path, bucket)


if __name__ == '__main__':
    getQldPolAds()
