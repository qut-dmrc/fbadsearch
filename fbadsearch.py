import time
import json
import urllib.request
from random import randint
import logging

import backoff
import requests
from more_itertools import chunked
from requests import HTTPError
from tqdm import tqdm
import math


class FacebookAdSearch:
    def __init__(
            self,
            api_version="v9.0",
            retries=3,
            handle_rate_limits=True,
            resume_page_url=None,
            logger=None,
            **kwargs
    ):
        self.params = kwargs
        if not resume_page_url:
            required_fields = ['ad_reached_countries', 'access_token']
            for field in required_fields:
                assert field in self.params, f"Parameter {field} is required by Facebook."
            assert (
                        'search_term' in self.params or 'search_page_ids' in self.params), \
                        "Either search_term or search_page_ids must be provided"

        self.retries = retries
        self.api_version = api_version
        self.handle_rate_limits = handle_rate_limits
        self.next_page_url = resume_page_url
        self.rate_limit_times_backed_off = 0

        if logger:
            self.logger = logger
        elif not self.logger:
            self.logger = logging.getLogger(__name__)

    def fetch_ads(self):
        # for the first time, just use the endpoint URL and pass the search parameters
        self.logger.debug(f"Starting collection with search params:\n{self.params}")
        self.next_page_url = f'https://graph.facebook.com/{self.api_version}/ads_archive'
        return self._recurse_ad_pages(first_query=True)

    def _recurse_ad_pages(self, first_query=False):
        self.last_error_url = None
        num_retries = 0
        rate_limit_times_backed_off = 0

        MAXIMUM_BACKOFF = 300  # five minutes

        while self.next_page_url is not None:
            if first_query:
                self.logger.debug(f'Fetching first search URL: {self.next_page_url}')
                response = make_api_request_with_backoff(self.next_page_url, self.params)
            else:
                self.logger.debug(f'Fetching URL: {self.next_page_url}')
                response = requests.get(self.next_page_url)
            self.logger.debug(f'HTTP status: {response.status_code}')
            response_data = json.loads(response.text)
            if "error" in response_data:
                if self.next_page_url == self.last_error_url:
                    # failed again
                    if num_retries >= self.retries:
                        error_msg = f"Error fetching URL: [{self.next_page_url}]: {json.dumps(response_data['error'])}"
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
                else:
                    self.last_error_url = self.next_page_url
                    num_retries = 0
                num_retries += 1
                continue

            yield response_data["data"]

            if "paging" in response_data:
                self.next_page_url = response_data["paging"]["next"]
            else:
                self.next_page_url = None

            if self.handle_rate_limits:
                time_wait, ratelimit_percentage = self.__class__.get_rate_limits_from_headers(response.headers)
                self.logger.debug(f"Percentage of rate limit used: {ratelimit_percentage}.")
                if time_wait > 0:
                    self.logger.info(f"Hit FB rate limit; backing off for {time_wait} seconds.")
                    time.sleep(time_wait)
                elif ratelimit_percentage > 95:  # if we are approaching the rate limit, start backing off exponentially
                    sleep_time = min(((2 ^ rate_limit_times_backed_off) + randint(1, 10)), MAXIMUM_BACKOFF)
                    self.rate_limit_times_backed_off += 1
                    self.logger.info(f"Approaching rate limit; backing off for {time_wait} seconds.")
                    time.sleep(sleep_time)
                else: # normal operation again
                    self.rate_limit_times_backed_off = 0



    def generate_ad_archives_resume(self, next_page_url=None):
        """
        Resume when we hit an error
        """
        self.logger.info(f"Resuming collection from: {next_page_url}")
        if next_page_url:
            self.next_page_url = next_page_url
        assert self.next_page_url is not None, 'Cannot resume, no resume URL is stored and none is provided.'
        return self._recurse_ad_pages(first_query=False)

    @staticmethod
    def get_rate_limits_from_headers(headers):
        try:
            fb_usage = headers.__dict__['_store'].get('x-business-use-case-usage')[1]
            fb_usage = json.loads(fb_usage)
            _key = next(iter(fb_usage))
            time_wait = fb_usage[_key][0].get('estimated_time_to_regain_access', 0)
            ratelimit_percentage = fb_usage[_key][0].get('object_count_pct', 0)
            return time_wait, ratelimit_percentage
        except:  # This hasn't really been tested because we haven't come close to the rate limit.
            return 0, 0

@backoff.on_exception(backoff.expo,
                       HTTPError,
                       max_tries=8)
def make_api_request_with_backoff(self, url, params):
    response = requests.get(url, params=params)
    return response

@backoff.on_exception(backoff.expo,
                       urllib.error.HTTPError,
                       max_tries=3)
def save_data(gcs_client, bucket_name, filename, results):
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)

    # save as newline delimited json
    jsondata = '\n'.join(json.dumps(item) for item in results)
    blob.upload_from_string(jsondata)

    uri = f'gs://{bucket_name}/{filename}'
    logger = logging.getLogger(__name__)
    logger.debug(f'Successfully saved to: {uri}')

    return uri


def fetch_and_save_ads(page_ids, access_token, config, gcs_client, gcs_bucket, gcs_dir):
    logger = logging.getLogger('__name__')
    total_steps = math.ceil(len(page_ids) / 10)
    pbar = tqdm(total=total_steps)
    for n, batch in enumerate(chunked(page_ids, 10)):
        pbar.update(1)
        logger.debug(f"Fetching batch {n}.")
        params = {
            'access_token': access_token,
            'ad_type': config['ad_type'],
            'ad_reached_countries': config['ad_reached_countries'],
            'ad_active_status': config['ad_active_status'],
            'ad_delivery_date_min': config['start_date'].strftime('%Y-%m-%d'),
            'ad_delivery_date_max': config['end_date'].strftime('%Y-%m-%d'),
            'search_page_ids': ",".join(batch),
            'fields': ",".join(config['query_fields'])
        }
        gcs_file_path = f'{gcs_dir}/batch_{n}.json'
        adSearch = FacebookAdSearch(logger=logger, **params)
        ads = adSearch.fetch_ads()
        results = []
        try:
            for i, page in enumerate(ads):
                results.extend(page)
        except Exception as e:
            logger.error(f"Hit error: {e}")
        finally:
            if results:
                save_data(gcs_client, gcs_bucket, gcs_file_path, results)
            pbar.close()

    return True


def load_to_bq(bq_client, bq_table, uri):
    logger = logging.getLogger('__name__')
    from google.cloud import bigquery

    table_id = bigquery.Table(bq_table)
    bq_client.create_table(table_id, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        WriteDisposition='WRITE_APPEND'
    )

    logger.info(f'Loading files from {uri} to {bq_table}.')
    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = bq_client.get_table(table_id)
    logger.info(f"Table {bq_table} now contains {destination_table.num_rows} rows.")

    return True


def save_log_file(gcs_client, gcs_bucket, gcs_dir, log_file_name):
    logger = logging.getLogger('__name__')
    bucket = gcs_client.get_bucket(gcs_bucket)
    uri = f'gs://{gcs_bucket}/{gcs_dir}/{log_file_name}'
    blob = bucket.blob(f'{gcs_dir}/{log_file_name}')
    blob.upload_from_filename(log_file_name)
    logger.info(f'Saved log file to {uri}.')