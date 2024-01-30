import csv
import logging
import os
import re
import sys
import time
from datetime import datetime
from io import StringIO

import boto3
import s3fs

current = os.path.abspath("..")
sys.path.append(current)

from helpers.links import S3Links


class RegexFilter(logging.Filter):
    """
    This class will filter a logstream based on a regex expression
    The idea is to target a particular library as they usually have a consistent signature.
    """

    def __init__(self, regex_pattern):
        super(RegexFilter, self).__init__()
        self.regex_pattern = re.compile(regex_pattern)

    def filter(self, record):
        # Apply the regex pattern to the log message
        return not bool(self.regex_pattern.search(record.msg))


def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    It also writes logs to local disk if a regex expression is used in the
    subclass instance.
    """

    def __setup_logging(self, tstamp):
        log_filename = f"logs/{self.data_format}-{tstamp}.log"
        logger = logging.getLogger("fsspec")
        logger.setLevel(logging.DEBUG)
        self.regex_filter = RegexFilter(self.logs_regex)
        # add regerx to root logger
        logging.getLogger().addFilter(self.regex_filter)
        self._file_handler = logging.FileHandler(log_filename)
        self._file_handler.setLevel(logging.DEBUG)
        # Add the handler to the root logger
        logging.getLogger().addHandler(self._file_handler)

    def __turnoff_logging(self):
        logging.getLogger().removeFilter(self.regex_filter)
        logging.getLogger().removeHandler(self._file_handler)
        self._file_handler.close()

    def wrapper(self, *args, **kwargs):
        tstamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        if self.logs_regex:
            __setup_logging(self, tstamp)
        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()
        if self.logs_regex:
            __turnoff_logging(self)
        execution_time = end_time - start_time
        # Call the store method here
        if self.store_results:
            results_key = f"{tstamp}_{self.name}_{self.data_format}_results.csv"
            s3_key = f"{self.results_directory}/{results_key}"
            self.store(
                run_time=execution_time,
                result=result,
                bucket=self.bucket,
                s3_key=s3_key,
            )
        return result, execution_time

    return wrapper


class H5Test:
    def __init__(
        self, data_format: str, files=None, store_results=True, logs_regex=None, anon_access=False, source="cryocloud"
    ):
        self.name = self.__class__.__name__
        self.data_format = data_format
        self.logs_regex = logs_regex
        if files:
            self.files = files
        else:
            if source == "cryocloud":
                links = "../helpers/s3filelinks.json"
            else:
                links = "../helpers/itslivelinks.json"
            self.files = S3Links(links).get_links_by_format(data_format)
        self.s3_fs = s3fs.S3FileSystem(anon=anon_access)
        
        self.store_results = store_results
        self.bucket = "nasa-cryo-persistent"
        self.results_directory = "h5cloud/benchmark_results"

    @timer_decorator
    def run(self, io_params={}):
        """
        When implemented we can pass io_params as runtime tweaks to the underlying
        libraries e.g. fsspec.
        """

        raise NotImplementedError("The run method has not been implemented")

    def store(self, run_time: float, result: str, bucket: str, s3_key: str):
        """
        Store test results to an S3 bucket as a CSV file.

        :param run_time: The runtime of the test
        :param result: The result of the test
        :param bucket: The name of the S3 bucket where the CSV will be uploaded
        :param s3_key: The S3 key (filename) where the CSV will be stored
        """
        self.s3_client = boto3.client("s3")  # Ensure AWS credentials are configured

        # Create a CSV in-memory
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(["Name", "Data Format", "Run Time", "Result"])  # Headers
        csv_writer.writerow([self.name, self.data_format, run_time, result])

        # Reset the buffer's position to the beginning
        csv_buffer.seek(0)

        # Upload the CSV to S3
        self.s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())


## Example subclass
# class SampleTest(H5Test):
#     @timer_decorator
#     def run():
#         # Some logic here...
#         return "Sample Result"

# test_instance = SampleTest(name="SampleTest", format="CSV")
# result, execution_time = test_instance.run()
# print(f"Result: {result}")
# print(f"Execution Time: {execution_time} seconds")

## TODO: Do we also want to include the git commit hash in the filename or csv output?
# import subprocess

# def get_git_commit_hash():
#     try:
#         return subprocess.check_output(['git', 'rev-parse', 'HEAD'], stderr=subprocess.STDOUT).decode('utf-8').strip()
#     except subprocess.CalledProcessError:
#         print("Error retrieving git commit hash. Ensure you're in a git repository or git is installed.")
#         return None

# # Usage:
# commit_hash = get_git_commit_hash()
