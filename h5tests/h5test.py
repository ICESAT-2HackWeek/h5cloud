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


import csv
import logging
import os
import pathlib
import re
import time
from datetime import datetime
from io import StringIO

import boto3
import fsspec
import h5py
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from tqdm import tqdm


class RegexFilter(logging.Filter):
    def __init__(self, regex_pattern):
        super(RegexFilter, self).__init__()
        self.regex_pattern = re.compile(regex_pattern)

    def filter(self, record):
        # Apply the regex pattern to the log message
        return not bool(self.regex_pattern.search(record.msg))


def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """

    def fsspec_stats(log_file):
        with open(log_file, "r") as input_file:
            num_requests = 0
            total_requested_bytes = 0
            for line in input_file:
                # Strip leading and trailing whitespaces from the line

                try:
                    read_range = line.split("read:")[1].split(" - ")
                    request_size = int(read_range[1]) - int(read_range[0])
                    total_requested_bytes += request_size
                    num_requests += 1
                except Exception:
                    pass
            stats = {
                "total_reqs": num_requests,
                "total_reqs_bytes": total_requested_bytes,
                "avg_req_size": int(round(total_requested_bytes / num_requests, 2)),
            }
        return stats

    def __setup_logging(self, tstamp):
        pathlib.Path(f"./logs").mkdir(exist_ok=True)
        self.log_filename = f"logs/{self.data_format}-{tstamp}.log"
        logger = logging.getLogger("fsspec")
        logger.setLevel(logging.DEBUG)
        self.regex_filter = RegexFilter(self.logs_regex)
        # add regerx to root logger
        logging.getLogger("fsspec").addFilter(self.regex_filter)
        self._file_handler = logging.FileHandler(self.log_filename)
        self._file_handler.setLevel(logging.DEBUG)
        # Add the handler to the root logger
        logging.getLogger("fsspec").addHandler(self._file_handler)

    def __turnoff_logging(self):
        logging.getLogger("fsspec").removeFilter(self.regex_filter)
        logging.getLogger("fsspec").removeHandler(self._file_handler)
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
        self.io_stats = fsspec_stats(self.log_filename)
        if self.store_results:
            results_key = f"{tstamp}_{self.name}_{self.data_format}_results.csv"
            self.store(run_time=execution_time, result=result, file_name=results_key)
        return result, execution_time, self.log_filename, self.io_stats

    return wrapper


class H5Test:
    def __init__(
        self,
        data_format: str,
        files=[],
        store_results=True,
        logs_regex=r"<File-like object S3FileSystem, .*?>\s*(read: \d+ - \d+)",
    ):
        self.name = self.__class__.__name__
        self.io_stats = {}
        self.log_filename = ""
        self.data_format = data_format
        self.logs_regex = logs_regex
        if len(files) > 0:
            self.files = files
        else:
            raise ValueError("We need at least 1 ATL03 granule URL hosted in S3")

        self.store_results = store_results

        if files[0].startswith("s3://nasa-cryo-persistent"):
            self.s3_client = boto3.client("s3")  #
            self.annon_access = False
            self.results_bucket = "s3://nasa-cryo-persistent/"
            self.results_directory = "h5cloud/benchmark_results"
            self.results_store_type = "S3"
        else:
            self.annon_access = True
            self.results_path = "results"
            pathlib.Path(f"./{self.results_path}").mkdir(exist_ok=True)
            self.results_store_type = "Local"

        self.s3_fs = s3fs.S3FileSystem(anon=self.annon_access)

    @timer_decorator
    def run(self, io_params, dataset, variable):
        raise NotImplementedError("The run method has not been implemented")

    def store(self, run_time: float, result: str, file_name: str):
        """
        Store test results to an S3 bucket as a CSV file.
        :param run_time: The runtime of the test
        :param result: The result of the test
        :param file_name: file to store the results
        """
        # Create a CSV in-memory
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(
            [
                "Name",
                "Data Format",
                "Run Time",
                "Result",
                "Access Log",
                "Total Bytes Tranferred",
                "Total Requests",
            ]
        )  # Headers
        csv_writer.writerow(
            [
                self.name,
                self.data_format,
                run_time,
                result,
                self.log_filename,
                self.io_stats["total_reqs_bytes"],
                self.io_stats["total_reqs"],
            ]
        )

        # Reset the buffer's position to the beginning
        csv_buffer.seek(0)

        # Upload the CSV to S3
        if self.results_store_type == "S3":
            # assumes s3 can write to bucket
            self.s3_client.put_object(
                Bucket=self.results_bucket,
                Key=f"{self.results_directory}/{file_name}",
                Body=csv_buffer.getvalue(),
            )
        else:
            with open(f"{self.results_path}/{file_name}", "w", newline="") as csv_file:
                csv_file.write(csv_buffer.getvalue())


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
