import csv
import logging
import os
import pathlib
import sys
import time
from datetime import datetime
from io import StringIO

import boto3
import s3fs

current = os.path.abspath("..")
sys.path.append(current)


def fsspec_logging_decorator(func):
    """
    It will store the fsspec logs inside ./logs and will get some stats from file access
    Will pass values to timer_decorator
    """

    def __setup_logging(self):
        pathlib.Path(f"./logs").mkdir(exist_ok=True)
        logger = logging.getLogger("fsspec")
        logger.setLevel(logging.DEBUG)
        self._file_handler = logging.FileHandler(self.log_filename)
        self._file_handler.setLevel(logging.DEBUG)
        logging.getLogger("fsspec").addHandler(self._file_handler)

    def __turnoff_logging(self):
        [
            logging.getLogger("fsspec").debug(f"FileSize: {size}")
            for size in self.file_sizes
        ]
        logging.getLogger("fsspec").removeHandler(self._file_handler)
        self._file_handler.close()

    def fsspec_stats(log_file):
        stats = None
        with open(log_file, "r") as input_file:
            num_requests = 0
            total_requested_bytes = 0
            for line in input_file:
                try:
                    read_range = line.split("read:")[1].split(" - ")
                    request_size = int(read_range[1]) - int(read_range[0])
                    total_requested_bytes += request_size
                    num_requests += 1
                except Exception:
                    pass
            if total_requested_bytes > 0:
                stats = {
                    "total_reqs": num_requests,
                    "total_reqs_bytes": total_requested_bytes,
                    "avg_req_size": int(round(total_requested_bytes / num_requests, 2)),
                }
        return stats

    def wrapper(self, *args, **kwargs):
        tstamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        self.log_filename = f"logs/{self.data_format}-{tstamp}.log"

        __setup_logging(self)
        result = func(self, *args, **kwargs)
        __turnoff_logging(self)

        self.io_stats = fsspec_stats(self.log_filename)
        return result, {"logs": self.log_filename, "io_stats": self.io_stats}

    return wrapper


def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """

    def wrapper(self, *args, **kwargs):
        tstamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        if "io_params" in kwargs:
            self.runtime_params = kwargs["io_params"]
        if len(args) > 0:
            self.runtime_params = args[0]

        # Call the store method here
        if self.store_results:
            if type(result) in [list, dict, tuple]:
                # unpack
                func_result, _ = result
            else:
                func_result = result
            results_key = f"{tstamp}_{self.name}_{self.data_format}_results.csv"
            self.store(
                run_time=execution_time, result=func_result, file_name=results_key
            )
        return result, {"execution_time": execution_time}

    return wrapper


class H5Test:
    def __init__(
        self,
        data_format: str,
        files=[],
        store_results=True,
    ):
        self.name = self.__class__.__name__
        self.io_stats = None
        self.runtime_params = None
        self.log_filename = ""
        self.data_format = data_format
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
        self.file_sizes = [self.s3_fs.info(file)["size"] for file in self.files]

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
        if self.io_stats:  # if we are using the fsspec logger decorator
            csv_writer.writerow(
                [
                    "Name",
                    "Data Format",
                    "Run Time",
                    "Result",
                    "Runtime Params",
                    "Access Log",
                    "Total Bytes Tranferred",
                    "Total Requests",
                    "Average Request Size",
                ]
            )  # Headers
            csv_writer.writerow(
                [
                    self.name,
                    self.data_format,
                    run_time,
                    result,
                    self.runtime_params,
                    self.log_filename,
                    self.io_stats["total_reqs_bytes"],
                    self.io_stats["total_reqs"],
                    self.io_stats["avg_req_size"],
                ]
            )
        else:
            csv_writer.writerow(
                [
                    "Name",
                    "Data Format",
                    "Run Time",
                    "Result",
                ]
            )  # Headers
            csv_writer.writerow(
                [
                    self.name,
                    self.data_format,
                    run_time,
                    result,
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
