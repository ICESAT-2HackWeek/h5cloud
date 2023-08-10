import boto3
import csv
from io import StringIO
import time
from datetime import datetime
import os
import s3fs
import sys

current = os.path.abspath('..')
sys.path.append(current)
from helpers.links import S3Links

def generate_timestamp():
    return datetime.now().strftime('%Y-%m-%d-%H%M%S')

def timer_decorator(func):
    """
    A decorator to measure the execution time of the wrapped function.
    """
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        # Call the store method here
        s3_key = f"{self.results_directory}/{generate_timestamp()}_{self.name}_{self.data_format}_results.csv"
        self.store(run_time=execution_time, result=result, bucket=self.bucket, s3_key=s3_key)
        return result, execution_time
    return wrapper

class H5Test:
    def __init__(self, data_format: str):
        self.name = self.__class__.__name__
        self.data_format = data_format
        self.files = S3Links().get_links_by_format(data_format)
        self.s3_client = boto3.client('s3')  # Ensure AWS credentials are configured
        self.s3_fs = s3fs.S3FileSystem(anon=False)
        self.bucket = "nasa-cryo-scratch"
        self.results_directory = "h5cloud/benchmark_results"        

    @timer_decorator
    def run(self):
        raise NotImplementedError("The run method has not been implemented")

    def store(self, run_time: float, result: str, bucket: str, s3_key: str):
        """
        Store test results to an S3 bucket as a CSV file.

        :param run_time: The runtime of the test
        :param result: The result of the test
        :param bucket: The name of the S3 bucket where the CSV will be uploaded
        :param s3_key: The S3 key (filename) where the CSV will be stored
        """
        # Create a CSV in-memory
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(['Name', 'Data Format', 'Run Time', 'Result'])  # Headers
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
