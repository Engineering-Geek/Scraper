import boto3
import botocore
import logging
from typing import List


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class S3Bucket:
    """
    A class for interacting with an Amazon S3 bucket.
    
    Args:
        bucket_name (str): The name of the S3 bucket.

    Attributes:
        bucket_name (str): The name of the S3 bucket.
        s3 (boto3.client): The S3 client for low-level operations.
        s3_resource (boto3.resource): The S3 resource for high-level operations.
    """
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        if not self.bucket_exists():
            raise ValueError(f"The specified bucket '{bucket_name}' does not exist.")
    
    def bucket_exists(self):
        """
        Check if the specified S3 bucket exists.

        Returns:
            bool: True if the bucket exists, False otherwise.
        """
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise

    def object_list(self) -> List[dict]:
        """
        List objects in the S3 bucket.

        Returns:
            list: A list of dictionaries representing objects in the bucket.

        Example:
            >>> my_bucket = S3Bucket('my-bucket')
            >>> objects = my_bucket.object_list()
            >>> for obj in objects:
            ...     print(f"Object Key: {obj['Key']}")
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name)
            return response.get('Contents', [])
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error listing objects in {self.bucket_name}: {e}")
            return []

    def upload(self, local_file_path, remote_file_name) -> bool:
        """
        Upload a file to the S3 bucket.

        Args:
            local_file_path (str): The local path to the file to upload.
            remote_file_name (str): The name of the file in the S3 bucket.

        Example:
            >>> my_bucket = S3Bucket('my-bucket')
            >>> my_bucket.upload('local_file.txt', 'remote_file.txt')
        """
        try:
            self.s3.upload_file(local_file_path, self.bucket_name, remote_file_name)
            logger.info(f"File '{local_file_path}' uploaded as '{remote_file_name}' to {self.bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error uploading file to {self.bucket_name}: {e}")
            return False

    def download(self, remote_file_name, local_file_path) -> bool:
        """
        Download a file from the S3 bucket.

        Args:
            remote_file_name (str): The name of the file in the S3 bucket.
            local_file_path (str): The local path where the file will be saved.

        Example:
            >>> my_bucket = S3Bucket('my-bucket')
            >>> my_bucket.download('remote_file.txt', 'local_copy.txt')
        """
        try:
            self.s3.download_file(self.bucket_name, remote_file_name, local_file_path)
            logger.info(f"File '{remote_file_name}' downloaded to '{local_file_path}'")
            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error downloading file from {self.bucket_name}: {e}")
            return False

    def delete(self, file_name: str) -> bool:
        """
        Delete a file from the S3 bucket.

        Args:
            file_name (str): The name of the file to delete from the S3 bucket.

        Example:
            >>> my_bucket = S3Bucket('my-bucket')
            >>> my_bucket.delete('file_to_delete.txt')
        """
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=file_name)
            logger.info(f"File '{file_name}' deleted from {self.bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error deleting file '{file_name}' from {self.bucket_name}: {e}")
            return False


if __name__ == '__main__':
    my_bucket = S3Bucket('market-news-nm')
    my_bucket.upload_file('utils/test.txt', 'test.txt')
    objects = my_bucket.list_objects()
    for obj in objects:
        print(f"Object Key: {obj['Key']}")
