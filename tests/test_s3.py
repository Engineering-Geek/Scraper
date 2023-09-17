import os
import pandas as pd
import numpy as np

from src.S3 import S3Bucket

# Use a dedicated test bucket for testing
TEST_BUCKET = 'test-debug-nm'
s3 = S3Bucket(TEST_BUCKET)


def test_list():
    objects = s3.object_list()
    assert isinstance(objects, list), "the object_list() method should return a list"


def test_upload_download():
    # Create a test file
    with open('tests/test.txt', 'w') as f:
        f.write("This is a test file.")

    try:
        # Test upload
        result = s3.upload_file('tests/test.txt', 'tests/test.txt')
        assert result, "Something went wrong when executing upload_file"

        # Test download
        result = s3.download('tests/test.txt', 'tests/test2.txt')
        assert result, "Something went wrong when executing download"

        # Compare the contents of the original and downloaded files
        with open('tests/test.txt') as f:
            original = f.readlines()
        with open('tests/test2.txt') as f:
            downloaded = f.readlines()
        assert original == downloaded, "Uploaded and downloaded files do not match"
    finally:
        # Clean up test files
        os.remove('tests/test.txt')
        os.remove('tests/test2.txt')


def test_df():
    original_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    try:
        # Test DataFrame upload and download
        s3.upload_dataframe(df=original_df, remote_filename='tests/test.csv')
        new_df = s3.get_dataframe('tests/test.csv')
        pd.testing.assert_frame_equal(original_df, new_df, check_dtype=False)
    finally:
        # Clean up the test CSV file in S3
        s3.delete('tests/test.csv')


def test_delete():
    # Upload a test file to the S3 bucket
    with open('tests/test_delete.txt', 'w') as f:
        f.write("This file is for testing delete.")
    s3.upload_file('tests/test_delete.txt', 'tests/test_delete.txt')

    # Check if the file exists in the bucket
    objects_before_delete = s3.object_list()
    file_exists_before_delete = any(obj['Key'] == 'tests/test_delete.txt' for obj in objects_before_delete)

    # Delete the file from the S3 bucket
    result = s3.delete('tests/test_delete.txt')

    # Check if the deletion was successful
    objects_after_delete = s3.object_list()
    file_exists_after_delete = any(obj['Key'] == 'tests/test_delete.txt' for obj in objects_after_delete)

    # Clean up the local test file
    os.remove('tests/test_delete.txt')

    # Assert that the file existed before deletion and doesn't exist after deletion
    assert file_exists_before_delete, "The file does not exist before deletion"
    assert not file_exists_after_delete, "The file still exists after deletion"
    assert result, "Something went wrong when executing delete"
