import boto3
import polars as pl
from io import StringIO, BytesIO

class S3:

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> None:
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def get_file(self, bucket_name: str, file_key: str) -> pl.DataFrame:
        s3_object = self.client.get_object(Bucket=bucket_name, Key=file_key)

        file_content = s3_object['Body'].read().decode('utf-8')

        return pl.read_csv(StringIO(file_content))
    
    def drop_file(self, file_name: str, bucket_name: str, file_data: pl.DataFrame) -> None:
        csv_buffer = StringIO()

        file_data.write_csv(csv_buffer)

        csv_bytes = BytesIO(csv_buffer.getvalue().encode())

        self.client.upload_fileobj(csv_bytes, bucket_name, file_name)

    def list_files(self, bucket_name: str):
        file_paths = []

        response = self.client.list_objects_v2(Bucket=bucket_name)

        for object in response['Contents']:
            file_path = bucket_name + "/" + object['Key']

            # Only append .csv files
            if file_path[-4:] == '.csv':
                file_paths.append(file_path)

        return file_paths