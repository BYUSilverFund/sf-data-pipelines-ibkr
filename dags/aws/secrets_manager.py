import boto3
    
class SecretsManager:

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> None:
        self.client = boto3.client(
            'secretsmanager',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def get_secret(self, secret_id: str):
        return self.client.get_secret_value(SecretId=secret_id)