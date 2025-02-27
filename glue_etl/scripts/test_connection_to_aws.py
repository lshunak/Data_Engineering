import boto3
import os
from pathlib import Path

def check_aws_files():
    aws_dir = Path.home() / '.aws'
    credentials_file = aws_dir / 'credentials'
    config_file = aws_dir / 'config'
    
    print("\nChecking AWS files:")
    print(f"AWS directory exists: {aws_dir.exists()}")
    print(f"Credentials file exists: {credentials_file.exists()}")
    print(f"Config file exists: {config_file.exists()}")
    
    if credentials_file.exists():
        print("\nCredentials file permissions:", oct(credentials_file.stat().st_mode)[-3:])
    if config_file.exists():
        print("Config file permissions:", oct(config_file.stat().st_mode)[-3:])

def check_aws_env():
    print("\nAWS Environment Variables:")
    for key in ['AWS_PROFILE', 'AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_SDK_LOAD_CONFIG']:
        print(f"{key}: {os.environ.get(key)}")

def test_aws_connection():
    try:
        session = boto3.Session()
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print("\nAWS Connection Successful!")
        print(f"User ARN: {identity['Arn']}")
        return True
    except Exception as e:
        print(f"\nAWS Connection Error: {str(e)}")
        return False

if __name__ == "__main__":
    check_aws_files()
    check_aws_env()
    test_aws_connection()