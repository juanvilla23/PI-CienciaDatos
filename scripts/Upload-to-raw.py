import boto3
import os
import dotenv
import kagglehub


dotenv.load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
region_name = os.getenv('AWS_REGION')

kaggle_username = os.getenv('KAGGLE_USERNAME')
kaggle_api_token= os.getenv('KAGGLE_API_TOKEN')

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
)

s3 = session.client('s3')
path = kagglehub.competition_download('home-credit-default-risk', output_dir='./data')

directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
directory = os.path.join(directory, 'data')

for file in os.listdir(directory):
    if file.endswith('.csv'):
        file_path = os.path.join(directory, file)
        folder_name = os.path.splitext(file)[0]
        s3_key = 'raw/{}/{}'.format(folder_name, file)
        s3.upload_file(file_path, 'hcdr', s3_key)

print("Done")