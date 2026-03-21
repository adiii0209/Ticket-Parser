import os
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

def get_gmail_service():
    token_response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": os.getenv("GMAIL_CLIENT_ID"),
            "client_secret": os.getenv("GMAIL_CLIENT_SECRET"),
            "refresh_token": os.getenv("GMAIL_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        },
    )
    print(token_response.text)  # ADD THIS
    token_response.raise_for_status()
    access_token = token_response.json()["access_token"]

    creds = Credentials(token=access_token)

    return build("gmail", "v1", credentials=creds)