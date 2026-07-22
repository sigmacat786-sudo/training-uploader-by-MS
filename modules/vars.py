#MS BRO
# Add your details here and then deploy by clicking on HEROKU Deploy button
import os
from os import environ

API_ID = int(environ.get("API_ID", "38498066"))
API_HASH = environ.get("API_HASH", "c9696114751feacdeb1b4487f5839a1a")
BOT_TOKEN = environ.get("BOT_TOKEN", "")
DEFAULT_THUMB_URL = "https://graph.org/file/43961a18cb08375a57113-9049653bd8fd8a2b6a.jpg"
THUMB_PATH = "pdf_thumb_v2.jpg"
OWNER = int(environ.get("OWNER", "8909902924"))
CREDIT = environ.get("CREDIT", " @SmartBoy_ApnaMS")
cookies_file_path = os.getenv("cookies_file_path", "youtube_cookies.txt")

TOTAL_USER = os.environ.get('TOTAL_USERS', '8902042822,8715662594,8909902924,8838086114').split(',')
TOTAL_USERS = [int(user_id) for user_id in TOTAL_USER]

AUTH_USER = os.environ.get('AUTH_USERS', '8902042822,8715662594,8909902924,8838086114').split(',')
AUTH_USERS = [int(user_id) for user_id in AUTH_USER]
if int(OWNER) not in AUTH_USERS:
    AUTH_USERS.append(int(OWNER))

