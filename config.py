import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    GEMINI_API = os.environ.get("GEMINI_API_KEY", "")