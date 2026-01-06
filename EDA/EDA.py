import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
print("DB_NAME:", os.getenv("DB_NAME"))
