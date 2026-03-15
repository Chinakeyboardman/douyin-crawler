import os
from dotenv import load_dotenv

load_dotenv()

# Redis configuration (for Celery broker)
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# PostgreSQL configuration
POSTGRES_CONFIG = {
    'host': os.getenv('PGHOST', 'localhost'),
    'port': int(os.getenv('PGPORT', '5432')),
    'database': os.getenv('PGDATABASE', 'douyin'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', 'postgres'),
}

# Schedule configuration
SCHEDULE_HOUR = int(os.getenv('SCHEDULE_HOUR', '8'))  # 8am by default
SCHEDULE_MINUTE = int(os.getenv('SCHEDULE_MINUTE', '0'))

# Batch size for processing
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))

# Download API (Douyin_TikTok_Download_API)
DOWNLOAD_API_BASE_URL = os.getenv('DOWNLOAD_API_BASE_URL', 'http://127.0.0.1:8000')
DOWNLOAD_SAVE_DIR = os.getenv('DOWNLOAD_SAVE_DIR', './downloads')

# WebGemini API (video analysis via chat with attachment)
WEBGEMINI_API_URL = os.getenv('WEBGEMINI_API_URL', 'http://127.0.0.1:8200')

