import uvicorn
import logging
from src.core.logger import LOGGING

if __name__ == '__main__':
    uvicorn.run(
        'src.main:app',
        host="0.0.0.0",
        port=8000,
        log_config=LOGGING,
        log_level=logging.DEBUG,
        access_log=False
    )
