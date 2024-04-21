import logging
import logging.config
from src.user_tracking_logs.logs_settings.settings import LOGGING_CONFIG_BASE

# import logger
logging.config.dictConfig(LOGGING_CONFIG_BASE)
logger = logging.getLogger('user_tracking_logs')
