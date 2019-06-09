import logging

FORMAT = '%(levelname)s %(asctime)-15s  %(message)s'
logging.basicConfig(format=FORMAT, level="INFO")
logger = logging.getLogger(__name__)