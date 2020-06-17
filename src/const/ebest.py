from datetime import datetime, timedelta

STAND_BY = 0
RECEIVED = 1
SERVER_PORT = 20001
SHOW_CERTIFICATE_ERROR_DIALOG = False
REPEATED_DATA_QUERY = 1
TRANSACTION_REQUEST_EXCESS = -21
TODAY = datetime.now().strftime('%Y%m%d')
YESTERDAY = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
DELAY = 1