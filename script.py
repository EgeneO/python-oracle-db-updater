import oracledb
import csv
from datetime import datetime
from threading import Timer

# *************************************************** #
#                 Configurable Variables              #
# *************************************************** #
# DB details
# If SSL/TLS is enabled on the DB, the path to the wallet/truststore containing the required cert and its
# password (if encrypted) need to be provided. wallet_location is the path to the directory containing an
# wallet/truststore file. Wallet file should be named ewallet.pem and should not be included wallet_location
# path string. If wallet and/or wallet password is not required, set to None (without ""). For AWS RDS you
# can download the bundle containing the DBs CA Cert from
# https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html and rename the file.
dsn = "<url template>"
username = "<username>"
password = "<password>"
wallet_location = "<wallet/truststore location>"
wallet_password = "<wallet password>"

# Input CSV file
csv_file_location = "<csv file path>"
skip_header_row = True
csv_file_delimiter = ","
csv_file_quotechar = '"'

# Logging
# If logging to file is enabled, logging file will be created if it doesnt exist. Logs are appended to the 
# end of the file.
log_file_location = "<log file path>"
enable_logging_file = True
enable_logging_stdout = False

# DB and CSV columns
# csv_id_col_no and csv_update_col_no values start from zero
db_table_name = "<db table name>"
db_id_col_name = "<db table id column>"
db_update_col_name = "<db table column being update>"
csv_id_col_no = <csv file id column index>
csv_update_col_no = <csv file index of column containing new values>

# *************************************************** #

  
class Metrics:
    def __init__(self):
        self.success = 0
        self.failed = 0
        self.skipped = 0
      
    def __str__(self):
        return f"Success: {self.success}, Failed: {self.failed}, Skipped: {self.skipped}"
    
class Logger:
    def __init__(self, log_file_path: str = "", log_to_file=True, log_to_stdout=False):
        self.log_file_loc = log_file_path.strip()
        self.log_to_file = None != log_file_path and "" != log_file_path and log_to_file
        self.log_to_stdout = log_to_stdout
      
    def info(self, message):
        self.log("INFO", message)
      
    def error(self, message):
        self.log("ERROR", message)
      
    def log(self, log_type, message):
        if self.log_to_file:
            with open(self.log_file_loc, "a") as log_file:
                log_file.write(f"[{log_type}] [{datetime.now()}] {message}\n")
              
        if self.log_to_stdout:
            print(f"[{log_type}] [{datetime.now()}] {message}")
          
class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
          
def should_skip_row(csv_val: str = "") -> bool:
    return False
  
def print_metrics(met: Metrics, carriage_return: bool = True):
    if carriage_return:
        print(f"\r{met}", end="")
    else:
        print(met, end="\n")
      
conn = oracledb.connect(
    user=username,
    password=password,
    dsn=dsn,
    wallet_location=wallet_location,
    wallet_password=wallet_password)
conn.autocommit = True
cursor = conn.cursor()

metrics = Metrics()
logger = Logger(
    log_file_path=log_file_location,
    log_to_file=enable_logging_file,
    log_to_stdout=enable_logging_stdout)
progress_interval = RepeatTimer(1, print_metrics, args=(metrics, True))

logger.info(f"Starting script")

with open(csv_file_location, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=csv_file_delimiter, quotechar=csv_file_quotechar)
    if skip_header_row: next(reader, None)
    progress_interval.start()
  
    for row in reader:
        id_val = row[csv_id_col_no]
        new_col_val = row[csv_update_col_no]
      
        if should_skip_row(new_col_val):
            logger.info(f"Skipping {id_val} ({new_col_val})")
            metrics.skipped += 1
            continue
          
        try:
            cursor.execute(f"UPDATE {db_table_name} SET {db_update_col_name} = '{new_col_val}' \
                                WHERE {db_id_col_name} = '{id_val}'")
        except Exception as e:
            logger.error(f"Error updating {id_val} ({new_col_val}):\n{e}")
            metrics.failed += 1
            continue
        else:
            metrics.success += 1
          
progress_interval.cancel()
conn.close()

logger.info(metrics)
logger.info("Script completed")
print_metrics(metrics, False)
print("Script completed")
