from DSS import execute_job
import schedule
import time

schedule.every().day.at("18:40").do(execute_job)

while True:
    schedule.run_pending()
    time.sleep(300)

# testing
execute_job()
