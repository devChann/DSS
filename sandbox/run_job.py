from DSS import execute_job
import schedule
import time

schedule.every().day.at("18:36").do(execute_job)

while True:
    schedule.run_pending()
    time.sleep(300)
