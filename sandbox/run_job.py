from .DSS_system import execute_job
import schedule
import time

schedule.every().day.at("09:00").do(execute_job)

while True:
    schedule.run_pending()
    time.sleep(300)
