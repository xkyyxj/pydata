import logging

from schedulers.time_scheduler import schedule_job
from data import data_center

@schedule_job('cron', hour=9)
def fetch_stock_daily_info():
    """
    定时任务，每天9点执行一次
    :return:
    """
    logging.info("start fetching stock daily info")
    data_center.fetch_all_data_daily_use()
    pass