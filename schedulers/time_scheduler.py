from functools import wraps

from apscheduler.schedulers.blocking import BlockingScheduler
import logging

__scheduler = BlockingScheduler()


def schedule_job(trigger, **trigger_args):
    """
    定时任务装饰器
    :param trigger: 触发器类型
    :param trigger_args: 定时任务的参数
    :return:
    """

    def actual_decorator(func):
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)

        __scheduler.add_job(wrapper, trigger, **trigger_args)
        return wrapper

    return actual_decorator


def start_scheduler():
    """
    启动定时任务
    :return:
    """
    logging.info("开始启动定时任务")
    __scheduler.start()
    logging.info("定时任务启动完成")
