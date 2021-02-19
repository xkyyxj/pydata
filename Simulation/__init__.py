from Simulation.DayJudge import days_judge
from Simulation.EMAJudge import ema_judge
from Simulation.MACDJudge import macd_judge
from Simulation.PeriodSimulate import single_process_period_simulate
from Simulation.simulate import Simulate, MultiProcessor, single_cpu_core_simulate
from Simulation.KDJJudge import kdj_judge

"""
多线程模拟过程，其中Simulate类可以支持多进程，所以多进程汇总的时候需要统计哪些字段需要怎么汇总
"""


def period_simulate(data_center):
    """
    模拟N天之内的交易盈利情况（当天买入，N天之后卖出）
    :param data_center:
    :return:
    """
    stock_list = data_center.fetch_stock_list()
    stock_codes = []
    for i in range(len(stock_list)):
        stock_codes.append(stock_list[i][0])
    multi_processor = MultiProcessor(PeriodSimulate.PeriodSimulate.get_sum_field(), [], [])
    multi_processor.multi_process_simulate(stock_codes, single_process_period_simulate, "period_simulate",
                                           single_process_period_simulate)


def simulate_with_macd_kdj(data_center):
    """
    macd和KDJ两种指标合并模拟
    :param data_center:
    :return:
    """
    common_simulate_multi_process(data_center, 'macd_kdj', (kdj_judge, macd_judge))


def simulate_with_kdj_multiprocess(data_center):
    """
    通过KDJ指标模拟交易过程
    :param data_center:
    :return:
    """
    common_simulate_multi_process(data_center, 'kdj_judge', (kdj_judge,))


def simulate_with_macd_multi_process(data_center):
    """
    通过macd指标模拟交易过程
    :param data_center:
    :return:
    """
    common_simulate_multi_process(data_center, 'macd_judge', (macd_judge,))


def common_simulate_multi_process(data_center, out_dir_name, judge_time):
    """
    普通的模拟过程，通过判定买入卖出时间点，进行买入卖出操作
    :param judge_time:
    :param out_dir_name:
    :param data_center:
    :return:
    """
    stock_list = data_center.fetch_stock_list()
    stock_codes = []
    for i in range(len(stock_list)):
        stock_codes.append(stock_list[i][0])
    multi_processor = MultiProcessor(['win_num', 'lose_num'], ['max_win_pct'], ['max_lost_pct'])
    multi_processor.multi_process_simulate(stock_codes, judge_time, out_dir_name, single_cpu_core_simulate)


def simulate_with_ema(data_center):
    """
    通过EMA来模拟超短线买入卖出的机制
    :param judge_time:
    :param out_dir_name:
    :param data_center:
    :return:
    """
    common_simulate_multi_process(data_center, 'ema_judge', (ema_judge,))


def simulate_with_days(data_center):
    """
    通过简单上涨下降天数来模拟买入卖出的机制
    :param judge_time:
    :param out_dir_name:
    :param data_center:
    :return:
    """
    common_simulate_multi_process(data_center, 'days_judge', (days_judge,))
