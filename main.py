#!/usr/bin/env python3
"""
程序入口
"""
import time
import datetime
import numpy as np
import pandas
import matplotlib as mpl
import matplotlib.pyplot as plt
import tushare
import Data.DataCenter
import Algorithm.Calculator as Calculator
import Output.FileOutput as FileOutput
import Algorithm.Verify as Verify
import redis
import json
import DailyUtils.FindLowStock as FindLowStock

data_center = Data.DataCenter.DataCenter()


def batch_fetch_daily_info(data_center):
    all_stock_list = data_center.fetch_stock_list(where="ts_code > '603616.SH'")
    now_day = datetime.datetime.now()
    now_day = now_day.strftime("%Y%m%d")
    for item in range(len(all_stock_list)):
        data_center.fetch_base_data(all_stock_list[item][0], begin_date='20160101', end_date=now_day)
        time.sleep(1)


def fetch_day_index_data(stock_code, begin_date=None, end_date=None):
    if begin_date is None:
        begin_date = datetime.datetime.now()
        begin_date = begin_date.strftime("%Y%m%d")
    if end_date is None:
        end_date = begin_date
    data_center.fetch_index_data(stock_code, begin_date=begin_date, end_date=end_date)


def fetch_all_daily_info(trade_date=None, until_now=False):
    """
    按天获取所有的股票的信息，如果是@param until_now为True的话，那么一直获取到当天为止
    :param stock_code:
    :param trade_date:
    :param until_now:
    :return:
    """
    if trade_date is None:
        trade_date = datetime.datetime.now()
        trade_date = trade_date.strftime("%Y%m%d")
        data_center.fetch_all_base_one_day(trade_date=trade_date)
    else:
        now_time = datetime.datetime.now()
        now_date = now_time.strftime("%Y%m%d")
        temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
        if trade_date < now_date and until_now:
            while trade_date < now_date:
                data_center.fetch_all_base_one_day(trade_date=trade_date)
                temp_date += datetime.timedelta(days=1)
                trade_date = temp_date.strftime("%Y%m%d")


def write_base_info_to_redis(write_form='JSON'):
    if write_form == 'JSON':
        data_center.flush_data_to_redis()
    elif write_form == 'DataFrame':
        data_center.flush_data_frame_to_redis()


print(int(3/2))
# 计算波动幅度较大的股票
# result = Verify.three_max_stock(data_center)
# FileOutput.csv_output(None, result, 'three_max_up.csv')
# result = Calculator.cal_wave_high(data_center)
# FileOutput.csv_output(None, result, 'wave_high_hand.csv')
data_center.init_adj_factor()

# data_center.fetch_all_daily_info_until_now('20190119')
# data_center.init_redis_cache()
# datafram1 = data_center.get_data_frame_from_redis('600017.SH')
# data_center.modify_redis_data_frame("20190119")
# frame1 = pandas.DataFrame([[1, 2], [2, 4]], columns=("1", '2'))
# frame2 = pandas.DataFrame([[1], [2]], columns=('3',))
# frame1['4'] = frame1['1'] * frame2['3']
# print(frame1)
# result = Calculator.cal_wave_hot(data_center, windows=40, column='af_close',wave_percent=0.7, curr_days=True)
# result = FindLowStock.find_has_up_stocks
# dataframe1 = pandas.DataFrame(result, columns=("ts_code",))
# FileOutput.csv_output(None, result, "hot_stock.csv")

# fetch_all_daily_info('20190105', True)

# data_center.init_adj_factor()

# fetch_all_daily_info(trade_date='20181227', until_now=True)

# write_base_info_to_redis("DataFrame")
# value1 = datetime.datetime.now()
# Verify.curr_win_percent(data_center, begin_date="20181213", end_date="20190103")
# print(data_center.get_fetch_data_time())
# value2 = datetime.datetime.now()
# print((value2 - value1).seconds)

# Verify.windows_low_buy(data_center, meet_first_up=True)

# f1 = pandas.DataFrame([[123, 55, 77]], columns=("123", "333", "444"))
# content = f1.to_msgpack()
# f2 = pandas.read_msgpack(content)
# print(f2)

# rdp = redis.ConnectionPool(host='127.0.0.1', port=6379)
# rdc = redis.StrictRedis(connection_pool=rdp)
# rdp.disconnect()
# rdc.lpush("haha", 398475934)
#
# value1 = {}
#
# data1 = pandas.DataFrame(columns=("22", "55"))
# data1 = data1.append({"22": 4356, "55": 888888}, ignore_index=True)
# data1 = data1.append({"22": 7777, "55": 9999}, ignore_index=True)
# se1 = pandas.Series((123, 666), index=("44", "33"))
# all_value = data1.values
# for i in range(all_value.shape[0]):
#     temp_value = all_value[i]
#     setemp = pandas.Series(temp_value, index=data1.columns)
#     print(setemp.to_json())
# # print(value2.to_json())

# ----------------------------------------------DataFrame.rolling --------------------------------------------
# se1 = pandas.Series([1, 4, 3, 7, 8], index=range(5))
# print(se1.shift(-3))
# print(se1.rolling(3).min())
#
# d1 = pandas.DataFrame([[2, 4], [5, 7]], columns=("1", "2"))
# print(d1.rolling(2)['1'].min())
#
# d1 = pandas.DataFrame([[2, 4], [5, 7]], columns=("1", "2"))
# print(d1['1'].rolling(2).min())
# ----------------------------------------------end -------------------------------------------------------------

# ------------------------------------------------Series & Operation --------------------------------------------
# se2 = pandas.Series([False, True, False], index=range(3))
# se3 = pandas.Series([True, True, False], index=range(3))
# print(se2 & se3)
# -------------------------------------------------Series & Operation end ---------------------------------------

# -----------------------------------------------获取每天数据-----------------------------------------------
# data_center.fetch_all_base_one_day('20181218')
# -----------------------------------------------获取煤炭数据 -- end ---------------------------------------

# print('hello!')

# temp_val = [1, 2, 3, 4]
# print(len(temp_val))

# temp_val = [1, 2, 3, 4]
# print(len(temp_val))
#
# data_center = Data.DataCenter.DataCenter()
# temp_value = data_center.fetch_base_data("600312.SH", "20181001", "20181202")

# frame1 = pandas.DataFrame([12.0, 234, 555, 77, 234, 22])
#
# frame2 = pandas.DataFrame([66, 234, 55, 22, 55, 22])
#
# print(frame2[0])
#
# value1 = np.divide(frame1[0], frame2[0])
# print(value1)
#
# value2 = np.log(value1)
# print(value2)
# plt.hist(value2)
# plt.show()

# value1 = np.array([1,97,35,422,66,200])
# x = range(len(value1))
# plt.hist(value1, bins=7)
# plt.show()

# mu, sigma = 2, 0.5
# v = np.random.normal(mu, sigma, 10000)
# plt.hist(v, bins=50, density=1)
# plt.show()

# data_center = Data.DataCenter.DataCenter()
# data_value = data_center.fetch_index_data('000001.SH', '20161001', '20181023')

# ------------------------------------------------ begin ------------------------------------------------------
# data_center = Data.DataCenter.DataCenter()

# stock_list = data_center.fetch_stock_list()

# base_data = data_center.fetch_base_data("601588.SH", begin_date="20160104", end_date="20181203")
# adj_factor = data_center.fetch_adj_factor("601588.SH", begin_date="20160104", end_date="20181203")
# index_data = data_center.fetch_index_data("000001.SH", begin_date='20160104', end_date='20181203')
# Calculator.cal_ma(index_data, column_name='close', ma_array=[50])
# base_data['af_close'] = base_data['close'] * adj_factor['adj_factor']
# Calculator.cal_af_percent(base_data)
# Calculator.cal_percent_ma(base_data)
# Calculator.cal_ma(base_data)
# Calculator.cal_score(base_data)
# pre_value = base_data['close']
# shift_pre_value = pre_value.shift(7)
# temp_result = shift_pre_value / pre_value
# temp_value = np.log(temp_result)
# Verify.period_low_verify(base_data)
# plt.grid(True)
# plt.show()
# ------------------------------------------------ end ------------------------------------------------------


# -------------------------------------------------批量获取数据 ---------------------------------------------------
# data_center = Data.DataCenter.DataCenter()
# all_stock_list = data_center.fetch_stock_list(where="ts_code > '603616.SH'")
# for item in range(len(all_stock_list)):
#     data_center.fetch_base_data(all_stock_list[item][0], begin_date='20160101', end_date='20181217')
#     time.sleep(1)
# -------------------------------------------------批量获取数据 end  ---------------------------------------------------

# -------------------------------------------------批量统计最低买入 ----------------------------------------------------
# data_center = Data.DataCenter.DataCenter()
# Verify.batch_low_verify(data_center)
# -------------------------------------------------批量统计最低买入 -- end ---------------------------------------------

# -------------------------------------------------批量统计最低买入 ----------------------------------------------------
# data_center = Data.DataCenter.DataCenter()
# Verify.with_15_up_buy(data_center)
# -------------------------------------------------批量统计最低买入 -- end ---------------------------------------------

# plt.hist(temp_value[7:], bins=80)
# plt.grid(True)
# plt.show()

# base_data['af_close'].plot()
# (base_data['pma5'] * 10000 - 10000).plot()
# plt.show()

#
# value1 = pandas.Series([123, 4, 5, 6, 6])
# value2 = value1.shift(2)
# print(value2)

# se1 = pandas.Series((1, 2, 3, 4, 5))
# print(se1.rolling(2).mean())

# base_data['log_percent'] = np.log(base_data['percent'])
# plt.hist(base_data['log_percent'], bins=len())0
# plt.show()


# a = np.array((12, 23, 44, 5, 123.78))
#
# np.random.seed(1000)
# y = np.random.standard_normal(50)
# x = range(len(y))
# plt.plot(x, y)
# plt.show()

