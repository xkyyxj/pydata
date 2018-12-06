#!/usr/bin/env python3
"""
程序入口
"""
import numpy as np
import pandas
import matplotlib as mpl
import matplotlib.pyplot as plt
import tushare
import Data.DataCenter
import Algorithm.Calculator as Calculator
import Algorithm.Verify as Verify

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


data_center = Data.DataCenter.DataCenter()

stock_list = data_center.fetch_stock_list()

base_data = data_center.fetch_base_data("601588.SH", begin_date="20170102", end_date="20181203")
adj_factor = data_center.fetch_adj_factor("601588.SH", begin_date="20170102", end_date="20181203")
base_data['af_close'] = base_data['close'] * adj_factor['adj_factor']
Calculator.cal_af_percent(base_data)
Calculator.cal_percent_ma(base_data)
Calculator.cal_ma(base_data)
Calculator.cal_score(base_data)
pre_value = base_data['close']
shift_pre_value = pre_value.shift(7)
temp_result = shift_pre_value / pre_value
temp_value = np.log(temp_result)
Verify.real_verify(base_data)
plt.grid(True)
plt.show()
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

print(tushare.__version__)
