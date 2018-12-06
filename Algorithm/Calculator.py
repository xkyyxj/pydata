#!/usr/bin/env python3
"""
计算逻辑
"""


def cal_score(base_data):
    base_data['score'] = (0.8 * base_data['ma10slope'] + 0.1 * base_data['ma5slope'] +
                          0.3 * base_data['ma2slope']) / base_data['af_close'] * 0.1 + base_data['pct_chg']


def cal_ma(base_data, ma_array=(60, 30, 10, 8, 5, 4, 3, 2)):
    """
    对于传入的DataFrame，根据@param ma_array当中指定的计算项，依次计算移动均线MA
    这个计算是根据收盘价计算的
    :param base_data: 基础数据
    :param ma_array: 计算项
    :return:00
    """
    for ma_i in ma_array:
        base_data['ma' + str(ma_i)] = base_data['af_close'].rolling(ma_i).mean()
        # 计算相应的MA的斜率：取极限的概念，后一天的MA值减去前一天的MA值
        shift_ma = base_data['ma' + str(ma_i)].shift(-1)
        base_data['ma' + str(ma_i) + "slope"] = (shift_ma - base_data['ma' + str(ma_i)])


def cal_percent_ma(base_data, ma_array=(60, 30, 10, 8, 5, 4, 3, 2)):
    """
    首先base_data当中要有后复权收盘价的涨跌百分比（相比于前一天收盘价）
    然后对收盘价的涨跌百分比做个移动平均线
    :param base_data:
    :param ma_array:
    :return:
    """
    for ma_i in ma_array:
        base_data["pma" + str(ma_i)] = base_data['af_close_percent'].rolling(ma_i).mean()


def cal_af_percent(base_data):
    shift_af_close = base_data['af_close'].shift(-1)
    base_data['af_close_percent'] = (base_data['af_close'] - shift_af_close) / base_data['af_close']


class Calculator:

    def __init__(self, base_data):
        self.__base_data = base_data

    def cal_ma(self, ma_array=(60, 30, 10, 5, 2)):
        for ma_i in ma_array:
            self.__base_data
