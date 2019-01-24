#!/usr/bin/env python3
"""
计算逻辑
"""
import datetime
import pandas


def cal_score(base_data):
    base_data['score'] = (0.8 * base_data['ma10slope'] + 0.1 * base_data['ma5slope'] +
                          0.3 * base_data['ma2slope']) / base_data['af_close'] * 0.1 + base_data['pct_chg']


def cal_ma(base_data, column_name='af_close', ma_array=(60, 30, 10, 8, 5, 4, 3, 2)):
    """
    对于传入的DataFrame，根据@param ma_array当中指定的计算项，依次计算移动均线MA
    这个计算是根据收盘价计算的
    :param base_data: 基础数据
    :param ma_array: 计算项
    :return:00
    """
    for ma_i in ma_array:
        base_data['ma' + str(ma_i)] = base_data[column_name].rolling(ma_i).mean()
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


def cal_wave_hot(data_center, windows=30, wave_percent=0.2, column='close', curr_days=False):
    """
    计算股票的波动幅度
    :param curr_days: 是否计算最近@param windows天之内的波动情况
    :param wave_percent: @params windows期间之内，（最高价 - 最低价） / 最低价 超过 @param wave_percent，即为高幅度波动
    股票
    :type windows: object 计算窗口，窗口之内的最高价最低价判定波动幅度
    :return: pandas.Series，包含了所有的波动幅度大于@wave_percent的股票代码，如果是常远方分析的话；如果是近期分析的话，、
    返回pandas.DataFrame，包含了最大波动幅度的起止时间以及波动幅度
    """
    if curr_days:
        result = pandas.DataFrame(columns=("ts_code", "start_date", "end_date", "wave_percent"))
    else:
        result = pandas.Series()
    if curr_days:
        end_date = datetime.datetime.now()
        begin_date = end_date - datetime.timedelta(days=windows)
        begin_date = begin_date.strftime("%Y%m%d")
        end_date = end_date.strftime("%Y%m%d")
    else:
        begin_date = '00000000'
        end_date = '99991231'
    stock_list = data_center.fetch_stock_list()

    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date, end_date=end_date)
        if len(base_data) > 0:
            if curr_days:
                max_price_i = base_data[column].idxmax()
                min_price_i = base_data[column].idxmin()
                if max_price_i > min_price_i:
                    t_wave_percent = (base_data.at[max_price_i, column] - base_data.at[min_price_i, column]) / \
                                      base_data.at[min_price_i, column]
                    t_start_date = base_data.at[min_price_i, 'trade_date']
                    t_end_date = base_data.at[max_price_i, 'trade_date']
                else:
                    t_wave_percent = (base_data.at[min_price_i, column] - base_data.at[max_price_i, column]) / \
                                      base_data.at[max_price_i, column]
                    t_start_date = base_data.at[max_price_i, 'trade_date']
                    t_end_date = base_data.at[min_price_i, 'trade_date']
                temp_dict = {'ts_code': stock_list[i][0], 'start_date': t_start_date, 'wave_percent': t_wave_percent,
                             'end_date': t_end_date}
                abs_wave_pct = t_wave_percent if t_wave_percent >= 0 else -t_wave_percent
                if abs_wave_pct >= wave_percent:
                    result = result.append(temp_dict, ignore_index=True)
            else:
                rolling_windows = base_data[column].rolling(windows)
                max_price = rolling_windows.max()
                min_price = rolling_windows.min()
                base_data['delta_pct'] = (max_price - min_price) / min_price
                if len(base_data[base_data['delta_pct'] > wave_percent]) > 0:
                    result = result.append(pandas.Series(stock_list[i][0]))

    return result

