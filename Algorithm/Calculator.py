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


def cal_big_wave_stock(data_center, column='af_close'):
    """
    计算并得出一个月内波动幅度很大的股票
    TODO -- 待验证(寻找一个月内波动幅度比较大的股票)
    :param data_center: 数据中心对象
    :param column: 默认采用哪一列计算最终的结果
    :return: 返回DataFrame, 包含股票的列表以及最终的最大涨幅
    """
    result = pandas.DataFrame(columns=('ts_code', 'max_up_percent'))
    start_date = datetime.datetime.now()
    start_date = start_date + datetime.timedelta(days=-30)
    start_date = start_date.strftime("%Y%m%d")
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=start_date)
        if not base_data.empty and len(base_data) > 0:
            min_price = base_data.at[0, column]
            max_price = base_data.at[0, column]
            up_count = 0  # 上涨达到10%的天数
            down_count = 0  # 下跌达到10%的天数
            max_wave_pct = -100
            for j in range(len(base_data)):
                if base_data.at[j, column] < min_price:
                    min_price = base_data.at[j, column]
                if base_data.at[j, column] > max_price:
                    max_price = base_data.at[j, column]

                wave_pct_up = (base_data.at[j, column] - min_price) / min_price
                wave_pct_down = (base_data.at[j, column] - max_price) / max_price
                # 统计一下最大涨幅
                if wave_pct_up > max_wave_pct:
                    max_wave_pct = wave_pct_up
                up_count += 1 if wave_pct_up > 0.1 else 0
                down_count += 1 if wave_pct_down < -0.1 else 0

            # 上涨和下跌的天数相近的时候我们就算作OK
            min_count = up_count if up_count < down_count else down_count
            if min_count != 0 and (up_count + down_count) / min_count - 1 < 1.1:
                result = result.append({'ts_code': stock_list[i][0], "max_wave_pct": max_wave_pct}, ignore_index=True)
    return result


def cal_wave_high(data_center, start_date=None):
    """
    采用统计的方式，计算波动百分比的标准差，从而确定波动幅度比较大的
    该方法适用于近期波动幅度比较大的情况
    """
    result = pandas.DataFrame(columns=('ts_code', 'std_value'))
    if start_date is None:
        start_date = datetime.datetime.now()
        start_date = start_date + datetime.timedelta(days=-30)
        start_date = start_date.strftime("%Y%m%d")
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=start_date)
        std_value = base_data['pct_chg'].std()
        result = result.append({'ts_code': stock_list[i][0], 'std_value': std_value}, ignore_index=True)
    result = result.sort_values(by=['std_value'], ascending=False)
    # 注意需要重新规整下result的index，不然下面的loc方法不能返回正确的数据
    result.index = range(len(result))

    middle_index = int(len(result) / 2)
    middle_wave = result.loc[middle_index - 15: middle_index + 15]

    final_result = result.loc[0:30]
    final_result = final_result.append(middle_wave)
    return final_result


def get_max_up_stock(data_center, up_days=2):
    """
    得到最近两天涨幅在9%以上的,geige qingdan 。
    """
    result = pandas.DataFrame(columns=('ts_code', 'name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        if not base_data.empty and len(base_data) > up_days:
            can_add = True
            for j in range(up_days):
                can_add = base_data.at[len(base_data) - j - 1, 'pct_chg'] > 9
                if not can_add:
                    break
            if can_add:
                result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]}, ignore_index=True)
    return result


def get_his_max_up_pct(data_center, max_up_days=2, order_days=1):
    """
    追涨停，查看历史信息，该只股票连续:param max_up_days天涨停之后买入，:param order_days天持有期后，
    按照平均收益率排序
    :param order_days: 买入后持有多少天的收益率排序
    :param max_up_days: 最强上涨N天之后买入
    :param data_center: 数据中心，获取数据使用
    """
    result = pandas.DataFrame(columns=('ts_code', 'order_days_ave_pct', 'match_count'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty and len(base_data) > 0:
            up_index = None
            for j in range(max_up_days):
                temp_pct = base_data['pct_chg'].shift(-j)
                if j == 0:
                    up_index = temp_pct > 9
                else:
                    up_index = up_index & (temp_pct > 9)
            before_day_pct = base_data['pct_chg'].shift(1)
            up_index = up_index & (before_day_pct < 9)

            buy_day_price = base_data['af_close'].shift(-max_up_days + 1)
            order_days_price = base_data['af_close'].shift(-max_up_days - order_days + 1)
            base_data['start_date'] = base_data['trade_date'].shift(-max_up_days + 1)
            base_data['order_days_pct'] = (order_days_price - buy_day_price) / buy_day_price

            # :param max_up_days天之内涨停
            temp_rst = base_data[up_index]
            order_days_ave_pct = temp_rst['order_days_pct'].mean()
            result = result.append({'ts_code': stock_list[i][0], 'order_days_ave_pct': order_days_ave_pct,
                                    'match_count': len(temp_rst)},
                                   ignore_index=True)
    result = result.sort_values(by=['order_days_ave_pct'])
    return result


def find_has_up_some(data_center, begin_date=None, end_date=None):
    """
    查找已经上涨了一部分的股票，目标如下：
    1. 最近10天上涨了20%
    2. 一直处于上涨的走势当中，下跌都是微跌
    :param data_center:
    :return:
    """
    result = None
    stock_list = data_center.fetch_stock_list()
    if begin_date is not None and end_date is not None:
        result = pandas.DataFrame(columns=('ts_code', 'up_pct', 'name', 'begindate'))
    else:
        result = pandas.DataFrame(columns=('ts_code', 'up_pct', 'name'))
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if begin_date is not None and end_date is not None:
            begin_date_time = datetime.date(int(begin_date[0:4]), int(begin_date[4:6]), int(begin_date[6:8]))
            end_date_time = datetime.date(int(end_date[0:4]), int(end_date[4:6]), int(end_date[6:8]))
            while begin_date_time <= end_date_time:
                last_time_str = begin_date_time.strftime('%Y%m%d')
                temp_data = base_data[(base_data['trade_date'] >= '20160101') & (base_data['trade_date'] <= last_time_str)]
                cal_has_up(stock_list[i][0], stock_list[i][2], temp_data, result)
                begin_date_time += datetime.timedelta(days=1)
        else:
            result = cal_has_up(stock_list[i][0], stock_list[i][2], base_data, result)
    return result


def cal_has_up(stock_code, stock_name, base_data, result):
    if not base_data.empty and len(base_data) > 0:
        temp_base_data = base_data.sort_values(by=['trade_date'], ascending=False)
        temp_base_data.index = range(len(temp_base_data))
        temp_base_data = temp_base_data.loc[0:10]
        pre_day_af_close = temp_base_data['af_close'].shift(1)
        temp_base_data['af_pct_chg'] = (temp_base_data['af_close'] - pre_day_af_close) / pre_day_af_close

        # 期间之内的下跌幅度不能超过4%
        down_days = temp_base_data[temp_base_data['af_pct_chg'] < 0.04]
        if down_days.empty:
            return

        # 最后一天相比于这期间的最低价，已经上涨了20%
        min_price = temp_base_data['af_close'].min()
        curr_price = temp_base_data.at[0, 'af_close']
        up_pct = (curr_price - min_price) / min_price
        if up_pct > 0.2:
            result = result.append({'ts_code': stock_code, 'up_pct': up_pct, 'name': stock_name},
                                    ignore_index=True)
    return result





