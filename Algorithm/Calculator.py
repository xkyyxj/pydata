#!/usr/bin/env python3
"""
计算逻辑
"""
import datetime
import pandas
import numpy as np
import Output.FileOutput as FileOutput


def cal_score(base_data):
    base_data['score'] = (0.8 * base_data['ma10slope'] + 0.1 * base_data['ma5slope'] +
                          0.3 * base_data['ma2slope']) / base_data['af_close'] * 0.1 + base_data['pct_chg']


def cal_ma(base_data, column_name='af_close', ma_array=(60, 30, 20, 10, 8, 5, 4, 3, 2)):
    """
    对于传入的DataFrame，根据@param ma_array当中指定的计算项，依次计算移动均线MA
    这个计算是根据收盘价计算的
    :param base_data: 基础数据
    :param ma_array: 计算项
    :return:00
    """
    for ma_i in ma_array:
        base_data['ma' + str(ma_i)] = base_data[column_name].rolling(ma_i).mean()
        # 计算相应的MA的斜率：取极限的概念，当天的MA值减去前一天的MA值
        shift_ma = base_data['ma' + str(ma_i)].shift(1)
        base_data['ma' + str(ma_i) + "slope"] = (base_data['ma' + str(ma_i)] - shift_ma)


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


def find_has_up_some(data_center, check_days=10, target_up_pct=0.2):
    """
    查找已经上涨了一部分的股票，目标如下：
    1. 最近:param check_days天上涨了:param target_up_pct
    2. 一直处于上涨的走势当中，下跌都是微跌
    :param target_up_pct: 上涨的目标百分比
    :param check_days: target_up_pct要在改参数指定的天内完成
    :param data_center: 数据中心对象
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'up_pct', 'name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty and len(base_data) > 0:
            temp_base_data = base_data.sort_values(by=['trade_date'], ascending=False)
            temp_base_data.index = range(len(temp_base_data))
            temp_base_data2 = temp_base_data.loc[0:check_days]
            pre_day_af_close = temp_base_data2['close'].shift(1)
            temp_base_data2['af_pct_chg'] = (temp_base_data2['close'] - pre_day_af_close) / pre_day_af_close

            # 期间之内的跌幅小于0.04
            # down_days = temp_base_data2[temp_base_data2['pct_chg'] < -4]
            # if not down_days.empty:
            #     continue

            # 最后一天相比于这期间的最低价，已经上涨了20%
            min_price = temp_base_data2['close'].min()
            curr_price = temp_base_data2.at[0, 'close']
            up_pct = (curr_price - min_price) / min_price
            if up_pct > target_up_pct:
                result = result.append({'ts_code': stock_list[i][0], 'up_pct': up_pct, 'name': stock_list[i][2]},
                                       ignore_index=True)
    return result


def sudden_break_stock(data_center):
    """
    长期横盘，价格波动幅度不大的情况下，忽然突破
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'up_pct', 'name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty and len(base_data) > 0:
            close_se = base_data['close']
            close_se = close_se[len(close_se) - 15: len(close_se) - 2]
            std_value = close_se.std()
            ave_value = close_se.mean()
            last_close = base_data.at[len(base_data) - 1, 'close']
            if std_value < 2 and last_close > ave_value:
                up_pct = (last_close - ave_value) / ave_value
                if up_pct > 0.1:
                    result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2], 'up_pct': up_pct},
                                           ignore_index=True)
    return result


def no_max_continue_down_count(data_center):
    """
    非跌停下的连续下跌的交易日数量
    同时统计如下数据：
    1. 平均连续下跌的天数（去除连续跌停）
    2. 平均下跌的百分比
    :param data_center:
    :return:get_max_up_stock
    """
    result = pandas.DataFrame(columns=('ts_code', 'ts_name', 'down_days', 'times', 'ave_down_pct'))
    stock_list = data_center.fetch_stock_list()
    continue_days = 0
    has_down = False
    has_up = False
    for i in range(len(stock_list)):
        print(stock_list[i][0])
        print(len(result))
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty:
            base_data['down_price'] = base_data['af_close'].shift(-1) - base_data['af_close']
            for j in range(len(base_data)):
                if base_data.at[j, 'down_price'] < 0:
                    if has_up:
                        result = _update_result(base_data, continue_days, j, stock_list, result)
                        continue_days = 0
                    continue_days += 1
                    has_down = True
                    has_up = False
                elif base_data.at[j, 'down_price'] > 0:
                    if has_down:
                        result = _update_result(base_data, continue_days, j, stock_list, result)
                        continue_days = 0
                    continue_days += 1
                    has_up = True
                    has_down = False
                else:
                    result = _update_result(base_data, continue_days, j, stock_list, result)
                    has_up = False
                    has_down = False
                    continue_days = 0
    return result


def _update_result(base_data, continue_days, i, stock_list, result):
    # 计算一下上涨的比率，并更新到结果集当中
    start_price = base_data.at[i - continue_days, 'af_close']
    end_price = base_data.at[i, 'af_close']
    pct = (end_price - start_price) / start_price
    temp_index = result[(result['ts_code'] == stock_list[i][0]) &
                        (result['down_days'] == continue_days)].index.tolist()
    if len(temp_index) > 0:
        temp_index = temp_index[0]
        origin_times = result.loc[temp_index, 'times']
        result.loc[temp_index, 'times'] = origin_times + 1
        temp_pct = (result.loc[temp_index, 'ave_down_pct'] + pct) / (origin_times + 1)
        result.loc[temp_index, 'ave_pct'] = temp_pct
    else:
        result = result.append({"ts_code": stock_list[i][0], 'ts_name': stock_list[i][2],
                                'down_days': continue_days, 'times': 1, 'ave_down_pct': pct}, ignore_index=True)
    return result


def cal_max_green_days(data_center, stock_code):
    """
    计算最多持续多少天的阴线
    :param data_center:
    :param stock_code:
    :return:
    """
    base_data = data_center.fetch_base_data_pure_database(stock_code,
                                                          begin_date='20160101')
    if not base_data.empty:
        for i in range(len(base_data)):
            if base_data.at[i, 'pct_chg'] < 0:
                pass


def find_stock_by_ma_slope(data_center, slope_name='ma8'):
    """
    寻找这样的股票，
    :param data_center:
    :param slope_name:
    :return:
    """
    pass


def find_max_start_max_down(data_center):
    """
    寻找第一天涨停然后第二天没有涨停的股票(第一天涨停的前一天必须不是涨停的),并将当天的信息输出到CSV文件当中
    1. 第一天涨停第二天仍然最终涨停的排在靠前
    2. 第一天涨停，第二天开盘涨停，但是最终收盘挂掉的排在第二
    3. 第一天涨停，但是第二天最高价触及涨停，最终没有涨停的排在第三
    此外，还要统计一下概率：
    有如下情况：
    S: 第一天涨停，第二天涨停
    A: 第一天涨停，第二天开盘涨停
    B: 第一天涨停，第二天开盘涨停，最终收盘未收到涨停
    C: 第一天涨停，第二天开盘涨停，并且最终收盘收到涨停
    D: 第一天涨停，第二天开盘价未涨停，第二天最高价涨停，最终收盘未收到涨停
    E: 第一天涨停，第二天开盘未涨停，最终收盘价收到涨停
    概率统计如下：
    1. B / A    意义：第二天开盘涨停价买入，最终有多少几率当天挂掉
    2. C / A    意义：第二天开盘涨停价买入，最终有多少几率当天成功
    3. D / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    4. E / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    5. C / S    意义：开盘涨停最终导致收盘也涨停的概率
    同时对于各只股票单独统计一下，然后输出成为一个CSV文件
    :param data_center:
    :return:
    """
    result = None
    per_stock_rst = pandas.DataFrame(columns=('ts_code', 'name', 'begin_max_fail', 'begin_max_success',
                                              'begin_down_fail', 'begin_down_success'))
    stock_list = data_center.fetch_stock_list()
    S_num = 0
    A_num = 0
    B_num = 0
    C_num = 0
    D_num = 0
    E_num = 0
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty:
            base_data.loc[:, 'next_pct_chg'] = base_data['pct_chg'].shift(-1)
            base_data.loc[:, 'pre_pct_chg'] = base_data['pct_chg'].shift(1)
            base_data.loc[:, 'next_open'] = base_data['open'].shift(-1)
            base_data.loc[:, 'next_high'] = base_data['high'].shift(-1)
            base_data.loc[:, 'next_close'] = base_data['close'].shift(-1)
            base_data.loc[:, 'next_open_pct'] = (base_data['next_open'] - base_data['close']) / base_data['close']
            base_data.loc[:, 'next_high_pct'] = (base_data['next_high'] - base_data['high']) / base_data['high']
            s_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9)
            a_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9)
            b_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9)
            c_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            d_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9) & (base_data['next_high_pct'] > 0.09)
            e_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            S_num += len(s_index[s_index])
            A_num += len(a_index[a_index])
            B_num += len(b_index[b_index])
            C_num += len(c_index[c_index])
            D_num += len(d_index[d_index])
            E_num += len(e_index[e_index])

            m_s_num = len(s_index[s_index])
            m_a_num = len(a_index[a_index])
            m_b_num = len(b_index[b_index])
            m_c_num = len(c_index[c_index])
            m_d_num = len(d_index[d_index])
            m_e_num = len(e_index[e_index])

            temp_dic = {
                'ts_code': stock_list[i][0],
                'name': stock_list[i][2],
                'begin_max_fail': m_b_num / m_a_num if m_a_num > 0 else None,
                'begin_max_success': m_c_num / m_a_num if m_a_num > 0 else None,
                'begin_down_fail': m_d_num / m_s_num if m_s_num > 0 else None,
                'begin_down_success': m_e_num / m_s_num if m_s_num > 0 else None,
            }

            per_stock_rst = per_stock_rst.append(temp_dic, ignore_index=True)

            if result is None:
                result = base_data[s_index | b_index | d_index]
            else:
                result = result.append(base_data[s_index | b_index | d_index])
    if result is not None:
        # 统计一下数据
        begin_max_fail = B_num / A_num
        begin_max_success = C_num / A_num
        begin_down_fail = D_num / S_num
        begin_down_success = E_num / S_num
        begin_max_final_suc = C_num / S_num
        extra_content = "开盘涨停收盘未涨停/开盘涨停：" + str(begin_max_fail) + "\n"
        extra_content += "开盘涨停收盘涨停/开盘涨停：" + str(begin_max_success) + "\n"
        extra_content += "开盘未涨停收盘未涨停最高涨停/两日涨停成功：" + str(begin_down_fail) + "\n"
        extra_content += "开盘未涨停收盘涨停/两日涨停成功：" + str(begin_down_success) + "\n"
        extra_content += "开盘涨停收盘涨停/两日涨停成功：" + str(begin_max_final_suc) + "\n"
        FileOutput.csv_output(None, result, 'total_max_up_rate.csv',
                              extra_content=extra_content, spe_dir_name='max_up_rate')
    if per_stock_rst is not None and not per_stock_rst.empty:
        FileOutput.csv_output(None, result, 'per_stock_max_up_rate.csv', spe_dir_name='max_up_rate')


def find_max_start_max_down_with_buy(data_center, start_days=None, end_days=None):
    """
    寻找第一天涨停然后第二天没有涨停的股票(第一天涨停的前一天必须不是涨停的),并将当天的信息输出到CSV文件当中
    1. 第一天涨停第二天仍然最终涨停的排在靠前
    2. 第一天涨停，第二天开盘涨停，但是最终收盘挂掉的排在第二
    3. 第一天涨停，但是第二天最高价触及涨停，最终没有涨停的排在第三
    4. 第一天涨停买入，第二天最高价卖出是什么情况的
    5. 第一天涨停，第二天没有涨停但是开盘价买入了，第三天的时候最高价卖出是什么情况
    PS: 获利是根据第二天的开盘价做的，没有考虑到第二天的最高价
    此外，还要统计一下概率：
    有如下情况：
    S: 第一天涨停，第二天涨停
    A: 第一天涨停，第二天开盘涨停
    B: 第一天涨停，第二天开盘涨停，最终收盘未收到涨停
    C: 第一天涨停，第二天开盘涨停，并且最终收盘收到涨停
    D: 第一天涨停，第二天开盘价未涨停，第二天最高价涨停，最终收盘未收到涨停
    E: 第一天涨停，第二天开盘未涨停，最终收盘价收到涨停
    F: 第一天涨停
    G: 第一天涨停，第二天开盘未涨停
    H: 第一天涨停，第二天涨停，第三天水下开
    I: 第一天一字涨停
    J: 第一天一字涨停， 第二天收盘涨停
    K: 第一天一字涨停，第二天开盘涨停并且收盘涨停
    L: 第一天涨停，第二天没有涨停
    M: 第一天涨停，第二天没有涨停，并且开盘没有涨停
    概率统计如下：
    1. B / A    意义：第二天开盘涨停价买入，最终有多少几率当天挂掉
    2. C / A    意义：第二天开盘涨停价买入，最终有多少几率当天成功
    3. D / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    4. E / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    5. C / S    意义：开盘涨停最终导致收盘也涨停的概率
    6. S / F    意义：统计一下，第一天涨停后，第二天涨停的概率是多大
    7. A / F    意义：第一天开盘涨停后，第二天开盘涨停的比率如何
    8. G / F    意义：第一天开盘涨停后，第二天开盘未涨停的比率如何
    9. H / S    意义：第一天涨停，第二天涨停，第三天水下开，获利概率
    10. J / I   意义：第一天一字涨停，第二天涨停的概率是多大
    11. K / I   意义：第一天一字涨停，第二天开盘涨停并且收盘涨停概率，决定是否开盘以涨停价买入
    12. J / S   意义：第一天一字涨停，第二天涨停在两天连续涨停当中所占的比率
    同时对于各只股票单独统计一下，然后输出成为一个CSV文件
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'trade_date', 'is_next_day_max_up',
                                       'buy_price', 'sold_price', 'is_win_10', 'is_win_7', 'is_win_3',
                                       'is_begin_down_end_max'))
    per_stock_rst = pandas.DataFrame(columns=('ts_code', 'name', 'begin_max_fail', 'begin_max_success',
                                              'begin_down_fail', 'begin_down_success'))
    stock_list = data_center.fetch_stock_list()
    S_num = 0
    A_num = 0
    B_num = 0
    C_num = 0
    D_num = 0
    E_num = 0
    F_num = 0
    G_num = 0
    H_num = 0
    I_num = 0
    J_num = 0
    K_num = 0
    L_num = 0
    M_num = 0

    # 两日涨停时相关统计数据
    begin_max_suc_win_num = 0  # 第二天开盘涨停并且收盘涨停并且盈利的天数 / 第二天开盘涨停并且收盘涨停的天数
    begin_max_suc_win3_num = 0  # 第二天开盘涨停并且收盘涨停并且盈利3%的天数 / 第二天开盘涨停并且收盘涨停的天数
    begin_max_suc_win7_num = 0
    begin_max_suc_win10_num = 0
    begin_down_max_win_num = 0  # 第二天开盘未涨停并且收盘涨停并且盈利的天数 / 第二天开盘未涨停并且收盘涨停的天数
    begin_down_max_win3_num = 0
    begin_down_max_win7_num = 0
    begin_down_max_win10_num = 0

    begin_max_end_down_win_num = 0
    begin_max_end_down_win3_num = 0
    begin_max_end_down_win7_num = 0
    begin_max_end_down_win10_num = 0
    begin_max_end_down_max_win_num = 0
    begin_max_end_down_max_win3_num = 0
    begin_max_end_down_max_win7_num = 0
    begin_max_end_down_max_win10_num = 0

    water_down_win_num = 0
    water_down_win3_num = 0
    water_down_win7_num = 0
    water_down_win10_num = 0

    # 第一天涨停买入，然后第二天的时候卖出相关统计数据
    one_max_sold_win_num = 0
    one_max_sold_win3_num = 0
    one_max_sold_win7_num = 0
    one_max_sold_win10_num = 0

    # 第一天涨停，第二天没有涨停但是开盘价买入了，第三天的时候最高价卖出是什么情况
    one_max_up_third_sold_num = 0
    one_max_up_third_sold3_num = 0
    one_max_up_third_sold7_num = 0
    one_max_up_third_sold10_num = 0

    # 第一天涨停，第二天未涨停并且开盘非涨停并且以开盘价买入，第三天最高价卖出效果
    two_down_third_max_sold_win_num = 0
    two_down_third_max_sold_win3_num = 0
    two_down_third_max_sold_win7_num = 0
    two_down_third_max_sold_win10_num = 0

    # 第一天涨停，第二天未涨停并且开盘非涨停并且以开盘价买入，第三天最高价卖出效果
    two_down_third_max_sold_nmax_b_win_num = 0
    two_down_third_max_sold_nmax_b_win3_num = 0
    two_down_third_max_sold_nmax_b_win7_num = 0
    two_down_third_max_sold_nmax_b_win10_num = 0

    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty:
            base_data.loc[:, 'next_pct_chg'] = base_data['pct_chg'].shift(-1)
            base_data.loc[:, 'pre_pct_chg'] = base_data['pct_chg'].shift(1)
            base_data.loc[:, 'next_open'] = base_data['open'].shift(-1)
            base_data.loc[:, 'next_high'] = base_data['high'].shift(-1)
            base_data.loc[:, 'next_close'] = base_data['close'].shift(-1)
            base_data.loc[:, 'third_open'] = base_data['open'].shift(-2)
            base_data.loc[:, 'third_high'] = base_data['high'].shift(-2)
            base_data.loc[:, 'next_open_pct'] = (base_data['next_open'] - base_data['close']) / base_data['close']
            base_data.loc[:, 'next_high_pct'] = (base_data['next_high'] - base_data['high']) / base_data['high']
            base_data.loc[:, 'sold_price'] = base_data['open'].shift(-2)
            base_data.loc[:, 'max_sold_price'] = base_data['high'].shift(-2)

            if start_days is not None:
                end_days = datetime.datetime.now().strftime("%Y%m%d") if end_days is None else end_days
                base_data = base_data[(base_data['trade_date'] <= end_days) & (base_data['trade_date'] >= start_days)]

            s_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9)
            a_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9)
            b_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9)
            c_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            d_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9) & (base_data['next_high_pct'] > 0.09)
            e_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (
                    base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            f_index = (base_data['pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9)
            g_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9)
            h_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['third_open'] < base_data['next_open'])
            i_index = (base_data['pct_chg'] > 9) & (base_data['open'] == base_data['close']) & \
                      (base_data['open'] == base_data['high'])
            l_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] < 9) & (base_data['pre_pct_chg'] < 9)
            m_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] < 9) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_open_pct'] < 0.09)
            base_data.loc[:, 'b_index'] = b_index
            base_data.loc[:, 'c_index'] = c_index
            base_data.loc[:, 'e_index'] = e_index
            base_data.loc[:, 'h_index'] = h_index
            S_num += len(s_index[s_index])
            A_num += len(a_index[a_index])
            B_num += len(b_index[b_index])
            C_num += len(c_index[c_index])
            D_num += len(d_index[d_index])
            E_num += len(e_index[e_index])
            F_num += len(f_index[f_index])
            G_num += len(g_index[g_index])
            H_num += len(h_index[h_index])
            I_num += len(i_index[i_index])
            J_num += len(base_data[s_index & i_index])
            K_num += len(base_data[c_index & i_index])
            L_num += len(base_data[l_index])
            M_num += len(base_data[m_index])

            # 下面统计一下第二天开盘涨停并且收盘涨停的股票当中，获利百分比
            begin_max_value = base_data[c_index]
            begin_max_value['win_pct'] = (begin_max_value['sold_price'] - begin_max_value['next_open']) \
                                         / begin_max_value['next_open']
            begin_max_suc_win_num += len(begin_max_value[begin_max_value['win_pct'] > 0])
            begin_max_suc_win3_num += len(begin_max_value[begin_max_value['win_pct'] > 0.03])
            begin_max_suc_win7_num += len(begin_max_value[begin_max_value['win_pct'] > 0.07])
            begin_max_suc_win10_num += len(begin_max_value[begin_max_value['win_pct'] > 0.1])

            # 下面统计一下第二天开盘未涨停并且收盘涨停的股票当中，获利百分比
            begin_down_max_value = base_data[e_index]
            begin_down_max_value['win_pct'] = (begin_down_max_value['sold_price'] - begin_down_max_value['next_open']) / \
                                              begin_down_max_value['next_open']
            begin_down_max_win_num += len(begin_down_max_value[begin_down_max_value['win_pct'] > 0])
            begin_down_max_win3_num += len(begin_down_max_value[begin_down_max_value['win_pct'] > 0.03])
            begin_down_max_win7_num += len(begin_down_max_value[begin_down_max_value['win_pct'] > 0.07])
            begin_down_max_win10_num += len(begin_down_max_value[begin_down_max_value['win_pct'] > 0.1])

            # 下面统计一下第二天开盘涨停并且收盘未涨停的股票当中，获利百分比(开盘价卖出以及最高价卖出)
            begin_max_end_down_value = base_data[b_index]
            begin_max_end_down_value['win_pct'] = (begin_max_end_down_value['sold_price'] - begin_max_end_down_value[
                'next_open']) / \
                                                  begin_max_end_down_value['next_open']
            begin_max_end_down_value['max_win_pct'] = (begin_max_end_down_value['max_sold_price'] -
                                                       begin_max_end_down_value['next_open']) / \
                                                      begin_max_end_down_value['next_open']
            # 开盘价卖出获利百分比
            begin_max_end_down_win_num += len(begin_max_end_down_value[begin_max_end_down_value['win_pct'] > 0])
            begin_max_end_down_win3_num += len(begin_max_end_down_value[begin_max_end_down_value['win_pct'] > 0.03])
            begin_max_end_down_win7_num += len(begin_max_end_down_value[begin_max_end_down_value['win_pct'] > 0.07])
            begin_max_end_down_win10_num += len(begin_max_end_down_value[begin_max_end_down_value['win_pct'] > 0.1])
            # 最高价卖出百分比
            begin_max_end_down_max_win_num += len(begin_max_end_down_value[begin_max_end_down_value['max_win_pct'] > 0])
            begin_max_end_down_max_win3_num += len(
                begin_max_end_down_value[begin_max_end_down_value['max_win_pct'] > 0.03])
            begin_max_end_down_max_win7_num += len(
                begin_max_end_down_value[begin_max_end_down_value['max_win_pct'] > 0.07])
            begin_max_end_down_max_win10_num += len(
                begin_max_end_down_value[begin_max_end_down_value['max_win_pct'] > 0.1])

            # 统计一下两天封涨停并且第三天水下开的情况当中，获利概率如何
            water_down_data = base_data[h_index]
            water_down_data['win_pct'] = (water_down_data['sold_price'] - water_down_data['next_open']) \
                                         / water_down_data['next_open']
            water_down_win_num += len(water_down_data[water_down_data['win_pct'] > 0])
            water_down_win3_num += len(water_down_data[water_down_data['win_pct'] > 0.03])
            water_down_win7_num += len(water_down_data[water_down_data['win_pct'] > 0.07])
            water_down_win10_num += len(water_down_data[water_down_data['win_pct'] > 0.1])

            # 下面统计一下第一天涨停，然后第二天最高价卖出是怎么样的
            one_max_sold = base_data[f_index]
            one_max_sold_win_num += len(one_max_sold[one_max_sold['next_high_pct'] > 0])
            one_max_sold_win3_num += len(one_max_sold[one_max_sold['next_high_pct'] > 0.03])
            one_max_sold_win7_num += len(one_max_sold[one_max_sold['next_high_pct'] > 0.07])
            one_max_sold_win10_num += len(one_max_sold[one_max_sold['next_high_pct'] > 0.1])

            # 统计一下第二天没有封涨停并且开盘价非涨停价以开盘价买入，第三天最高价卖出是什么情况
            two_down_third_max_sold = base_data[l_index]
            two_down_third_max_sold_win_num += len(
                two_down_third_max_sold[two_down_third_max_sold['next_high_pct'] > 0])
            two_down_third_max_sold_win3_num += len(
                two_down_third_max_sold[two_down_third_max_sold['next_high_pct'] > 0.03])
            two_down_third_max_sold_win7_num += len(
                two_down_third_max_sold[two_down_third_max_sold['next_high_pct'] > 0.07])
            two_down_third_max_sold_win10_num += len(
                two_down_third_max_sold[two_down_third_max_sold['next_high_pct'] > 0.1])

            # 统计一下第二天没有封涨停并且开盘价非涨停价以开盘价买入，第三天最高价卖出是什么情况
            two_down_third_max_sold_nmax_b = base_data[m_index]
            two_down_third_max_sold_nmax_b_win_num += len(
                two_down_third_max_sold_nmax_b[two_down_third_max_sold_nmax_b['next_high_pct'] > 0])
            two_down_third_max_sold_nmax_b_win3_num += len(
                two_down_third_max_sold_nmax_b[two_down_third_max_sold_nmax_b['next_high_pct'] > 0.03])
            two_down_third_max_sold_nmax_b_win7_num += len(
                two_down_third_max_sold_nmax_b[two_down_third_max_sold_nmax_b['next_high_pct'] > 0.07])
            two_down_third_max_sold_nmax_b_win10_num += len(
                two_down_third_max_sold_nmax_b[two_down_third_max_sold_nmax_b['next_high_pct'] > 0.1])

            m_s_num = len(s_index[s_index])
            m_a_num = len(a_index[a_index])
            m_b_num = len(b_index[b_index])
            m_c_num = len(c_index[c_index])
            m_d_num = len(d_index[d_index])
            m_e_num = len(e_index[e_index])

            temp_dic = {
                'ts_code': stock_list[i][0],
                'name': stock_list[i][2],
                'begin_max_fail': m_b_num / m_a_num if m_a_num > 0 else None,
                'begin_max_success': m_c_num / m_a_num if m_a_num > 0 else None,
                'begin_down_fail': m_d_num / m_s_num if m_s_num > 0 else None,
                'begin_down_success': m_e_num / m_s_num if m_s_num > 0 else None,
            }

            per_stock_rst = per_stock_rst.append(temp_dic, ignore_index=True)

            temp_rst = pandas.DataFrame(columns=('ts_code', 'name', 'trade_date', 'is_next_day_max_up',
                                                 'buy_price', 'sold_price', 'max_sold_price', 'is_win_10', 'is_win_7',
                                                 'is_win_3',
                                                 'win_pct', 'is_two_max_up', 'is_begin_max_end_max',
                                                 'is_begin_down_end_max', 'max_sold_win_pct', 'is_max_sold_3pct',
                                                 'is_max_sold_7pct', 'is_max_sold_10pct', 'is_water_down_begin'))
            temp_value = base_data[s_index | b_index | d_index]
            # 以次日开盘价买入
            if not temp_value.empty:
                temp_value.loc[:, 'win_pct'] = (temp_value['sold_price'] - temp_value['next_open']) \
                                               / temp_value['next_open']
                temp_value.loc[:, 'max_sold_win_pct'] = (temp_value['max_sold_price'] - temp_value['next_open']) \
                                                        / temp_value['next_open']
                temp_rst.loc[:, 'ts_code'] = temp_value['ts_code']
                temp_rst.loc[:, 'name'] = stock_list[i][2]
                temp_rst.loc[:, 'trade_date'] = temp_value['trade_date']
                temp_rst.loc[:, 'is_next_day_max_up'] = temp_value['next_open_pct'] > 0.09
                temp_rst.loc[:, 'buy_price'] = temp_value['next_open']
                temp_rst.loc[:, 'sold_price'] = temp_value['sold_price']
                temp_rst.loc[:, 'max_sold_price'] = temp_value['max_sold_price']
                temp_rst.loc[:, 'is_win_10'] = temp_value['win_pct'] > 0.09
                temp_rst.loc[:, 'is_win_7'] = temp_value['win_pct'] > 0.07
                temp_rst.loc[:, 'is_win_3'] = temp_value['win_pct'] > 0.03
                temp_rst.loc[:, 'win_pct'] = temp_value['win_pct']
                temp_rst.loc[:, 'max_sold_win_pct'] = temp_value['max_sold_win_pct']
                temp_rst.loc[:, 'is_two_max_up'] = temp_value['next_pct_chg'] > 9
                temp_rst.loc[:, 'is_begin_max_end_max'] = temp_value['c_index']
                temp_rst.loc[:, 'is_begin_down_end_max'] = temp_value['e_index']
                temp_rst.loc[:, 'is_begin_max_end_down'] = temp_value['b_index']
                temp_rst.loc[:, 'is_water_down_begin'] = temp_value['h_index']
                temp_rst.loc[:, 'is_max_sold_10pct'] = temp_value['max_sold_win_pct'] > 0.09
                temp_rst.loc[:, 'is_max_sold_7pct'] = temp_value['max_sold_win_pct'] > 0.07
                temp_rst.loc[:, 'is_max_sold_3pct'] = temp_value['max_sold_win_pct'] > 0.03
                result = result.append(temp_rst)
    if result is not None:
        # 统计一下数据
        begin_max_fail = B_num / A_num
        begin_max_success = C_num / A_num
        begin_down_fail = D_num / S_num
        begin_down_success = E_num / S_num
        begin_max_final_suc = C_num / S_num
        water_down_pct = H_num / S_num

        # 统计一下两日涨停的获利概率
        two_max_up_rst = result[result['is_two_max_up']]
        win_pct = len(two_max_up_rst[two_max_up_rst['win_pct'] > 0]) / len(two_max_up_rst)
        win_3_pct = len(two_max_up_rst[two_max_up_rst['win_pct'] > 0.03]) / len(two_max_up_rst)
        win_7_pct = len(two_max_up_rst[two_max_up_rst['win_pct'] > 0.07]) / len(two_max_up_rst)
        win_10_pct = len(two_max_up_rst[two_max_up_rst['win_pct'] > 0.09]) / len(two_max_up_rst)
        extra_content = "开盘涨停收盘未涨停/开盘涨停：" + str(begin_max_fail) + "\n"
        extra_content += "开盘涨停收盘涨停/开盘涨停：" + str(begin_max_success) + "\n"
        extra_content += "开盘未涨停收盘未涨停最高涨停/两日涨停成功：" + str(begin_down_fail) + "\n"
        extra_content += "开盘未涨停收盘涨停/两日涨停成功：" + str(begin_down_success) + "\n"
        extra_content += "开盘涨停收盘涨停/两日涨停成功：" + str(begin_max_final_suc) + "\n"
        extra_content += "连续两天涨停/第一天涨停：" + str(S_num / F_num) + "\n"
        extra_content += "第一天涨停且第二天开盘涨停/第一天涨停：" + str(A_num / F_num) + "\n"
        extra_content += "第一天涨停且第二天开盘未涨停/第一天涨停：" + str(G_num / F_num) + "\n"
        extra_content += "第一天一字涨停且第二天收盘涨停/第一天一字涨停：" + str(J_num / I_num) + "\n"
        extra_content += "第一天一字涨停且第二天收盘涨停/第一天开盘涨停且收盘涨停：" + str(K_num / I_num) + "\n"
        extra_content += "第一天一字涨停且第二天收盘涨停/两日涨停成功：" + str(J_num / S_num) + "\n"
        # 下面统计一下如下数据：
        # 1. 第二天开盘涨停并且收盘涨停的时候，第三天开盘价卖出的收益概率分布
        # 2. 第二天开盘未涨停并且收盘涨停的时候，第三天开盘价卖出的收益概率分布
        extra_content += "第二天开盘涨停并且收盘涨停并且盈利的天数/第二天开盘涨停并且收盘涨停的天数:" + \
                         str(begin_max_suc_win_num / C_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘涨停并且盈利超3%的天数/第二天开盘涨停并且收盘涨停的天数:" + \
                         str(begin_max_suc_win3_num / C_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘涨停并且盈利超7%的天数/第二天开盘涨停并且收盘涨停的天数:" + \
                         str(begin_max_suc_win7_num / C_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘涨停并且盈利超10%的天数/第二天开盘涨停并且收盘涨停的天数:" + \
                         str(begin_max_suc_win10_num / C_num) + "\n"
        extra_content += "第二天开盘未涨停并且收盘涨停并且盈利的天数/第二天开盘未涨停并且收盘涨停的天数:" + \
                         str(begin_down_max_win_num / E_num) + "\n"
        extra_content += "第二天开盘未涨停并且收盘涨停并且盈利超3%的天数/第二天开盘未涨停并且收盘涨停的天数:" + \
                         str(begin_down_max_win3_num / E_num) + "\n"
        extra_content += "第二天开盘未涨停并且收盘涨停并且盈利超7%的天数/第二天开盘未涨停并且收盘涨停的天数:" + \
                         str(begin_down_max_win7_num / E_num) + "\n"
        extra_content += "第二天开盘未涨停并且收盘涨停并且盈利超10%的天数/第二天开盘未涨停并且收盘涨停的天数:" + \
                         str(begin_down_max_win10_num / E_num) + "\n"
        # 3. 第二天开盘涨停收盘未涨停的时候，第三天开盘价卖出的收益情况
        extra_content += "第二天开盘涨停并且收盘未涨停并且盈利的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_win_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且盈利超3%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_win3_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且盈利超7%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_win7_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且盈利超10%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_win10_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且最高价卖出盈利的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_max_win_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且最高价卖出盈利超3%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_max_win3_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且最高价卖出盈利超7%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_max_win7_num / B_num) + "\n"
        extra_content += "第二天开盘涨停并且收盘未涨停并且最高价卖出盈利超10%的天数/第二天开盘涨停并且收盘未涨停的天数:" + \
                         str(begin_max_end_down_max_win10_num / B_num) + "\n"
        extra_content += "两日涨停并且第三天水下开天数/两日涨停天数：" + str(water_down_pct) + "\n"
        extra_content += "两日涨停并且第三天水下开并且获利天数/连续两天涨停" + str(water_down_win_num / H_num) + "\n"
        extra_content += "两日涨停并且第三天水下开并且获利超3%天数/连续两天涨停" + str(water_down_win3_num / H_num) + "\n"
        extra_content += "两日涨停并且第三天水下开并且获利超7%天数/连续两天涨停" + str(water_down_win7_num / H_num) + "\n"
        extra_content += "两日涨停并且第三天水下开并且获利超10%天数/连续两天涨停" + str(water_down_win10_num / H_num) + "\n" + "\n"
        extra_content += "两日涨停获利概率：" + str(win_pct) + "\n"
        extra_content += "两日涨停获利超过3%概率：" + str(win_3_pct) + "\n"
        extra_content += "两日涨停获利超过7%概率：" + str(win_7_pct) + "\n"
        extra_content += "两日涨停获利超过10%概率：" + str(win_10_pct) + "\n"

        # 第一天涨停，然后第二天最高价卖出的相关统计数据
        extra_content += "第一天涨停价买入第二天最高价卖出获利比率：" + str(one_max_sold_win_num / F_num) + "\n"
        extra_content += "第一天涨停价买入第二天最高价卖出获利超过3%比率：" + str(one_max_sold_win3_num / F_num) + "\n"
        extra_content += "第一天涨停价买入第二天最高价卖出获利超过7%比率：" + str(one_max_sold_win7_num / F_num) + "\n"
        extra_content += "第一天涨停价买入第二天最高价卖出获利超过10%比率：" + str(one_max_sold_win10_num / F_num) + "\n"

        # 第一天涨停，第二天未涨停，第三天最高价卖出效果
        extra_content += "第一天涨停价第二天未涨停开盘价买入第三天最高价卖出获利比率：" + str(two_down_third_max_sold_win_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入第三天最高价卖出获利超3%比率：" + str(two_down_third_max_sold_win3_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入第三天最高价卖出获利超7%比率：" + str(two_down_third_max_sold_win7_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入第三天最高价卖出获利超10%比率：" + str(two_down_third_max_sold_win10_num / L_num) + "\n"

        # 第一天涨停，第二天未涨停并且以开盘价买入并且第二天价格非涨停价，第三天最高价卖出效果
        extra_content += "第一天涨停价第二天未涨停开盘价买入且开盘未涨停第三天最高价卖出获利比率：" + str(
            two_down_third_max_sold_nmax_b_win_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入且开盘未涨停第三天最高价卖出获利超3%比率：" + str(
            two_down_third_max_sold_nmax_b_win3_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入且开盘未涨停第三天最高价卖出获利超7%比率：" + str(
            two_down_third_max_sold_nmax_b_win7_num / L_num) + "\n"
        extra_content += "第一天涨停价第二天未涨停开盘价买入且开盘未涨停第三天最高价卖出获利超10%比率：" + str(
            two_down_third_max_sold_nmax_b_win10_num / L_num) + "\n"
        FileOutput.csv_output(None, result, 'total_max_up_rate.csv',
                              extra_content=extra_content, spe_dir_name='max_up_rate_with_buy_09')
    if per_stock_rst is not None and not per_stock_rst.empty:
        FileOutput.csv_output(None, result, 'per_stock_max_up_rate.csv', spe_dir_name='max_up_rate_with_buy_09')


def cal_macd(data_center):
    """
    计算MACD值，标准的MACD(12,26,9)
    """
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty:
            pass


def cal_macd_per_stock(base_data):
    """
    单只股票计算MACD
    :type base_data: object
    """
    if not base_data.empty:
        base_data.loc[:, 'ema26'] = float(0)
        base_data.loc[:, 'ema12'] = float(0)
        base_data.loc[:, 'diff'] = float(0)
        base_data.loc[:, 'dea'] = float(0)
        base_data.loc[:, 'bar'] = float(0)
        for i in range(1, len(base_data)):
            base_data.at[i, 'ema12'] = (11 * base_data.at[i - 1, 'ema12'] + 2 * base_data.at[i, 'close']) / 13
            base_data.at[i, 'ema26'] = (25 * base_data.at[i - 1, 'ema26'] + 2 * base_data.at[i, 'close']) / 27
            base_data.at[i, 'diff'] = base_data.at[i, 'ema12'] - base_data.at[i, 'ema26']
            base_data.at[i, 'dea'] = (7 * base_data.at[i - 1, 'dea'] + 2 * base_data.at[i, 'diff']) / 9
        base_data.loc[:, 'bar'] = base_data['diff'] - base_data['dea']
        base_data.loc[:, 'bar'] = base_data['bar'] * 2
    return base_data


def cal_trix_per_stock(base_data):
    """
    计算TRIX指标数值，按照同花顺的默认参数来：TRIX（12,20）
    :param base_data:
    :return:
    """
    if not base_data.empty:
        base_data.loc[:, 'ax'] = float(0)
        base_data.loc[:, 'bx'] = float(0)
        base_data.loc[:, 'trix'] = float(0)
        base_data.loc[:, 'trma'] = float(0)
        for i in range(1, len(base_data)):
            base_data.at[i, 'ax'] = (base_data.at[i - 1, 'ax'] * 11 + base_data.at[i, 'close'] * 2) / 13
            base_data.at[i, 'bx'] = (base_data.at[i - 1, 'bx'] * 11 + base_data.at[i, 'ax'] * 2) / 13
            base_data.at[i, 'trix'] = (base_data.at[i - 1, 'trix'] * 11 + base_data.at[i, 'bx'] * 2) / 13
        base_data.loc[:, 'trma'] = base_data['trix'].rolling(20).mean()
    return base_data


def cal_kdj_per_stock(base_data):
    """
    计算单只stock的KDJ指标
    :param base_data:
    :return:
    """
    if not base_data.empty:
        recent_9_max_close = base_data['close'].rolling(9).max()
        recent_9_min_close = base_data['close'].rolling(9).min()
        if len(base_data) > 9:
            recent_min_value = 99999
            recent_max_value = 0
            for k in range(8):
                if base_data.at[k, 'close'] < recent_min_value:
                    recent_min_value = base_data.at[k, 'close']
                if base_data.at[k, 'close'] > recent_max_value:
                    recent_max_value = base_data.at[k, 'close']
                recent_9_min_close[k] = recent_min_value
                recent_9_max_close[k] = recent_max_value
        rsv = (base_data['close'] - recent_9_min_close) / (recent_9_max_close - recent_9_min_close) * 100
        base_data.loc[:, 'k_value'] = float(0)
        base_data.loc[:, 'd_value'] = float(0)
        base_data.loc[:, 'j_value'] = float(0)
        for i in range(1, len(base_data)):
            temp_val = ((2 * base_data.at[i - 1, 'k_value']) + rsv[i]) / 3
            temp_val = 100 if temp_val > 100 else temp_val
            temp_val = 0 if temp_val < 0 else temp_val
            base_data.at[i, 'k_value'] = temp_val
            temp_val = ((2 * base_data.at[i - 1, 'd_value']) + base_data.at[i - 1, 'k_value']) / 3
            temp_val = 100 if temp_val > 100 else temp_val
            temp_val = 0 if temp_val < 0 else temp_val
            base_data.at[i, 'd_value'] = temp_val
    j_t_val = 3 * base_data['k_value']
    temp_val = 2 * base_data['d_value']
    j_val = j_t_val.sub(temp_val)
    base_data.loc[:, 'j_value'] = j_val
    return base_data


def find_quick_down_stock(data_center):
    """
    寻找快速下跌的股票
    1. 5个交易日之内跌去了13%的股票
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        if not base_data.empty:
            five_before_price = base_data['close'].shift(5)
            base_data.loc[:, 'five_pct'] = (five_before_price - base_data['close']) / base_data['close']
            if base_data.at[len(base_data) - 1, 'five_pct'] > 0.13:
                result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]}, ignore_index=True)
    file_name = 'quick_down_stock'
    now_time = datetime.datetime.now()
    now_time_str = now_time.strftime('%Y%m%d')
    file_name += now_time_str
    file_name += '.csv'
    FileOutput.csv_output(None, result, file_name, spe_dir_name='quick_down_stock')


def find_quick_up_stock(data_center, period=5, up_pct_min=0.10, up_pct_max=0.45, need_stable=False, is_red=False):
    """
    查找快速上涨的股票
    1. :param period个交易日之内上涨在:param up_pct_min到:param up_pct_max之间的股票，如果need_stable为True
    2. 如果:param need_stable为False，则:param period个交易日之内上涨大于:param up_pct_min
    :param is_red:
    :param need_stable:
    :param up_pct_max:
    :param up_pct_min:
    :param period:
    :param data_center:
    :return:
    """
    columns = ['ts_code', 'name']
    for i in range(period):
        columns.append(str(i + 1) + '_pct_cht')
    result = pandas.DataFrame(columns=columns)
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        if not base_data.empty and len(base_data) > period:
            five_before_price = base_data['close'].shift(period)
            base_data.loc[:, 'five_pct'] = (base_data['close'] - five_before_price) / five_before_price
            if base_data.at[len(base_data) - 1, 'five_pct'] > up_pct_min:
                if need_stable and base_data.at[len(base_data) - 1, 'five_pct'] < up_pct_max:
                    # 如果:param need_stable置位True，那么需要这几天稳步上涨，且没有下跌
                    is_continue_up = True
                    temp_dic = {
                        'ts_code': stock_list[i][0],
                        'name': stock_list[i][2]
                    }
                    for j in range(1, period + 1):
                        if base_data.at[len(base_data) - j, 'pct_chg'] > 0:  # 稳步上涨意味着每天涨幅大于0
                            temp_dic[str(period - j - 1) + '_pct_chg'] = base_data.at[len(base_data) - j, 'pct_chg']
                            if is_red:
                                is_continue_up = base_data.at[len(base_data) - j, 'close'] > \
                                                 base_data.at[len(base_data) - j, 'open']
                                if not is_continue_up:
                                    break
                            else:
                                is_continue_up = True
                        else:
                            is_continue_up = False
                            break
                    if is_continue_up:
                        result = result.append(temp_dic, ignore_index=True)
                elif not need_stable:
                    result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]}, ignore_index=True)
    file_name = 'quick_up_stock'
    file_name = file_name + '_stable' if need_stable else file_name
    now_time = datetime.datetime.now()
    now_time_str = now_time.strftime('%Y%m%d')
    file_name += '_' + now_time_str + "_" + str(period)
    file_name += '_stable' if need_stable else ''
    file_name += '_allred' if is_red else ''
    file_name += '.csv'
    FileOutput.csv_output(None, result, file_name, spe_dir_name='quick_up_stock')


def find_down_then_up(data_center):
    """
    查找下跌后然后开始上涨走势的股票
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(
        columns=('ts_code', 'ts_name', 'buy_date', 'buy_price', 'sold_date', 'sold_price', 'continue_days', 'win_pct'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        continue_down_days = 0
        first_red = False
        has_stock = False
        if not base_data.empty:
            for j in range(len(base_data)):
                # 判定sold条件
                if base_data.at[j, 'pct_chg'] < 0 and has_stock:
                    result.at[len(result) - 1, 'sold_date'] = base_data.at[j, 'trade_date']
                    result.at[len(result) - 1, 'sold_price'] = base_data.at[j, 'close']
                    start_date = result.at[len(result) - 1, 'buy_date']
                    start_date = datetime.date(int(start_date[0:4]), int(start_date[4:6]), int(start_date[6:8]))
                    end_date = base_data.at[j, 'trade_date']
                    end_date = datetime.date(int(end_date[0:4]), int(end_date[4:6]), int(end_date[6:8]))
                    result.at[len(result) - 1, 'continue_days'] = (end_date - start_date).days
                if base_data.at[j, 'pct_chg'] < 0 and base_data.at[j, 'close'] < base_data.at[j, 'open']:
                    continue_down_days += 1
                elif base_data.at[j, 'pct_chg'] < 0 and base_data.at[j, 'close'] > base_data.at[j, 'open']:
                    first_red = True
                elif first_red and base_data.at[j, 'open'] > base_data.at[j - 1, 'close']:
                    temp_dic = {
                        'ts_code': stock_list[i][0],
                        'ts_name': stock_list[i][2],
                        'buy_date': base_data.at[j, 'trade_date'],
                        'buy_price': base_data.at[j, 'open']
                    }
                    result = result.append(temp_dic, ignore_index=True)
                    has_stock = True
                    continue_down_days = 0
                    first_red = False
                else:
                    # 失败
                    continue_down_days = 1
                    first_red = False
    if not result.empty:
        result.loc[:, 'win_pct'] = (result['sold_price'] - result['buy_price']) / result['buy_price']

        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name = 'down_then_up_buy' + now_time_str + ".csv"
        FileOutput.csv_output(None, result, file_name, spe_dir_name='quick_up_stock')


def find_down_then_up_for_buy(data_center):
    """
    查找下跌后然后开始上涨走势的股票
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'ts_name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        continue_down_days = 0
        first_red = False
        has_stock = False
        if not base_data.empty:
            continue_down_days = 0
            first_red = False
            last_index = len(base_data) - 1
            if len(base_data) > 7:
                for j in range(5):
                    if j < 4 and base_data.at[last_index - 4 + j, 'pct_chg'] < 0:
                        continue_down_days += 1
                    elif j == 4 and base_data.at[last_index, 'close'] > base_data.at[last_index, 'open']:
                        first_red = True
                if first_red and continue_down_days == 4:
                    result = result.append({'ts_code': stock_list[i][0], 'ts_name': stock_list[i][2]},
                                           ignore_index=True)
    if not result.empty:
        file_name = "down_then_up_for_buy.csv"
        FileOutput.csv_output(None, result, file_name)

    if not result.empty:
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name = 'down_then_up_for_buy' + now_time_str + ".csv"
        FileOutput.csv_output(None, result, file_name, spe_dir_name='quick_up_stock')


def find_continue_max_up_stock(data_center, continue_days=5):
    """
    连续5天涨停的股票
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'start_date'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190101')
        if not base_data.empty:
            for j in range(1, continue_days):
                base_data.loc[:, 'pct_chg' + str(j)] = base_data['pct_chg'].shift(-j)
            base_data.loc[:, 'is_success'] = base_data['pct_chg'] > 9
            for j in range(1, continue_days):
                base_data.loc[:, 'is_success'] = (base_data['is_success'] & (base_data['pct_chg' + str(j)] > 9))
            temp_rst = base_data[base_data['is_success']]
            temp_rst = temp_rst.loc[:, ('ts_code', 'trade_date')]
            if not temp_rst.empty:
                temp_rst.loc[:, 'name'] = stock_list[i][2]
                result = result.append(temp_rst)
    file_name = 'success_continue_win_'
    file_name += str(continue_days)
    file_name += '.csv'
    FileOutput.csv_output(None, result, file_name)


def max_up_continue_days(data_center):
    """
    探究一下连续涨停这种现：
    1. 连续多少天涨停最为常见
    2. N天连续涨停之后，后续一天能够涨停的比率
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'start_date', 'continue_days'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20160101')
        if not base_data.empty:
            continue_days = 0
            for j in range(len(base_data)):
                if base_data.at[j, 'pct_chg'] > 9 and base_data.at[j, 'close'] == base_data.at[j, 'high']:
                    temp_dic = {
                        'ts_code': stock_list[i][0],
                        'name': stock_list[i][2]
                    }
                    if continue_days == 0:
                        temp_dic['start_date'] = base_data.at[j, 'trade_date']
                        continue_days += 1
                        temp_dic['continue_days'] = continue_days
                        result = result.append(temp_dic, ignore_index=True)
                    else:
                        continue_days += 1
                        result.at[len(result) - 1, 'continue_days'] = continue_days
                else:
                    continue_days = 0
    if not result.empty:
        # 统计数据
        continue_days_value = result['continue_days']
        min_value = continue_days_value.min()
        max_value = continue_days_value.max()
        extra_content = ''
        for j in range(min_value, max_value + 1):
            j_show = continue_days_value[continue_days_value == j]
            show_num = len(j_show)
            show_rate = show_num / len(continue_days_value)
            extra_content += '涨停' + str(j) + '天出现次数：' + str(show_num) + '， 出现频率：' + str(show_rate) + "\n"
        FileOutput.csv_output(None, result, 'continue_max_up_show_info01.csv', extra_content=extra_content)


def find_target_up_pct_stock(data_center, begin_date, end_date, target_pct):
    """
    寻找在目标期间之内涨幅达到:param target_pct百分比的股票
    """
    result = pandas.DataFrame(
        columns=('ts_code', 'name', 'start_price', 'start_date', 'end_price', 'end_date', 'continue_days'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date)
        if not base_data.empty:
            min_close = 10000
            max_close = 0
            min_close_i = 0
            max_close_i = 0
            end_index = 0
            for j in range(len(base_data)):
                if j <= end_index:
                    continue
                temp_val = base_data.at[j, 'close']
                if temp_val < min_close:
                    min_close = temp_val
                    min_close_i = j
                if temp_val > max_close:
                    max_close = temp_val
                    max_close_i = j
                # 计算上涨百分比
                up_pct = (max_close - min_close) / min_close
                if up_pct > target_pct:
                    temp_dic = {
                        'ts_code': stock_list[j][0],
                        'name': stock_list[j][2],
                        'start_date': base_data.at[min_close_i, 'trade_date'],
                        'end_date': base_data.at[max_close_i, 'trade_date'],
                        'start_price': base_data.at[min_close_i, 'trade_date'],
                    }
                    start_date = base_data.at[min_close_i, 'trade_date']
                    start_date = datetime.date(int(start_date[0:4]), int(start_date[4:6]), int(start_date[6:8]))
                    end_date = base_data.at[min_close_i, 'trade_date']
                    end_date = datetime.date(int(end_date[0:4]), int(end_date[4:6]), int(end_date[6:8]))
                    temp_dic['continue_days'] = (end_date - start_date).days
                    result = result.append(temp_dic, ignore_index=True)
                    end_index = j
    if not result.empty:
        # 计算一下股票的上涨幅度
        result.loc[:, 'up_pct'] = (result['start_price'] - result['end_price']) / result['start_price']
        FileOutput.csv_output(None, result, 'up_history.csv')


def find_v_wave(data_center, down_days=4, down_must_green=False, up_must_high=True, up_must_red=True, allow_s_up=False):
    """
    寻找V型反转的股票，:param down_days天下跌之后，开始上涨，转红了
    :param allow_s_up: 是否允许略微上涨，涨幅不高于1%，并且上涨天数仅限于一天
    :param down_days 连续下跌多少天后反转算是OK
    :param down_must_green 是否下跌的过程当中必须是收盘价低于开盘价
    :param up_must_high 用于指定是否最后一天，开盘价是否高于上一天的收盘价
    :param up_must_red 最后上涨的那一天是否必须是收盘价高于开盘价
    """
    result = pandas.DataFrame(columns=('ts_code', 'name'))
    # 最后一天收盘价比前一天的收盘价或者开盘价当中较高的更高的结果
    high_than_s_max_rst = pandas.DataFrame(columns=('ts_code', 'name'))
    # 最后一天收盘价比前一天的最高价更高的结果集
    high_than_max_rst = pandas.DataFrame(columns=('ts_code', 'name'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190501')
        if not base_data.empty and len(base_data) > (down_days + 1):
            has_up_days = 0
            last_index = len(base_data) - 1
            is_continue_down = False
            for j in range(1, down_days + 1):
                if base_data.at[last_index - j, 'pct_chg'] <= 0 or (allow_s_up and has_up_days <= 0 and
                                                                    base_data.at[last_index - j, 'pct_chg'] <= 1):
                    if down_must_green:
                        is_continue_down = base_data.at[last_index - j, 'close'] < base_data.at[last_index - j, 'open']
                    else:
                        is_continue_down = True
                        if base_data.at[last_index - j, 'pct_chg'] > 0:
                            has_up_days += 1
                else:
                    is_continue_down = False
                    break
            if is_continue_down:
                if not up_must_high and base_data.at[last_index, 'pct_chg'] > 0:
                    result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]},
                                           ignore_index=True)
                    continue
                if base_data.at[last_index, 'open'] > base_data.at[last_index - 1, 'close'] \
                        and base_data.at[last_index, 'pct_chg'] > 0:
                    if up_must_red:
                        if base_data.at[last_index, 'close'] < base_data.at[last_index, 'open']:
                            continue
                    pre_days_m_oc = base_data.at[last_index - 1, 'close'] if base_data.at[last_index - 1, 'close'] > \
                                                                             base_data.at[last_index - 1, 'open'] else \
                        base_data.at[last_index - 1, 'open']
                    if base_data.at[last_index, 'close'] > base_data.at[last_index - 1, 'high']:
                        high_than_max_rst.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]},
                                                 ignore_index=True)
                    elif base_data.at[last_index, 'close'] > pre_days_m_oc:
                        high_than_s_max_rst.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]},
                                                   ignore_index=True)
                    else:
                        result = result.append({'ts_code': stock_list[i][0], 'name': stock_list[i][2]},
                                               ignore_index=True)
    final_result = pandas.DataFrame(columns=('ts_code', 'name'))
    final_result = final_result.append(high_than_max_rst)
    final_result = final_result.append(high_than_s_max_rst)
    final_result = final_result.append(result)
    if not final_result.empty:
        file_name = 'v_wave'
        file_name += '_up_must_high_' if up_must_high else '_not_up_must_high_'
        file_name += "_" + str(down_days) + "_"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str
        file_name += '.csv'
        FileOutput.csv_output(None, final_result, file_name)
    else:
        print("no such stock!")


def find_continue_up_stock(data_center, up_days=4):
    """
    寻找连续上涨的股票：连续N天的时候都是阳线，:param is_real_up控制比前一天收盘价是否是真的上涨
    :param up_days: 连续上涨的天数
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'up_pct'))
    # 最后一天收盘价比前一天的收盘价或者开盘价当中较高的更高的结果
    high_than_s_max_rst = pandas.DataFrame(columns=('ts_code', 'name', 'up_pct'))
    # 最后一天收盘价比前一天的最高价更高的结果集
    high_than_max_rst = pandas.DataFrame(columns=('ts_code', 'name', 'up_pct'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date='20190501')
        if not base_data.empty and len(base_data) > up_days:
            is_continue_up = True
            last_index = len(base_data) - 1
            up_pct = 0
            for j in range(1, up_days):
                if base_data.at[last_index - j, 'close'] > base_data.at[last_index - j, 'open'] and is_continue_up:
                    is_continue_up = True
                    up_pct += base_data.at[last_index - j, 'pct_chg']
                else:
                    is_continue_up = False
            if is_continue_up and base_data.at[last_index, 'close'] > base_data.at[last_index, 'open']:
                up_pct += base_data.at[last_index, 'pct_chg']
                temp_dict = {
                    'ts_code': stock_list[i][0],
                    'name': stock_list[i][2],
                    'up_pct': up_pct
                }
                pre_open = base_data.at[last_index - 1, 'open']
                pre_close = base_data.at[last_index - 1, 'close']
                o_c_max = pre_open if pre_open > pre_close else pre_close
                pre_max = base_data.at[last_index, 'high']
                if base_data.at[last_index, 'close'] > o_c_max:
                    high_than_s_max_rst = high_than_s_max_rst.append(temp_dict, ignore_index=True)
                elif base_data.at[last_index, 'close'] > pre_max:
                    high_than_s_max_rst = high_than_max_rst.append(temp_dict, ignore_index=True)
                else:
                    result = result.append(temp_dict, ignore_index=True)
    high_than_s_max_rst = high_than_s_max_rst.append(high_than_max_rst)
    high_than_s_max_rst = high_than_s_max_rst.append(result)
    if not high_than_s_max_rst.empty:
        file_name = "continue_up" + str(up_days)
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str
        file_name += '.csv'
        FileOutput.csv_output(None, high_than_s_max_rst, file_name)
    else:
        print("no such stock!")


def find_history_down_stock(data_center, begin_date='20191001'):
    """
    寻找历史低值区间的股票，从:param begin_date开始
    亦即最新的价格同该区间内的最低价格相比，上涨幅度不超过5%
    :param data_center: 数据中心
    :param begin_date: 开始日期
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'industry', 'curr_price', 'up_pct'))
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date)
        if not base_data.empty:
            close_price = base_data.loc[:, 'close']
            min_price = close_price.min()
            curr_price = base_data.at[len(base_data) - 1, 'close']
            up_pct = (curr_price - min_price) / min_price
            if up_pct < 0.05:
                temp_dict = {
                    'ts_code': stock_list[i][0],
                    'name': stock_list[i][2],
                    'industry': stock_list[i][4],
                    'curr_price': curr_price,
                    'up_pct': up_pct
                }
                result = result.append(temp_dict, ignore_index=True)
    if not result.empty:
        file_name = "history_down_"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str
        file_name += '.csv'
        FileOutput.csv_output(None, result, file_name)


# 查找类-----------------important!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------
# TODO -- 修正一下，第一个版本，效果似乎不怎么样
def find_down_then_up_stock(data_center, period=60, up_days=3, max_up_pct=0.1):
    """
    查找下跌然后开始上涨流程的股票
    下降，然后开始连续:param up_days天之内上涨的股票，并且在该期间之内的最大涨幅不超过:param max_up_pct代表的百分比
    其中，上涨的含义就是，后一天的收盘价比前一天的收盘价高
    附加条件：1. :param period期间之内，股票达到了最近的最低点
    :param period:
    :param max_up_pct:
    :param up_days:
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=(
        'ts_code', 'name', 'curr_price', 'min_price', 'already_up_pct', 'price_delta'))
    stock_list = data_center.fetch_stock_list()
    end_date = datetime.date.today()
    end_date_str = end_date.strftime("%Y%m%d")
    begin_date = end_date - datetime.timedelta(days=period)
    begin_date_str = begin_date.strftime("%Y%m%d")
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date_str, end_date=end_date_str)
        if not base_data.empty:
            price_series = base_data.loc[:, 'close']
            min_price = price_series.min()
            min_first_index = price_series.idxmin()
            last_day_price = base_data.at[len(base_data) - 1, 'close']
            curr_win_pct = (last_day_price - min_price) / min_price
            # 上涨幅度超过了最高的上涨幅度，所以不能入选
            if curr_win_pct > max_up_pct:
                continue
            # 判定是否是连续:param up_days天内收盘价大于前一天的收盘价
            is_higher_than_last_day = True
            last_index = len(base_data) - 1
            first_index = last_index - up_days - 1 if last_index - up_days - 1 > 0 else 1
            for index in range(first_index, last_index + 1):
                is_higher_than_last_day &= base_data.at[index - 1, 'close'] < base_data.at[index, 'close']
            if is_higher_than_last_day:
                temp_dict = {
                    'ts_code': stock_list[i][0],
                    "name": stock_list[i][2],
                    "curr_price": last_day_price,
                    "min_price": min_price,
                    "already_up_pct": curr_win_pct,
                    "price_delta": last_day_price - min_price
                }
                result = result.append(temp_dict, ignore_index=True)
    if not result.empty:
        file_name = "down_then_up_stock_"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str + '_'
        file_name += str(period)
        file_name += '.csv'
        FileOutput.csv_output(None, result, file_name)


# 查找类-----------------important!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------
def find_long_flow_or_down_then_up(data_center, flow_period=30, max_wave_pct=0.05, exceed_count=10):
    """
    查找长期横盘或者长期微弱下跌，期间上涨幅度最大允许幅度:param max_wave_pct，可以有超过该幅度的上涨， 但是超过该幅度的天数不超过
    :param exceed_count天
    ，然后开始上涨的股票
    :param exceed_count: 上涨超过:param max_wave_pct的最大允许天数
    :param max_wave_pct: 相隔两天的最大波动幅度（仅限上涨）
    :param flow_period: 横盘时间
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=(
        'ts_code', 'name', 'curr_price', 'min_price', 'already_up_pct'))
    stock_list = data_center.fetch_stock_list()
    end_date = datetime.date.today()
    end_date_str = end_date.strftime("%Y%m%d")
    begin_date = end_date - datetime.timedelta(days=flow_period + 10)
    begin_date_str = begin_date.strftime("%Y%m%d")
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date_str, end_date=end_date_str)
        if not base_data.empty and len(base_data) > 5:
            continue_up = True
            curr_exceed_count = 0
            first_close = base_data.at[0, 'close']
            for j in range(len(base_data) - 5):  # 预留五天的时间用于上涨，看着五天内是否突破开始上涨了
                if float(base_data.at[j, 'change']) > max_wave_pct * 100:
                    curr_exceed_count += 1
                if (base_data.at[j, 'close'] - first_close) / first_close > 0.15:
                    # 如果这个不是横盘或者微弱的上涨，那么就pass掉，中间有过大幅上涨
                    continue_up = False # 借用这一条语句推出循环
                    break
            for j in range(len(base_data) - 5, len(base_data)):
                continue_up &= base_data.at[j, 'change'] > 0
            if continue_up:
                price_series = base_data.loc[:, 'close']
                min_price = price_series.min()
                curr_price = base_data.at[len(base_data) - 1, 'close']
                temp_dict = {
                    'ts_code': stock_list[i][0],
                    "name": stock_list[i][2],
                    "curr_price": curr_price,
                    "min_price": min_price,
                    "already_up_pct": (curr_price - min_price) / min_price
                }
                result = result.append(temp_dict, ignore_index=True)
    if not result.empty:
        file_name = "down_or_flow_then_up"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str + '_'
        file_name += '.csv'
        FileOutput.csv_output(None, result, file_name)
    else:
        print("no data!")


# 统计类-----------------important!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------
def find_period_max_win(data_center, period, end_date_str=None):
    """
    从:param end_date到:param period天之前，期间之内盈利由多到少的排名
    :param end_date_str:
    :param data_center:
    :param period:
    :return:
    """
    # 没设置最后日期的话，确定为程序执行当天
    if end_date_str is None:
        end_date_str = datetime.date.today()
        end_date_str = end_date_str.strftime("%Y%m%d")
    result = pandas.DataFrame(columns=(
        'ts_code', 'name', 'curr_price', 'max_win_pct', 'last_win_pct', 'min_price', 'last_day_price', 'price_delta'))
    stock_list = data_center.fetch_stock_list()
    end_date = datetime.date(int(end_date_str[0:4]), int(end_date_str[4:6]), int(end_date_str[6:8]))
    begin_date = end_date - datetime.timedelta(days=period)
    begin_date_str = begin_date.strftime("%Y%m%d")
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date_str, end_date=end_date_str)
        if not base_data.empty:
            temp_dict = {
                'ts_code': stock_list[i][0],
                "name": stock_list[i][2]
            }
            price_series = base_data.loc[:, 'close']
            min_price = price_series.min()
            min_first_index = price_series.idxmin()
            temp_series = price_series.loc[lambda x: x.index > min_first_index]
            max_price = temp_series.max()
            last_day_price = base_data.at[len(base_data) - 1, 'close']
            max_win_pct = (max_price - min_price) / min_price
            last_win_pct = (last_day_price - min_price) / min_price
            temp_dict['curr_price'] = last_day_price
            temp_dict['max_win_pct'] = max_win_pct
            temp_dict['last_win_pct'] = last_win_pct
            temp_dict['min_price'] = min_price
            temp_dict['last_day_price'] = last_day_price
            temp_dict['price_delta'] = last_day_price - min_price
            result = result.append(temp_dict, ignore_index=True)
    if not result.empty:
        file_name = "period_max_win_"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str + '_'
        file_name += str(period)
        file_name += '.csv'
        FileOutput.csv_output(None, result, file_name)


# 统计类-----------------important!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------
def order_stock_by_stdev(data_center, period=30):
    """
    计算最近几天的标准差，然后输出，找到波动率大的股票，看下能否做短期套利
    :param period: 计算期间
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=(
        'ts_code', 'name', 'curr_price', 'last_win_pct', 'stdev'))
    stock_list = data_center.fetch_stock_list()
    end_date = datetime.date.today()
    end_date_str = end_date.strftime("%Y%m%d")
    begin_date = end_date - datetime.timedelta(days=period)
    begin_date_str = begin_date.strftime("%Y%m%d")
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date_str, end_date=end_date_str)
        if not base_data.empty:
            change_series = base_data.loc[:, 'change']
            close_series = base_data.loc[:, 'close']
            min_price = close_series.min()
            last_price = base_data.at[len(base_data) - 1, 'close']
            stdev = np.std(change_series, ddof=1)
            last_win_pct = (last_price - min_price) / min_price
            temp_dict = {
                'ts_code': stock_list[i][0],
                "name": stock_list[i][2],
                "curr_price": last_price,
                "min_price": min_price,
                "last_win_pct": last_win_pct,
                "stdev": stdev
            }
            result = result.append(temp_dict, ignore_index=True)
    if not result.empty:
        file_name = "stdev_order_"
        now_time = datetime.datetime.now()
        now_time_str = now_time.strftime('%Y%m%d')
        file_name += '_' + now_time_str + '_'
        file_name += str(period)
        file_name += '.csv'
        FileOutput.csv_output(None, result, file_name)


# def find_max_up_max_stock(data_center):

