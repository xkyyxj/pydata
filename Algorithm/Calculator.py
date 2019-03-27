#!/usr/bin/env python3
"""
计算逻辑
"""
import datetime
import pandas
import Output.FileOutput as FileOutput


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


def find_has_up_some(data_center):
    """
    查找已经上涨了一部分的股票，目标如下：
    1. 最近10天上涨了20%
    2. 一直处于上涨的走势当中，下跌都是微跌
    :param data_center:
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
            temp_base_data = temp_base_data.loc[0:10]
            pre_day_af_close = temp_base_data['af_close'].shift(1)
            temp_base_data['af_pct_chg'] = (temp_base_data['af_close'] - pre_day_af_close) / pre_day_af_close

            # 期间之内的下跌幅度不能超过4%
            down_days = temp_base_data[temp_base_data['af_pct_chg'] < 0.04]
            if down_days.empty:
                continue

            # 最后一天相比于这期间的最低价，已经上涨了20%
            min_price = temp_base_data['af_close'].min()
            curr_price = temp_base_data.at[0, 'af_close']
            up_pct = (curr_price - min_price) / min_price
            if up_pct > 0.2:
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
            b_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9)
            c_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            d_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9) & (base_data['next_high_pct'] > 0.09)
            e_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9) & \
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
    概率统计如下：
    1. B / A    意义：第二天开盘涨停价买入，最终有多少几率当天挂掉
    2. C / A    意义：第二天开盘涨停价买入，最终有多少几率当天成功
    3. D / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    4. E / S    意义：统计一下，第二天开盘没有涨停话，追入的意义有多大
    5. C / S    意义：开盘涨停最终导致收盘也涨停的概率
    6. S / F    意义：统计一下，第一天涨停后，第二天涨停的概率是多大
    7. A / F    意义：第一天开盘涨停后，第二天开盘涨停的比率如何
    8. G / F    意义：第一天开盘涨停后，第二天开盘未涨停的比率如何
    同时对于各只股票单独统计一下，然后输出成为一个CSV文件
    :param data_center:
    :return:
    """
    result = pandas.DataFrame(columns=('ts_code', 'name', 'trade_date', 'is_next_day_max_up',
                                       'buy_price', 'sold_price', 'is_win_10', 'is_win_7', 'is_win_3'))
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

    # 两日涨停时相关统计数据
    begin_max_suc_win_num = 0   # 第二天开盘涨停并且收盘涨停并且盈利的天数 / 第二天开盘涨停并且收盘涨停的天数
    begin_max_suc_win3_num = 0  # 第二天开盘涨停并且收盘涨停并且盈利3%的天数 / 第二天开盘涨停并且收盘涨停的天数
    begin_max_suc_win7_num = 0
    begin_max_suc_win10_num = 0
    begin_down_max_win_num = 0  # 第二天开盘未涨停并且收盘涨停并且盈利的天数 / 第二天开盘未涨停并且收盘涨停的天数
    begin_down_max_win3_num = 0
    begin_down_max_win7_num = 0
    begin_down_max_win10_num = 0
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
            base_data.loc[:, 'sold_price'] = base_data['open'].shift(-2)

            if start_days is not None:
                end_days = datetime.datetime.now().strftime("%Y%m%d") if end_days is None else end_days
                base_data = base_data[(base_data['trade_date'] <= end_days) & (base_data['trade_date'] >= start_days)]

            s_index = (base_data['pct_chg'] > 9) & (base_data['next_pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9)
            a_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9)
            b_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9)
            c_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] > 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            d_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] < 9) & (base_data['next_high_pct'] > 0.09)
            e_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9) & \
                      (base_data['next_pct_chg'] > 9)
            f_index = (base_data['pct_chg'] > 9) & (base_data['pre_pct_chg'] < 9)
            g_index = (base_data['pct_chg'] > 9) & (base_data['next_open_pct'] < 0.09) & (base_data['pre_pct_chg'] < 9)
            base_data.loc[:, 'c_index'] = c_index
            base_data.loc[:, 'e_index'] = e_index
            S_num += len(s_index[s_index])
            A_num += len(a_index[a_index])
            B_num += len(b_index[b_index])
            C_num += len(c_index[c_index])
            D_num += len(d_index[d_index])
            E_num += len(e_index[e_index])
            F_num += len(f_index[f_index])
            G_num += len(g_index[g_index])

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
                                                 'buy_price', 'sold_price', 'is_win_10', 'is_win_7', 'is_win_3',
                                                 'win_pct', 'is_two_max_up', 'is_begin_max_end_max',
                                                 'is_begin_down_end_max'))
            temp_value = base_data[s_index | b_index | d_index]
            # 以次日开盘价买入
            if not temp_value.empty:
                temp_value.loc[:, 'win_pct'] = (temp_value['sold_price'] - temp_value['next_open']) \
                                               / temp_value['next_open']
                temp_rst.loc[:, 'ts_code'] = temp_value['ts_code']
                temp_rst.loc[:, 'name'] = stock_list[i][2]
                temp_rst.loc[:, 'trade_date'] = temp_value['trade_date']
                temp_rst.loc[:, 'is_next_day_max_up'] = temp_value['next_open_pct'] > 0.09
                temp_rst.loc[:, 'buy_price'] = temp_value['next_open']
                temp_rst.loc[:, 'sold_price'] = temp_value['sold_price']
                temp_rst.loc[:, 'is_win_10'] = temp_value['win_pct'] > 0.09
                temp_rst.loc[:, 'is_win_7'] = temp_value['win_pct'] > 0.07
                temp_rst.loc[:, 'is_win_3'] = temp_value['win_pct'] > 0.03
                temp_rst.loc[:, 'win_pct'] = temp_value['win_pct']
                temp_rst.loc[:, 'is_two_max_up'] = temp_value['next_pct_chg'] > 9
                temp_rst.loc[:, 'is_begin_max_end_max'] = temp_value['c_index']
                temp_rst.loc[:, 'is_begin_down_end_max'] = temp_value['e_index']
                result = result.append(temp_rst)
    if result is not None:
        # 统计一下数据
        begin_max_fail = B_num / A_num
        begin_max_success = C_num / A_num
        begin_down_fail = D_num / S_num
        begin_down_success = E_num / S_num
        begin_max_final_suc = C_num / S_num

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
                         str(begin_down_max_win10_num / E_num) + "\n" + "\n"
        extra_content += "两日涨停获利概率：" + str(win_pct) + "\n"
        extra_content += "两日涨停获利超过3%概率：" + str(win_3_pct) + "\n"
        extra_content += "两日涨停获利超过7%概率：" + str(win_7_pct) + "\n"
        extra_content += "两日涨停获利超过10%概率：" + str(win_10_pct) + "\n"
        FileOutput.csv_output(None, result, 'total_max_up_rate.csv',
                              extra_content=extra_content, spe_dir_name='max_up_rate_with_buy_03')
    if per_stock_rst is not None and not per_stock_rst.empty:
        FileOutput.csv_output(None, result, 'per_stock_max_up_rate.csv', spe_dir_name='max_up_rate_with_buy_03')


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
    """
    if not base_data.empty:
        base_data.loc[:, 'ema26'] = 0
        base_data.loc[:, 'ema12'] = 0
        base_data.loc[:, 'diff'] = 0
        base_data.loc[:, 'dea'] = 0
        base_data.loc[:, 'bar'] = 0
        for i in range(1, len(base_data)):
            base_data.at[i, 'ema12'] = (11 * base_data.at[i - 1, 'ema12'] + 2 * base_data.at[i, 'close']) / 13
            base_data.at[i, 'ema26'] = (25 * base_data.at[i - 1, 'ema26'] + 2 * base_data.at[i, 'close']) / 27
            base_data.at[i, 'diff'] = base_data.at[i, 'ema12'] - base_data.at[i, 'ema26']
            base_data.at[i, 'dea'] = (7 * base_data.at[i - 1, 'dea'] + 2 * base_data.at[i, 'diff']) / 9
        base_data.loc[:, 'bar'] = base_data['diff'] - base_data['dea']
        base_data.loc[:, 'bar'] = base_data['bar'] * 2
    return base_data
 