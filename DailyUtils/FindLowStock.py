#! /usr/bin/env python
"""
日常辅助程序之一：查找价格较低的股票
"""
import datetime
import pandas


def is_target_lowest(base_data, windows=220, target_day=None):
    if target_day is not None and len(target_day) > 0 and not target_day.isspace():
        start_index = len(base_data) - windows if len(base_data) - windows > 0 else 0
        min_price_index = start_index
        min_price = base_data.at[min_price_index, 'close']
        for i in range(start_index, len(base_data)):
            if base_data.at[i, 'close'] < min_price:
                min_price_index = i
                min_price = base_data.at[min_price_index, 'close']
        if base_data.at[min_price_index, 'trade_date'] == target_day:
            return True
        else:
            # 添加一点冗余，如果是最低点在target_day的前几天也是可以考虑的
            min_price_day_str = base_data.at[min_price_index, 'trade_date']
            min_price_day = datetime.date(min_price_day_str[0:4], min_price_day_str[4:6], min_price_day_str[6:8])
            target_day_date = datetime.date(target_day[0:4], target_day[4:6], target_day[6:8])
            time_between = target_day_date - min_price_day
            if time_between.days < 5:
                return True
            return False


def find_low_record(base_data, windows=365, column='close'):
    min_index_set = set()
    result = pandas.DataFrame(columns=base_data.columns)
    j = 0
    for index in range(len(base_data)):
        min_index = index
        min_price = base_data.at[index, column]
        for j in range(index, index + windows):
            if j < len(base_data) and base_data.at[j, column] < min_price:
                min_price = base_data.at[j, column]
                min_index = j
        if min_index not in min_index_set and j == index + windows - 1:
            min_index_set.add(min_index)
            result = result.append(base_data.iloc[min_index])
    return result


def find_low_record_adv(base_data, windows=365, column='close'):
    """
    该函数用于确定当前天之前的@param windows天数之内(包含当前天)，当前天是否是该期间内的最低价
    返回一个pandas.Series，如果是最低价，那么相应index上就为True， 否则为False
    PS: 上述find_low_record是个什么垃圾版本，为什么会有这么垃圾的代码？？？？？
    :param base_data:
    :param windows:
    :param column:
    :return:
    """
    ret_value = pandas.Series()
    if base_data is None or len(base_data) == 0:
        return ret_value
    min_value = base_data.rolling(windows)[column].min()
    return base_data[column] == min_value


def find_low_and_has_up(base_data, up_percent, last_days, column):
    """
    计算最低价位时又在@param last_days之内上涨了@param up_percent比例的情况,
    同时上涨比例也要小于@param up_percent加上20%
    返回有一个Series，符合上述情况的达到@param up_percent那一天设为True，否则为False
    :param base_data:
    :param up_percent:
    :param last_days:
    :param column:
    :return:
    """
    low_index = find_low_record_adv(base_data, column=column)
    target_day_price = base_data[column].shift(-last_days)
    base_data['days_up_percent'] = (target_day_price - base_data[column]) / base_data[column]
    ret_index = base_data['days_up_percent'] >= up_percent & base_data['days_up_percent'] <= up_percent + 0.2
    return ret_index & low_index


def find_has_up_stocks(data_center, windows=3, target_pct=0.2):
    """
    查找已经上涨的股票，基于以下理念：既然追涨杀跌，那么就看涨势启动的股票好了
    :param data_center: 数据中心对象
    :param windows: 连续windows天内上涨即算上涨
    :param target_pct: @param windows之内上涨到的百分比加入到最终结果集当中
    :return:
    """
    begin_date = datetime.datetime.now()
    begin_date = begin_date - datetime.timedelta(days=20)
    begin_date = begin_date.strftime("%Y%m%d")
    end_date = datetime.datetime.now()
    end_date = end_date.strftime("%Y%m%d")
    result = pandas.Series()
    stock_list = data_center.fetch_stock_list()
    for i in range(len(stock_list)):
        base_data = data_center.fetch_base_data_pure_database(stock_list[i][0],
                                                              begin_date=begin_date, end_date=end_date)
        if len(base_data > windows):
            three_after = base_data['af_close'].shift(windows)
            base_data['w_up_pct'] = (three_after - base_data['af_close']) / base_data['af_close']
            up_pct_before=  base_data['w_up_pct'].shift(windows)
            if up_pct_before.iloc[len(up_pct_before) - 1] > target_pct:
                result.append(pandas.Series(stock_list[i][0]))
    return result


def find_term_stock(data_center):
    """
    寻找断片的股票
    """
    # TODO -- 后续补完
    pass

