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
    result = pandas.DataFrame(columns=base_data.columns)
    if len(base_data) <= 0:
        return result
    min_index = 0
    min_price = base_data.at[min_index, column]
    for index in range(1, len(base_data)):
        temp_price = base_data.at[index, column]
        if temp_price < min_price:
            pass
    for index in range(windows, len(base_data)):

        pass
    return result
