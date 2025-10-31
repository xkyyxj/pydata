#! /usr/bin/env python
"""
Assistance Utils : Find has up
"""
import datetime
import pandas

def find_has_up_by_windows(data_center, window, begin_date = datetime.datetime.now()):
    """
    Find Has Up, in the period of window, continue up
    """
    begin_date = begin_date - datetime.timedelta(days=window)
    begin_date = begin_date.strftime("%Y%m%d")
    all_data = data_center.fetch_base_until_now_no_tushare(begin_date)
    # calculate last win percent
    first_day_price = all_data.iloc[all_data.groupby('ts_code')['trade_date'].idxmin()][['ts_code', 'close', 'trade_date']]
    all_data = pandas.merge(all_data, first_day_price, on='ts_code', how='left', suffixes=('', '_first_day'))
    all_data['last_pct'] = (all_data['close'] - all_data['close_first_day']) / all_data['close_first_day']
    all_data.sort_values(['ts_code', 'trade_date'], inplace=True)
    result = all_data.groupby('ts_code')['close'].apply(lambda x: x.is_monotonic_increasing)

    up_stocks = result[result == True].index
    filtered_data = all_data[all_data['ts_code'].isin(up_stocks)]
    filtered_data = filtered_data.loc[filtered_data.groupby('ts_code')['trade_date'].idxmax()]
    filtered_data.sort_values(by = 'last_pct', inplace=True)
    return filtered_data
    
def find_has_up_by_percent(data_center, window, up_pct, begin_date = datetime.datetime.now()):
    """
    Find Has Up, with target percent in the last day
    """
    begin_date = begin_date - datetime.timedelta(days=window)
    begin_date = begin_date.strftime("%Y%m%d")
    all_data = data_center.fetch_base_until_now_no_tushare(begin_date)
    # calculate last win percent
    first_day_price = all_data.iloc[all_data.groupby('ts_code')['trade_date'].idxmin()][
        ['ts_code', 'close', 'trade_date']]
    all_data = pandas.merge(all_data, first_day_price, on='ts_code', how='left', suffixes=('', '_first_day'))
    all_data['last_pct'] = (all_data['close'] - all_data['close_first_day']) / all_data['close_first_day']
    filtered_data = all_data.iloc[all_data.groupby('ts_code')['trade_date'].idxmax()]
    filtered_data = filtered_data[filtered_data['last_pct'] >= up_pct]
    return filtered_data