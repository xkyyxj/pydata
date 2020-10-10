# 股票选择器
# 选择符合条件的股票，然后日常验证
from Selector.SelectorStrategy import SelectorStrategy
from Selector.Utils import merge_table_data
import pandas_ta as ta

from Selector.macd import MACDSelector

MIXED = 1           # 多表合并取交集
MERGE_ALL = 2       # 多表合并取并集


def select_from_in_low_by_indicator(data_center, indicators=None):
    # if indicators is None:
    #     return

    stock_list = data_center.common_query("select ts_code from in_low")
    strategy = SelectorStrategy(data_center, strategy=[MACDSelector])
    real_stock_list = []
    for item in stock_list:
        real_stock_list.append(item[0])
    strategy.set_pre_scope(real_stock_list)
    result = strategy.get_result()
    if result is not None:
        strategy.flush_result_to_db(result)


def common_select_from_table_by_indicators(data_center, tables, indicators, merge_type=MIXED):
    fast = 12
    slow = 26
    signal = 9
    ret_data = merge_table_data(data_center, tables)
    if ret_data is None or len(ret_data) == 0:
        return
    for indicator in indicators:
        pass