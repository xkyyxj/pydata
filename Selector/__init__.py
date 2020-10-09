# 股票选择器
# 选择符合条件的股票，然后日常验证
from Selector.Utils import merge_table_data
import pandas_ta as ta

MIXED = 1           # 多表合并取交集
MERGE_ALL = 2       # 多表合并取并集


def select_from_in_low_by_indicator(data_center):
    pass


def common_select_from_table_by_indi(data_center, tables, mergeType=MIXED):
    fast = 12
    slow = 26
    signal = 9
    ret_data = merge_table_data(data_center, tables)
    if ret_data is None or len(ret_data) == 0:
        return
    for i in range(len(ret_data)):
        temp_stock_code = ret_data[i]
        base_data = data_center.fetch_base_data(temp_stock_code)
        close = base_data['close']
        macd_ret = ta.macd(close, fast, slow, signal=signal)
    # 此处的指标先写死吧
    pass