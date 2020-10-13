import pandas
import pandas_ta as ta
import datetime

from Data.DataCenter import DataCenter


class MACDSelector:
    """
    通过MACD指标来选择对应的股票
    """

    BUY_SIGNAL_PERIOD = 2   # 两天之内发出买入信号的话，该只股票视为可买入

    def __init__(self, ts_codes=None):
        if ts_codes is None:
            ts_codes = []
        self.ts_codes = ts_codes
        self.fast = 12
        self.slow = 26
        self.signal = 9

    def set_fast(self, fast):
        self.fast = fast

    def set_slow(self, slow):
        self.slow = slow

    def set_signal(self, signal):
        self.signal = signal

    def __call__(self, *args, **kwargs):
        self.data_center = DataCenter()
        if len(self.ts_codes) == 0:
            return

        result = pandas.DataFrame(columns=('ts_code', 'in_price', 'in_date', 'origin_from', 'in_reason', 'finished', 'manual'))
        for ts_code in self.ts_codes:
            field_suffix = "_" + str(self.fast) + "_" + str(self.slow) + "_" + str(self.signal)
            macd_field_name = 'MACD' + field_suffix
            histogram_field_name = 'MACDh' + field_suffix
            signal_field_name = 'MACDs' + field_suffix
            base_infos = self.data_center.fetch_base_data(ts_code)
            if base_infos is None or len(base_infos) == 0:
                continue
            close = base_infos['close']
            macd_ret = ta.macd(close, self.fast, self.slow, signal=self.signal)
            if len(macd_ret) < 2:
                continue
            rst_length = len(macd_ret)
            start_index = min(rst_length, self.BUY_SIGNAL_PERIOD)
            start_index = rst_length - start_index
            low_flag = False
            for i in range(start_index, rst_length):
                if macd_ret.at[i, macd_field_name] is not None and macd_ret.at[i, signal_field_name] is not None and \
                        macd_ret.at[i, macd_field_name] < macd_ret.at[i, signal_field_name]:
                    low_flag = True
                else:
                    if low_flag:
                        now_time = datetime.datetime.now()
                        now_time_str = now_time.strftime('%Y%m%d')
                        temp_dict = {
                            'ts_code': ts_code,
                            'in_price': close[i],
                            'in_date': base_infos.at[start_index, 'trade_date'],
                            'origin_from': 'macd',
                            'in_reason': 'macd金叉',
                            'finished': 0,
                            'manual': 0
                        }
                        result = result.append(temp_dict, ignore_index=True)
        return result


