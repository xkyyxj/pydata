import datetime
from Data.DataCenter import DataCenter
from Output import FileOutput
from Simulation.simulate import Simulate


def single_process_period_simulate(stock_codes, judge_time, queue, dir_name, period=7):
    """
    单进程的模拟计算过程:计算当前买入，:param period天之后卖出，盈利情况
    :param stock_codes:
    :param dir_name:
    :param period:
    :return:
    """
    simulate = PeriodSimulate(stock_codes, dir_name, period)
    sum_dict = simulate()
    queue.put(sum_dict)


class PeriodSimulate(Simulate):
    def __init__(self, stock_codes, judge_out_name, period=7):
        super().__init__(stock_codes, judge_out_name)
        self.period = period

    @staticmethod
    def get_sum_field():
        return ['win_pct_less_than_50', 'win_pct_bigger_than_50', 'win_pct_bigger_than_60', 'win_pct_bigger_than_70']

    def __call__(self, *args, **kwargs):
        self.data_center = DataCenter()
        if self.stock_codes is None or len(self.stock_codes) == 0:
            return
        # 统计所有的股票，在:param peirod期间之内的盈利情况
        sum_dict = {
            'win_pct_less_than_50': 0,
            'win_pct_bigger_than_50': 0,
            'win_pct_bigger_than_60': 0,
            'win_pct_bigger_than_70': 0,
        }
        for stock_code in self.stock_codes:
            base_infos = self.data_center.fetch_base_data(stock_code)
            shift_close = base_infos['close'].shift(self.period)
            win_pct = (shift_close - base_infos['close']) / base_infos['close']
            base_infos['win_pct'] = win_pct
            base_infos.drop(['vol', 'amount', 'pre_close', 'change'], axis=1, inplace=True)
            if not base_infos.empty:
                count_percent = len(base_infos[base_infos['win_pct'] > 0]) / len(base_infos)
                sum_dict['win_pct_less_than_50'] = sum_dict['win_pct_less_than_50'] + 1 if count_percent < 0.5 \
                    else sum_dict['win_pct_less_than_50']
                sum_dict['win_pct_bigger_than_50'] = sum_dict['win_pct_bigger_than_50'] + 1 if count_percent >= 0.5 \
                    else sum_dict['win_pct_bigger_than_50']
                sum_dict['win_pct_bigger_than_60'] = sum_dict['win_pct_bigger_than_60'] + 1 if count_percent >= 0.6 \
                    else sum_dict['win_pct_bigger_than_60']
                sum_dict['win_pct_bigger_than_70'] = sum_dict['win_pct_bigger_than_70'] + 1 if count_percent >= 0.7 \
                    else sum_dict['win_pct_bigger_than_70']
                sum_str = "win count percent is " + str(count_percent)
                file_name = "period_simulate_" + stock_code
                now_time = datetime.datetime.now()
                now_time_str = now_time.strftime('%Y%m%d')
                file_name += '_' + now_time_str
                file_name += '.csv'
                FileOutput.csv_output(None, base_infos, file_name, spe_dir_name=self.judge_out_name,
                                      extra_content=sum_str)
        return sum_dict
