import pandas
import datetime
from Data.DataCenter import DataCenter
from Output import FileOutput
import multiprocessing
from multiprocessing import queues


def multi_process_simulate(stock_codes, judge_time, out_dir_name):
    """
    多进程进行模拟计算，充分发挥多核性能
    :param out_dir_name:
    :param stock_codes: 所有的股票编码
    :param judge_time: 判定时机的函数
    :return:
    """
    cpu_num = multiprocessing.cpu_count()
    each_cpu_load = len(stock_codes) // cpu_num
    queue = queues.Queue(20, ctx=multiprocessing)

    final_sum_dict = {
        'win_num': 0,
        'lose_num': 0,
        'max_win_pct': 0,
        'max_lost_pct': 0,
        'ave_win_mny': 0
    }
    for i in range(cpu_num):
        temp_stock_code = []
        for j in range(i * each_cpu_load, (i + 1) * each_cpu_load):
            temp_stock_code.append(stock_codes[j])

        if i == cpu_num - 1:
            for j in range(cpu_num * each_cpu_load, len(stock_codes)):
                temp_stock_code.append(stock_codes[j])
        process_i = multiprocessing.Process(target=single_cpu_core_simulate, args=(temp_stock_code, judge_time, queue,
                                                                                   out_dir_name))
        process_i.start()
        process_i.join()
    for i in range(cpu_num):
        temp_dict = queue.get()
        final_sum_dict['win_num'] = final_sum_dict['win_num'] + temp_dict['win_num']
        final_sum_dict['lose_num'] = final_sum_dict['lose_num'] + temp_dict['lose_num']
        final_sum_dict['max_win_pct'] = temp_dict['max_win_pct'] \
            if final_sum_dict['max_win_pct'] > temp_dict['max_win_pct'] else final_sum_dict['max_win_pct']
        final_sum_dict['max_lost_pct'] = temp_dict['max_lost_pct'] \
            if final_sum_dict['max_lost_pct'] <= temp_dict['max_lost_pct'] else final_sum_dict['max_lost_pct']
    sum_frame = pandas.DataFrame(columns=('win_num', 'lose_num', 'max_win_pct', 'max_lost_pct'))
    sum_frame = sum_frame.append(final_sum_dict, ignore_index=True)
    FileOutput.csv_output(None, sum_frame, "final_sum_info", spe_dir_name=out_dir_name)
    pass


def single_cpu_core_simulate(stock_codes, judge_time, queue, out_dir_name):
    """
    创建一个Simulate对象，然后做模拟操作
    :param out_dir_name:
    :param queue:
    :param stock_codes:
    :param judge_time:
    :return:
    """
    simulate = Simulate(stock_codes, out_dir_name, judge_time=judge_time)
    simulate.set_registry(judge_time)
    simulate.set_init_mny(100000)
    sum_dict = simulate()
    queue.put(sum_dict)


class Simulate:
    def __init__(self, stock_codes, judge_out_name, judge_time=None):
        self.__initial_mny = None
        self.__stock_codes = stock_codes
        self.__judge_time = judge_time
        self.__hold_num = 0  # 持仓手数
        self.__left_mny = 0  # 剩余金额
        self.__judge_out_name = judge_out_name  # 判定输出目录名称

    def set_registry(self, judge_time):
        self.__judge_time = judge_time

    def set_judge_name(self, judge_name):
        self.__judge_out_name = judge_name

    def set_init_mny(self, init_mny):
        """
        设置初始金额，金额为RMB
        :param init_mny:
        :return:
        """
        self.__initial_mny = init_mny
        self.__left_mny = init_mny

    def reset(self):
        self.__left_mny = self.__initial_mny
        self.__hold_num = 0

    def __call__(self, *args, **kwargs):
        # TODO -- 看一下Python的数值计算方式，是否有小数？？？
        self.__data_center = DataCenter()
        final_sum_dict = {
            'win_num': 0,
            'lose_num': 0,
            'max_win_pct': 0,
            'max_lost_pct': 0,
            'ave_win_mny': 0
        }
        trade_rst = pandas.DataFrame(columns=('ts_code', 'trade_times', 'final_win', 'win_pct'))
        for stock_code in self.__stock_codes:
            trade_rst_dict = {
                'ts_code': stock_code,
                'trade_times': 0,
                'final_win': 0,
                'win_pct': 0
            }
            base_infos = self.__data_center.fetch_base_data(stock_code)
            ret_time = self.__judge_time(base_infos)
            if ret_time is None:
                continue
            detail_trade_info = pandas.DataFrame(
                columns=('ts_code', 'curr_close', 'trade_date', 'trade_num', 'hold_num', 'hold_mny',
                         'total_mny'))
            for i in range(len(ret_time)):
                if ret_time.at[i, 'flag'] == 1:
                    # 默认买入10%
                    buy_pct = ret_time.at[i, 'percent'] if ret_time.at[i, 'percent'] > 0 else 0.1
                    buy_mny = self.__initial_mny * buy_pct
                    buy_mny = self.__left_mny if self.__left_mny < buy_mny else buy_mny
                    buy_num = buy_mny / base_infos.at[i, 'close']
                    self.__hold_num = self.__hold_num + buy_num
                    self.__left_mny = self.__left_mny - buy_mny
                    hold_mny = self.__hold_num * base_infos.at[i, 'close']
                    # 记录买入信息
                    temp_dict = {
                        'ts_code': stock_code,
                        'trade_date': base_infos.at[i, 'trade_date'],
                        'trade_num': buy_num,
                        'hold_num': self.__hold_num,
                        'hold_mny': hold_mny,
                        'total_mny': self.__left_mny + hold_mny,
                        'curr_close': base_infos.at[i, 'close']
                    }
                    detail_trade_info = detail_trade_info.append(temp_dict, ignore_index=True)
                    trade_rst_dict['trade_times'] = trade_rst_dict['trade_times'] + 1
                elif ret_time.at[i, 'flag'] == -1:
                    # 默认卖出持仓数量的10%
                    sold_pct = ret_time.at[i, 'percent'] if ret_time.at[i, 'percent'] > 0 else 0.1
                    sold_num = self.__hold_num * sold_pct
                    sold_num = sold_num / 100 * 100
                    self.__hold_num = self.__hold_num - sold_num
                    sold_mny = sold_num * base_infos.at[i, 'close']
                    self.__left_mny = self.__left_mny + sold_mny
                    hold_mny = self.__hold_num * base_infos.at[i, 'close']
                    # 记录卖出信息
                    temp_dict = {
                        'ts_code': stock_code,
                        'trade_date': base_infos.at[i, 'trade_date'],
                        'trade_num': sold_num,
                        'hold_num': self.__hold_num,
                        'hold_mny': hold_mny,
                        'total_mny': self.__left_mny + hold_mny,
                        'curr_close': base_infos.at[i, 'close']
                    }
                    detail_trade_info = detail_trade_info.append(temp_dict, ignore_index=True)
                    trade_rst_dict['trade_times'] = trade_rst_dict['trade_times'] + 1
            if not detail_trade_info.empty:
                file_name = "simulate_" + str(stock_code)
                now_time = datetime.datetime.now()
                now_time_str = now_time.strftime('%Y%m%d')
                file_name += '_' + now_time_str
                file_name += '.csv'
                FileOutput.csv_output(None, detail_trade_info, file_name, spe_dir_name=self.__judge_out_name)
            else:
                print("no such stock!")
            self.reset()
            if detail_trade_info.empty:
                continue
            # 统计该只股票的最终盈利
            last_win = detail_trade_info.at[(len(detail_trade_info) - 1), 'total_mny'] - self.__initial_mny
            last_win_pct = last_win / self.__initial_mny
            trade_rst_dict['final_win'] = last_win
            trade_rst_dict['win_pct'] = last_win_pct
            trade_rst = trade_rst.append(trade_rst_dict, ignore_index=True)

            # 统计多只股票的汇总
            final_sum_dict['win_num'] = final_sum_dict['win_num'] + 1 if last_win > 0 else final_sum_dict['win_num']
            final_sum_dict['lose_num'] = final_sum_dict['lose_num'] + 1 if last_win <= 0 else final_sum_dict['lose_num']
            final_sum_dict['max_win_pct'] = last_win_pct if last_win_pct > final_sum_dict['max_win_pct'] else \
                final_sum_dict['max_win_pct']
            final_sum_dict['max_lost_pct'] = last_win_pct if last_win_pct <= final_sum_dict['max_lost_pct'] else \
                final_sum_dict['max_lost_pct']

        sum_str = "获利数量：" + str(final_sum_dict['win_num']) + " 损失数量：" + str(final_sum_dict['lose_num']) \
                  + " 最大获利百分比：" + str(final_sum_dict['max_win_pct']) + " 最大损失百分比：" + \
                  str(final_sum_dict['max_lost_pct'])
        if not trade_rst.empty:
            file_name = "simulate_sum_" + self.__stock_codes[0]
            now_time = datetime.datetime.now()
            now_time_str = now_time.strftime('%Y%m%d')
            file_name += '_' + now_time_str
            file_name += '.csv'
            FileOutput.csv_output(None, trade_rst, file_name, spe_dir_name=self.__judge_out_name, extra_content=sum_str)
        return final_sum_dict
