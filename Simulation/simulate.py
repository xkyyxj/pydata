import pandas
import datetime
from Data.DataCenter import DataCenter
from Output import FileOutput
import multiprocessing
from multiprocessing import queues


class MultiProcessor:
    def __init__(self, sum_field=[], max_field=[], min_field=[]):
        self.sum_field = sum_field
        self.max_field = max_field
        self.min_field = min_field

    def multi_process_simulate(self, stock_codes, judge_time, out_dir_name, target_function=None):
        """
        多进程进行模拟计算，充分发挥多核性能
        :param target_function: 目标调用函数
        :param out_dir_name:
        :param stock_codes: 所有的股票编码
        :param judge_time: 判定时机的函数
        :return:
        """
        cpu_num = multiprocessing.cpu_count() + 4
        each_cpu_load = len(stock_codes) // cpu_num
        queue = queues.Queue(20, ctx=multiprocessing)

        if target_function is None:
            raise Exception('没有传入模拟函数！！！')

        final_sum_dict = {}
        for item in self.sum_field:
            final_sum_dict[item] = 0
        for item in self.max_field:
            final_sum_dict[item] = 0
        for item in self.min_field:
            final_sum_dict[item] = 0
        for i in range(cpu_num):
            temp_stock_code = []
            for j in range(i * each_cpu_load, (i + 1) * each_cpu_load):
                temp_stock_code.append(stock_codes[j])

            if i == cpu_num - 1:
                for j in range(cpu_num * each_cpu_load, len(stock_codes)):
                    temp_stock_code.append(stock_codes[j])
            process_i = multiprocessing.Process(target=target_function, args=(temp_stock_code, judge_time, queue,
                                                                                       out_dir_name))
            process_i.start()
            process_i.join()
        for i in range(cpu_num):
            temp_dict = queue.get()
            for item in self.sum_field:
                final_sum_dict[item] = final_sum_dict[item] + temp_dict[item]
            for item in self.max_field:
                final_sum_dict[item] = temp_dict[item] \
                    if final_sum_dict[item] < temp_dict[item] \
                    else final_sum_dict[item]
            for item in self.min_field:
                final_sum_dict[item] = temp_dict[item] \
                    if final_sum_dict[item] is None or final_sum_dict[item] > temp_dict[item] \
                    else final_sum_dict[item]
        all_field = self.sum_field
        all_field.extend(self.max_field)
        all_field.extend(self.min_field)
        sum_frame = pandas.DataFrame(columns=tuple(all_field))
        sum_frame = sum_frame.append(final_sum_dict, ignore_index=True)
        FileOutput.csv_output(None, sum_frame, "final_sum_info", spe_dir_name=out_dir_name)


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
    BUY_FLAG = 1
    SOLD_FLAG = -1
    DO_NOTHING = 0
    MULTI_INDI_BETWEEN = 5 # 如果有多种指标，多少天之内均发出买入信号就决定买入

    def __init__(self, stock_codes, judge_out_name, judge_time=None):
        self.initial_mny = None
        self.stock_codes = stock_codes
        self.judge_time = judge_time
        self.hold_num = 0  # 持仓手数
        self.left_mny = 0  # 剩余金额
        self.judge_out_name = judge_out_name  # 判定输出目录名称

    def set_registry(self, judge_time):
        self.judge_time = judge_time

    def set_judge_name(self, judge_name):
        self.judge_out_name = judge_name

    def set_init_mny(self, init_mny):
        """
        设置初始金额，金额为RMB
        :param init_mny:
        :return:
        """
        self.initial_mny = init_mny
        self.left_mny = init_mny

    def reset(self):
        self.left_mny = self.initial_mny
        self.hold_num = 0

    def __call__(self, *args, **kwargs):
        # TODO -- 看一下Python的数值计算方式，是否有小数？？？
        self.data_center = DataCenter()
        default_ope_pct = 0.1
        final_sum_dict = {
            'win_num': 0,
            'lose_num': 0,
            'max_win_pct': 0,
            'max_lost_pct': 0,
            'ave_win_mny': 0
        }
        trade_rst = pandas.DataFrame(columns=('ts_code', 'trade_times', 'final_win', 'win_pct'))
        for stock_code in self.stock_codes:
            trade_rst_dict = {
                'ts_code': stock_code,
                'trade_times': 0,
                'final_win': 0,
                'win_pct': 0
            }
            base_infos = self.data_center.fetch_base_data(stock_code)

            if base_infos is None or len(base_infos) == 0:
                continue

            ret_time = []
            for item in self.judge_time:
                ret_time.append(item(base_infos))
            if ret_time is None or len(ret_time) == 0:
                continue
            detail_trade_info = pandas.DataFrame(
                columns=('ts_code', 'curr_close', 'trade_date', 'trade_num', 'hold_num', 'hold_mny',
                         'total_mny', 'ope_flag'))
            if len(ret_time[0]) != len(base_infos):
                print("Not equals!!!")
            for i in range(len(ret_time[0])):
                # 判定是否是买入时机
                operate_flag = self.DO_NOTHING
                temp_buy_pct = 0.1
                for item in ret_time:
                    # 从当前往前5天之内是否有发出过买入信号，如果有，就算有
                    start_index = (i - self.MULTI_INDI_BETWEEN) if len(ret_time) > 1 else i
                    start_index = 0 if start_index < 0 else start_index
                    temp_val = self.DO_NOTHING
                    for j in range(start_index, i + 1):
                        if item.at[j, 'flag'] == self.BUY_FLAG:
                            temp_val = self.BUY_FLAG
                            temp_buy_pct = item.at[j, 'percent'] if 0 < item.at[j, 'percent'] < default_ope_pct \
                                else default_ope_pct
                            break
                        elif item.at[j, 'flag'] == self.SOLD_FLAG:
                            temp_val = self.SOLD_FLAG
                            temp_buy_pct = item.at[j, 'percent'] if 0 < item.at[j, 'percent'] < default_ope_pct \
                                else default_ope_pct
                            break
                    if operate_flag == self.DO_NOTHING or operate_flag == temp_val:
                        operate_flag = temp_val
                        continue
                    else:
                        operate_flag = self.DO_NOTHING
                        break

                if operate_flag == self.BUY_FLAG:
                    # 默认买入10%
                    buy_pct = temp_buy_pct
                    buy_mny = self.initial_mny * buy_pct
                    buy_mny = self.left_mny if self.left_mny < buy_mny else buy_mny
                    buy_num = buy_mny / base_infos.at[i, 'close']
                    buy_num = buy_num // 100 * 100
                    buy_mny = buy_num * base_infos.at[i, 'close']
                    self.hold_num = self.hold_num + buy_num
                    self.left_mny = self.left_mny - buy_mny
                    hold_mny = self.hold_num * base_infos.at[i, 'close']
                    # 记录买入信息
                    temp_dict = {
                        'ts_code': stock_code,
                        'trade_date': base_infos.at[i, 'trade_date'],
                        'trade_num': buy_num,
                        'hold_num': self.hold_num,
                        'hold_mny': hold_mny,
                        'total_mny': self.left_mny + hold_mny,
                        'curr_close': base_infos.at[i, 'close'],
                        'ope_flag': "buy"
                    }
                    detail_trade_info = detail_trade_info.append(temp_dict, ignore_index=True)
                    trade_rst_dict['trade_times'] = trade_rst_dict['trade_times'] + 1
                elif operate_flag == self.SOLD_FLAG:
                    # 默认卖出持仓数量的10%
                    sold_pct = temp_buy_pct
                    sold_num = self.hold_num * sold_pct
                    sold_num = sold_num // 100 * 100
                    self.hold_num = self.hold_num - sold_num
                    sold_mny = sold_num * base_infos.at[i, 'close']
                    self.left_mny = self.left_mny + sold_mny
                    hold_mny = self.hold_num * base_infos.at[i, 'close']
                    # 记录卖出信息
                    temp_dict = {
                        'ts_code': stock_code,
                        'trade_date': base_infos.at[i, 'trade_date'],
                        'trade_num': sold_num,
                        'hold_num': self.hold_num,
                        'hold_mny': hold_mny,
                        'total_mny': self.left_mny + hold_mny,
                        'curr_close': base_infos.at[i, 'close'],
                        'ope_flag': "sold"
                    }
                    detail_trade_info = detail_trade_info.append(temp_dict, ignore_index=True)
                    trade_rst_dict['trade_times'] = trade_rst_dict['trade_times'] + 1
            if not detail_trade_info.empty:
                file_name = "simulate_" + str(stock_code)
                now_time = datetime.datetime.now()
                now_time_str = now_time.strftime('%Y%m%d')
                file_name += '_' + now_time_str
                file_name += '.csv'
                FileOutput.csv_output(None, detail_trade_info, file_name, spe_dir_name=self.judge_out_name)
            else:
                print("no such stock!")
            self.reset()
            if detail_trade_info.empty:
                continue
            # 统计该只股票的最终盈利
            last_win = detail_trade_info.at[(len(detail_trade_info) - 1), 'total_mny'] - self.initial_mny
            last_win_pct = last_win / self.initial_mny
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
            file_name = "trade_group_" + self.stock_codes[0]
            now_time = datetime.datetime.now()
            now_time_str = now_time.strftime('%Y%m%d')
            file_name += '_' + now_time_str
            file_name += '.csv'
            FileOutput.csv_output(None, trade_rst, file_name, spe_dir_name=self.judge_out_name, extra_content=sum_str)
        return final_sum_dict
