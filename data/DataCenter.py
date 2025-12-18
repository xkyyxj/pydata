#! /usr/bin/env python
"""
数据中心，负责获取数据以及将数据同步到本地MySQL数据当中
数据获取是通过Tushare开源平台获取的
"""
import datetime
import time

import pandas
import data.Database
import data.DataPull
from algorithm.IndicatorCalculation import append_cal_ema


class DataCenter:

    def __init__(self):
        self.__database = data.Database.MySQLDB()
        self.__data_pull = data.DataPull.DataPull()
        self.__fetch_data_time = 0

    @staticmethod
    def get_instance():
        """
        单例模式，返回单例对象
        :return:
        """
        return data_center

    def get_fetch_data_time(self):
        return self.__fetch_data_time

    def fetch_all_data_daily_use(self):
        """
        日常获取数据的方法，使用该方法增量获取每天数据

        会从数据库当中查询出来最后一天的记录（如果多张表的时间不一致，取最小的一天），然后从tushare上增量获取该天以后的数据
        :return:
        """
        # 查询三张表当中最小的一个日期，然后删除所有大于该日期的数据
        temp = self.__database.common_query("select max(trade_date) from stock_base_info")
        max_date = self.__database.common_query("select max(trade_date) from stock_base_info")[0][0]
        temp_max_data = self.__database.common_query("select max(trade_date) from stock_index_baseinfo")[0][0]
        max_date = min(max_date, temp_max_data)
        temp_max_data = self.__database.common_query("select max(trade_date) from adj_factor")[0][0]
        max_date = min(max_date, temp_max_data)
        max_date = datetime.date(int(max_date[0:4]), int(max_date[4:6]), int(max_date[6:8]))
        max_date += datetime.timedelta(days=1)
        max_date = max_date.strftime("%Y%m%d")

        # 删除数据
        self.__database.common_query("delete from stock_base_info where trade_date > {}".format(max_date))
        self.__database.common_query("delete from stock_index_baseinfo where trade_date > {}".format(max_date))
        self.__database.common_query("delete from adj_factor where trade_date > {}".format(max_date))
        # 重新获取数据
        self.fetch_all_daily_info_until_now(trade_date=max_date)

    def fetch_index_data(self, index_code, begin_date, end_date):
        """
        获取指数的日交易信息，
        :param index_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.__database.fetch_index_daily_info(index_code, begin_date, end_date)

        if local_data.size == 0:
            # 每当获取数据的时候，从tushare上直接获取整年的数据
            temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
            temp_end_date = temp_end_date.strftime("%Y%m%d")
            ret_value = self.__data_pull.fetch_stock_index_info(index_code, temp_begin_date, temp_end_date)
            self.__database.write_index_daily_info(ret_value)
        else:
            ret_value = local_data
            ret_value.sort_index(axis=1)
            # 获取没有数据的天数，此处需要往后推一天
            temp_date = ret_value.at[len(ret_value) - 1, 'trade_date']
            now_time = datetime.datetime.now()
            now_time_str = now_time.strftime('%Y%m%d')
            is_last_year = now_time_str[0:4] == temp_date[0:4]
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date += datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            # 同时查看下是不是数据库当中已经有相关年份的数据了
            # 可能存在一种情况：要求的日期正好该只stock停牌，但是该年份的数据已经写入到数据库当中了
            last_date = self.__database.is_exist_index_base_data(index_code, end_date[0:4])
            is_exist = last_date and int(last_date[6:8]) > 2
            if temp_date < end_date and not is_exist:
                if is_last_year:
                    temp_begin_date = need_date
                else:
                    temp_begin_date = datetime.date(int(temp_date[0:4]) + 1, 1, 1)
                    temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                if last_date:
                    temp_end_date = datetime.date(int(last_date[0:4]), int(last_date[4:6]), int(last_date[6:8]))
                    temp_end_date = temp_end_date.strftime("%Y%m%d")
                else:
                    temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
                    temp_end_date = temp_end_date.strftime("%Y%m%d")
                after_data = self.__data_pull.fetch_stock_index_info(index_code, temp_begin_date, temp_end_date)
                self.__database.write_index_daily_info(after_data)
                after_data = after_data[after_data['trade_date'] <= end_date]
                ret_value = ret_value.merge(after_data, how="outer")

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            is_exist = self.__database.is_exist_index_base_data(index_code, begin_date[0:4])
            if temp_date > begin_date and not is_exist:
                temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
                temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                before_data = self.__data_pull.fetch_stock_index_info(index_code, temp_begin_date, need_date)
                self.__database.write_index_daily_info(before_data)
                before_data = before_data[before_data['trade_date'] >= begin_date]
                ret_value = before_data.merge(ret_value, how="outer")

        return ret_value

    def fetch_base_data(self, stock_code, begin_date, end_date):
        """
        获取stock的日交易信息，
        :param stock_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.__database.fetch_daily_info(stock_code, begin_date, end_date)

        if local_data.size == 0:
            # 每当获取数据的时候，从tushare上直接获取整年的数据
            temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            now_time = datetime.datetime.now()
            now_time_str = now_time.strftime('%Y%m%d')
            is_last_year = now_time_str[0:4] == end_date[0:4]
            if is_last_year:
                temp_end_date = end_date
            else:
                temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
                temp_end_date = temp_end_date.strftime("%Y%m%d")
            ret_value = self.__data_pull.pull_data(stock_code, temp_begin_date, temp_end_date)
            self.__database.write_stock_info(ret_value)
        else:
            ret_value = local_data
            ret_value.sort_index(axis=1)
            # 获取没有数据的天数，此处需要往后推一天
            temp_date = ret_value.at[len(ret_value) - 1, 'trade_date']

            now_time = datetime.datetime.now()
            now_time_str = now_time.strftime('%Y%m%d')
            is_last_year = now_time_str[0:4] == temp_date[0:4]
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date += datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            # 同时查看下是不是数据库当中已经有相关年份的数据了
            # 可能存在一种情况：要求的日期正好该只stock停牌，但是该年份的数据已经写入到数据库当中了
            # last_date = self.__database.is_exist_base_data(stock_code, end_date[0:4])
            # is_exist = last_date and int(last_date[6:8]) > 2
            # # TODO -- 此处有问题，留待后续修正
            # if temp_date < end_date and not is_exist:
            #     if is_last_year:
            #         temp_begin_date = need_date
            #     else:
            #         temp_begin_date = datetime.date(int(temp_date[0:4]) + 1, 1, 1)
            #         temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            #     if last_date:
            #         temp_end_date = datetime.date(int(last_date[0:4]), int(last_date[4:6]), int(last_date[6:8]))
            #         temp_end_date = temp_end_date.strftime("%Y%m%d")
            #     else:
            #         temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
            #         temp_end_date = temp_end_date.strftime("%Y%m%d")
            #     after_data = self.__datapull.pull_data(stock_code, temp_begin_date, temp_end_date)
            #     self.__database.write_stock_info(after_data)
            #     after_data = after_data[after_data['trade_date'] <= end_date]
            #     ret_value = ret_value.merge(after_data, how="outer")
            #
            # # 获取没有数据的天数，此处需要往前推一天
            # temp_date = ret_value.at[0, 'trade_date']
            # need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            # need_date -= datetime.timedelta(days=1)
            # need_date = need_date.strftime("%Y%m%d")
            #
            # is_exist = self.__database.is_exist_base_data(stock_code, begin_date[0:4])
            # if temp_date > begin_date and not is_exist:
            #     temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            #     temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            #     before_data = self.__datapull.pull_data(stock_code, temp_begin_date, need_date)
            #     self.__database.write_stock_info(before_data)
            #     before_data = before_data[before_data['trade_date'] >= begin_date]
            #     ret_value = before_data.merge(ret_value, how="outer")
        return ret_value

    def fetch_all_base_one_day(self, trade_date):
        """
        获取stock基本信息的数据，@param trade_date这一天所有stock的当日交易信息
        :param trade_date:
        :return:
        """
        data = self.__data_pull.pull_all_one_day(trade_date)
        self.__database.write_stock_info(data)
        return data

    def fetch_index_info_daily(self, begin_date, end_date):
        """
        获取stock指数的日线信息，@param trade_date这一天的所有信息
        目前先处理两个指数的信息（上证指数：000001.SH和深证成指：399001.SZ）
        :param trade_date:
        :return:
        """
        fetch_list = ['000001.SH', '399001.SZ']
        for item in fetch_list:
            data = self.__data_pull.fetch_stock_index_info(item, start_date=begin_date, end_date=end_date)
            self.__database.write_index_daily_info(data)

    def fetch_adj_factor(self, ts_code, begin_date='20180101', end_date='20181231'):
        """
        从数据库当中获取复权因子，如果是复权因子没有包含最新的，那么重新从tushare接口获取
        :param end_date: 结束日期
        :param begin_date: 开始日期
        :param ts_code: stocktushare编码
        :return:
        """
        date = datetime.date.today()
        date = date.strftime("%Y%m%d")
        local_data = self.__database.fetch_adj_factor(ts_code)
        local_data.sort_values(by=['trade_date'])
        if len(local_data) == 0:
            temp_adj_factor = self.__data_pull.fetch_adj_factor_by_code(ts_code)
            self.__database.write_adj_factor(temp_adj_factor)
            temp_adj_factor = temp_adj_factor[(temp_adj_factor['trade_date'] >= begin_date) &
                                              (temp_adj_factor['trade_date'] <= end_date)]
            return temp_adj_factor
        if local_data.at[len(local_data) - 1, "trade_date"] < date:
            # 获取当前天的下一天
            last_day = local_data.at[len(local_data) - 1, "trade_date"]
            next_day = datetime.date(int(last_day[0:4]), int(last_day[4:6]), int(last_day[6:8]))
            next_day += datetime.timedelta(days=1)
            next_day = next_day.strftime("%Y%m%d")
            ret_value = self.fetch_adj_factor_until_now(next_day)

            # 选择出当前stock的信息对应的复权信息
            ret_value = ret_value[ret_value['ts_code'] == ts_code]
            local_data = pandas.concat([local_data, ret_value], ignore_index=True)
        # 注意返回数据要根据@param begin_date和@param end_date过滤
        local_data = local_data[(local_data['trade_date'] >= begin_date) & (local_data['trade_date'] <= end_date)]
        local_data.index = range(len(local_data))  # 重新设置一下index，避免两Series相乘找不到对应位置
        return local_data

    def fetch_all_daily_info_until_now(self, trade_date):
        """
        按天获取所有的stock的信息，如果是@param until_now为True的话，那么一直获取到当天为止
        该方法同时会获取相关的复权信息，同时将基本信息和复权信息做处理后写入到Redis缓存当中
        :param trade_date:
        :return:
        """
        if trade_date is None:
            return
        origin_trade_date = trade_date
        now_time = datetime.datetime.now()
        now_date = now_time.strftime("%Y%m%d")
        temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
        if trade_date <= now_date:
            # 首先获取一下指数的日线信息
            self.fetch_index_info_daily(trade_date, now_date)
            temp_date += datetime.timedelta(days=1)
            trade_date = temp_date.strftime("%Y%m%d")
            while trade_date <= now_date:
                self.fetch_all_base_one_day(trade_date)
                temp_date += datetime.timedelta(days=1)
                trade_date = temp_date.strftime("%Y%m%d")
        self.fetch_adj_factor_until_now(trade_date=origin_trade_date)
        append_cal_ema()

    def fetch_adj_factor_until_now(self, trade_date, until_now=True):
        """
        从@param trade_date开始，一直到系统时间为止，获取每一天的所有stock的日交易信息
        :param trade_date:
        :param until_now:
        :return:
        """
        ret_value = pandas.DataFrame(columns=("ts_code", "trade_date", "adj_factor"))
        if trade_date is None:
            trade_date = datetime.datetime.now()
            trade_date = trade_date.strftime("%Y%m%d")
            ret_value = self.__data_pull.fetch_adj_factor_by_date(trade_date=trade_date)
            self.__database.write_adj_factor(ret_value)
        else:
            now_time = datetime.datetime.now()
            now_date = now_time.strftime("%Y%m%d")
            temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
            if trade_date <= now_date and until_now:
                while trade_date <= now_date:
                    ret_value = self.__data_pull.fetch_adj_factor_by_date(trade_date=trade_date) if ret_value.empty else pandas.concat([ret_value, self.__data_pull.fetch_adj_factor_by_date(trade_date=trade_date)], axis=0)
                    temp_date += datetime.timedelta(days=1)
                    trade_date = temp_date.strftime("%Y%m%d")
                # 将数据回写到数据库当中
                self.__database.write_adj_factor(ret_value)
        return ret_value

    def init_adj_factor(self):
        """
        获取所有的stock复权因子，同时写入到数据库当中
        :return:
        """
        all_stock_list = self.fetch_stock_list(where="ts_code not in (select ts_code from adj_factor)", market=None)
        for item in range(len(all_stock_list)):
            ret_data = self.__data_pull.fetch_adj_factor_by_code(all_stock_list[item][0])
            self.__database.write_adj_factor(ret_data)
            time.sleep(1)

    def init_base_info(self):
        """
        初始化基本信息，并且将基本信息写入到数据库当中
        现在默认是fetch从2016年1月1日开始的基本数据
        :return:
        """
        all_stock_list = self.fetch_stock_list()
        end_date = datetime.datetime.now()
        end_date = end_date.strftime("%Y%m%d")
        for item in range(len(all_stock_list)):
            result = self.__data_pull.pull_data(all_stock_list[item][0], start_date='20250101', end_date=end_date)
            self.__database.write_stock_info(result)
            time.sleep(1)
        # 获取一下日线数据
        self.fetch_index_list()
        # 把两个指数的数据获取到了
        self.fetch_index_data('000001.SZ', '20000101', end_date)
        self.fetch_index_data('399001.SZ', '20000101', end_date)

    def fetch_stock_list(self, code=None, market=['主板', '中小板'], where=''):
        """
        获取包含所有stock的列表
        :param code:
        :param market:
        :param where:
        :return:
        """
        local_data = self.__database.fetch_stock_list(code, where=where, market=market)
        if not local_data or (code is not None and len(code) > 0 and not code.isspace() and code not in local_data):
            stock_list = self.__data_pull.fetch_stock_list()
            self.__database.write_stock_list(stock_list)
            return stock_list
        else:
            return local_data

    def refresh_stock_list(self):
        """
        更新stock列表-- stock_list更新
        :return:
        """
        self.__database.delete_stock_list()
        stock_list = self.__data_pull.fetch_stock_list()
        self.__database.write_stock_list(stock_list)

    def fetch_index_list(self):
        """
        获取stock指数列表，亦即有那些指数
        :param market:
        :return:
        """
        market_list = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'OTH']
        for market_item in market_list:
            data = self.__data_pull.fetch_index_list(market_item)
            self.__database.write_index_list(data)

    def fetch_base_data_pure_db(self, stock_code, begin_date, end_date=None):
        """
        纯粹从数据库当中获取相关的stock基本信息，如果没有也不从tushare当中获取
        :param stock_code:
        :param begin_date: 字符串类型
        :param end_date: 字符串类型
        :return: stock基本信息
        """
        # 首先从redis缓存当中取数据，通常情况下应该已经放入到redis缓存当中了
        # data = self.get_base_info_from_redis(stock_code, begin_date=begin_date, end_date=end_date)
        # 没有取到数据再从数据库当中取数据
        # if data is None or len(data) <= 0:
        if end_date is None:
            end_date = datetime.datetime.now()
            end_date = end_date.strftime("%Y%m%d")
        # 后复权价格，导致程序崩溃，所以此处重新计算一下并将其写入到Redis当中
        data = self.__database.fetch_daily_info(stock_code, start_date=begin_date, end_date=end_date)
        if data is not None and not data.empty:
            data = data.sort_values(by=['trade_date'])
            adj_factor = self.fetch_adj_factor_pure_db(stock_code,
                                                       begin_date=data.at[0, 'trade_date'],
                                                       end_date=data.at[
                                                                 len(data) - 1, 'trade_date'])
            data['adj_factor'] = adj_factor['adj_factor']
            data['af_close'] = data['close'] * adj_factor['adj_factor']

        # 做下过滤
        data = data[(data['trade_date'] >= begin_date) & (data['trade_date'] <= end_date)]
        data = data.sort_values(by=['trade_date'])
        data.index = range(len(data))
        return data

    def fetch_adj_factor_pure_db(self, ts_code, begin_date='20180101', end_date=None):
        if end_date is None:
            end_date = datetime.datetime.now()
            end_date = end_date.strftime("%Y%m%d")
        local_data = self.__database.fetch_adj_factor_by_code_date(ts_code, begin_date, end_date)
        return local_data

    def fetch_base_until_now_pure_db(self, begin_date='202250805'):
        """
        从数据库当中获取从指定日期开始到现在的数据，不从tushare当中获取数据
        """
        sql = 'select ts_code, close, trade_date, pct_chg from stock_base_info where trade_date > \'' + begin_date + '\''
        stock_base_info_pandas = self.__database.common_query_to_pandas(sql)
        sql = 'select ts_code, close, trade_date, pct_chg from stock_index_baseinfo where trade_date >  \'' + begin_date + '\''
        index_daily_padas = self.__database.common_query_to_pandas(sql)
        return pandas.concat([stock_base_info_pandas, index_daily_padas], axis=0)

    def fetch_all_base_range_pure_db(self, begin_date, end_date):
        """
        从数据库获取所有的基本日线信息，不从tushare当中获取数据
        """
        if begin_date is None or end_date is None:
            raise RuntimeError('fetch_all_base_range_no_tushare 参数必传')
        sql = 'select ts_code, close, trade_date from stock_base_info where trade_date >= \'' + begin_date + '\' and trade_date <= \'' + end_date + '\''
        stock_base_info_pandas = self.__database.common_query_to_pandas(sql)
        sql = 'select ts_code, close, trade_date from stock_index_baseinfo where trade_date >=  \'' + begin_date + '\' and trade_date <= \'' + end_date + '\''
        index_daily_padas = self.__database.common_query_to_pandas(sql)
        return pandas.concat([stock_base_info_pandas, index_daily_padas], axis=0)

    def fetch_all_daily_info(self, trade_date=None, until_now=True):
        """
        按天获取所有的stock的信息，写入数据库，如果是@param until_now为True的话，那么一直获取到当天为止
        :param stock_code:
        :param trade_date:
        :param until_now:
        :return:
        """
        if trade_date is None:
            trade_date = datetime.datetime.now()
            trade_date = trade_date.strftime("%Y%m%d")
            self.fetch_all_base_one_day(trade_date=trade_date)
        else:
            now_time = datetime.datetime.now()
            now_date = now_time.strftime("%Y%m%d")
            temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
            if trade_date < now_date and until_now:
                while trade_date < now_date:
                    self.fetch_all_base_one_day(trade_date=trade_date)
                    temp_date += datetime.timedelta(days=1)
                    trade_date = temp_date.strftime("%Y%m%d")

    def fetch_finance_indicator(self, ts_code, start_date, end_date):
        """
        获取stock指标数据
        1. 首先是从数据库当中获取，
        2. 如果数据库当中没有存储，则从tushare上获取，并且存储到数据库当中
        :param ts_code:
        :param start_date:
        :param end_date:
        :return:
        """
        query_sql = "select * from finance_indicator where end_date>'" + str(start_date) + \
                    "' and end_date <= '" + str(end_date) + "' and ts_code='" + ts_code + "'"
        ret_data = self.__database.common_query_to_pandas(query_sql)
        if ret_data.empty:
            self.fetch_finance_indicator_from_tushare(ts_code, start_date, end_date)
        ret_data = self.__database.common_query_to_pandas(query_sql)
        return ret_data

    def fetch_finance_indicator_from_tushare(self, ts_code, start_date, end_date):
        """
        获取财务指标数据并且将财务数据写入到数据库当中
        注意只能单条获取，并且只能够获取60条的
        :param ts_code:
        :param start_date:
        :param end_date:
        :return:
        """
        ret_data = self.__data_pull.fetch_finance_data(ts_code, start_date, end_date)
        self.common_write_data_frame(ret_data, 'finance_indicator')

    def common_query(self, sql):
        """
        通用的数据查询接口
        :param sql:
        :return:
        """
        return self.__database.common_query(sql)

    def common_query_to_pandas(self, sql):
        """
        通常查询接口，将查询结果返回为pandas.DataFrame
        :param sql:
        :return:
        """
        return self.__database.common_query_to_pandas(sql)

    def common_write_data_frame(self, data_frame, table_name):
        """
        通用的将pandas.DataFrame写入到数据库当中
        :param table_name: 数据库表名
        :param data_frame: 将要写入的数据，类型是pandas.DataFrame
        :return:
        """
        self.__database.common_write_data_frame(data_frame, table_name)


data_center: DataCenter = DataCenter()