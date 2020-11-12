#! /usr/bin/env python
"""
数据中心，负责获取数据以及将数据同步到本地MySQL数据当中
数据获取是通过Tushare开源平台获取的
"""
import datetime
import time
import pandas
import Data.Database
import Data.DataPull
import redis
import json
import Algorithm.Calculator as Calculator


class DataCenter:

    def __init__(self):
        self.__database = Data.Database.MySQLDB()
        self.__datapull = Data.DataPull.DataPull()
        self.__redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
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

    def write_data_frame_to_redis(self, base_info):
        """
        将DataFrame写入到Redis缓存当中
        调用DataFrame的to_msgpack方法，获取相应的字符串作为value，key值为stock_code
        :param base_info: 股票基本信息，类型为DataFrame
        :return:
        """
        if base_info is None or len(base_info) <= 0:
            return
        redis_conn = redis.StrictRedis(connection_pool=self.__redis_pool)
        pipe_line = redis_conn.pipeline(transaction=False)
        # all_value = base_info.to_msgpack() # old version of pandas
        all_value = base_info.to_json(orient='table')

        # 构建缓存的key值
        key = base_info.at[0, 'ts_code']
        pipe_line.set(key, all_value)
        pipe_line.execute()

    def get_data_frame_from_redis(self, stock_code):
        """
        从redis缓存当中获取到相应的DataFrame
        :param stock_code:
        :return:
        """
        redis_conn = redis.StrictRedis(connection_pool=self.__redis_pool)
        value = redis_conn.get(stock_code)
        ret_value = pandas.DataFrame()
        if value is not None and len(value) > 0:
            # ret_value = pandas.read_msgpack(value) # old version of pandas
            ret_value = pandas.read_json(value, orient='table')
        return ret_value

    def write_one_day_info_to_redis(self, base_info, add_type='after'):
        if base_info is None or len(base_info) <= 0:
            return
        redis_conn = redis.StrictRedis(connection_pool=self.__redis_pool)
        pipe_line = redis_conn.pipeline(transaction=False)
        all_value = base_info.values

        if add_type == 'after':
            for i in range(all_value.shape[0]):
                temp_value = all_value[i]
                temp_series = pandas.Series(temp_value, index=base_info.columns)
                write_json = temp_series.to_json()
                pipe_line.rpush(temp_series['ts_code'], write_json)
        else:
            for i in range(all_value.shape[0]):
                temp_value = all_value[i]
                temp_series = pandas.Series(temp_value, index=base_info.columns)
                write_json = temp_series.to_json()
                pipe_line.lpush(temp_series['ts_code'], write_json)
        pipe_line.execute()

    def write_base_info_to_redis(self, stock_code, base_info, add_type='after'):
        """
        将股票的基本信息写入到redis缓存当中
        此方法不推荐使用，因为python的JSON格式化字符串太慢了，以至于比直接从数据库当中取数据还慢
        :param stock_code:
        :param base_info:
        :param add_type:
        :return:
        """
        if base_info is None or len(base_info) <= 0:
            return
        redis_conn = redis.StrictRedis(connection_pool=self.__redis_pool)
        pipe_line = redis_conn.pipeline(transaction=False)
        all_value = base_info.values

        # 判定一下数据是否存在于redis缓存当中
        rdc = redis.StrictRedis(connection_pool=self.__redis_pool)
        list_len = rdc.llen(stock_code)
        temp_value = rdc.lrange(stock_code, list_len - 1, -1)
        last_info = None
        if len(temp_value) > 0:
            last_info = json.loads(temp_value[0])
        if last_info is not None and last_info['trade_date'] == base_info.at[0, 'trade_date']:
            return
        if add_type == 'after':
            for i in range(all_value.shape[0]):
                temp_value = all_value[i]
                temp_series = pandas.Series(temp_value, index=base_info.columns)
                write_json = temp_series.to_json()
                pipe_line.rpush(stock_code, write_json)
        else:
            for i in range(all_value.shape[0]):
                temp_value = all_value[i]
                temp_series = pandas.Series(temp_value, index=base_info.columns)
                write_json = temp_series.to_json()
                pipe_line.lpush(stock_code, write_json)
        pipe_line.execute()

    def get_base_info_from_redis(self, stock_code, begin_date='20180101', end_date='201812313'):
        """
        从redis缓存当中取到所有的数据然后解析成DataFrame
        最终的返回数据会根据@param begin_date和@param end_date进行一下过滤
        :param stock_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        start_time = datetime.datetime.now()
        ret_value = pandas.DataFrame()
        redis_conn = redis.StrictRedis(connection_pool=self.__redis_pool)
        all_json = redis_conn.lrange(stock_code, 0, -1)
        if all_json is None or len(all_json) <= 0:
            return ret_value
        temp_obj = json.loads(all_json[0])
        ret_value = pandas.DataFrame(columns=temp_obj.keys())
        ret_value.append(temp_obj, ignore_index=True)
        for i in range(1, len(all_json)):
            temp_obj = json.loads(all_json[i])
            ret_value.append(temp_obj, ignore_index=True)
        ret_value = ret_value[(end_date > ret_value['trade_date']) & (ret_value['trade_date'] > begin_date)]
        end_time = datetime.datetime.now()
        delta_time = (end_time - start_time).seconds
        self.__fetch_data_time += delta_time
        return ret_value

    def fetch_index_data(self, index_code, begin_date="20180101", end_date="20181231"):
        """
        获取指数的日交易信息，
        :param index_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.get_base_info_from_redis(index_code, begin_date, end_date)
        if local_data.empty:
            local_data = self.__database.fetch_index_daily_info(index_code, begin_date, end_date)

        if local_data.size == 0:
            # 每当获取数据的时候，从tushare上直接获取整年的数据
            temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
            temp_end_date = temp_end_date.strftime("%Y%m%d")
            ret_value = self.__datapull.fetch_stock_index_info(index_code, temp_begin_date, temp_end_date)
            self.__database.write_index_daily_info(ret_value)
            self.write_base_info_to_redis(index_code, ret_value)
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
            # 可能存在一种情况：要求的日期正好该只股票停牌，但是该年份的数据已经写入到数据库当中了
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
                after_data = self.__datapull.fetch_stock_index_info(index_code, temp_begin_date, temp_end_date)
                self.__database.write_index_daily_info(after_data)
                after_data = after_data[after_data['trade_date'] <= end_date]
                ret_value = ret_value.merge(after_data, how="outer")
                self.write_base_info_to_redis(index_code, after_data)

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            is_exist = self.__database.is_exist_index_base_data(index_code, begin_date[0:4])
            if temp_date > begin_date and not is_exist:
                temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
                temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                before_data = self.__datapull.fetch_stock_index_info(index_code, temp_begin_date, need_date)
                self.__database.write_index_daily_info(before_data)
                before_data = before_data[before_data['trade_date'] >= begin_date]
                ret_value = before_data.merge(ret_value, how="outer")
                self.write_base_info_to_redis(index_code, before_data)

        return ret_value

    def fetch_base_data(self, stock_code, begin_date="20180101", end_date="20201231"):
        """
        获取股票的日交易信息，
        :param stock_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.get_data_frame_from_redis(stock_code)
        if local_data.empty:
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
            ret_value = self.__datapull.pull_data(stock_code, temp_begin_date, temp_end_date)
            self.__database.write_stock_info(ret_value)
            self.write_base_info_to_redis(stock_code, ret_value)
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
            # 可能存在一种情况：要求的日期正好该只股票停牌，但是该年份的数据已经写入到数据库当中了
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
        # self.write_data_frame_to_redis(ret_value)
        return ret_value

    def fetch_all_base_one_day(self, trade_date):
        """
        获取股票基本信息的数据，@param trade_date这一天所有股票的当日交易信息
        :param trade_date:
        :return:
        """
        data = self.__datapull.pull_all_one_day(trade_date)
        self.__database.write_stock_info(data)
        return data

    def fetch_index_info_daily(self, begin_date, end_date):
        """
        获取股票指数的日线信息，@param trade_date这一天的所有信息
        目前先处理两个指数的信息（上证指数：000001.SH和深证成指：399001.SZ）
        :param trade_date:
        :return:
        """
        fetch_list = ['000001.SH', '399001.SZ']
        for item in fetch_list:
            data = self.__datapull.fetch_stock_index_info(item, start_date=begin_date, end_date=end_date)
            self.__database.write_index_daily_info(data)

    def fetch_adj_factor(self, ts_code, begin_date='20180101', end_date='20181231'):
        """
        从数据库当中获取复权因子，如果是复权因子没有包含最新的，那么重新从tushare接口获取
        :param end_date: 结束日期
        :param begin_date: 开始日期
        :param ts_code: 股票tushare编码
        :return:
        """
        date = datetime.date.today()
        date = date.strftime("%Y%m%d")
        local_data = self.__database.fetch_adj_factor(ts_code)
        local_data.sort_values(by=['trade_date'])
        if len(local_data) == 0:
            temp_adj_factor = self.__datapull.fetch_adj_factor_by_code(ts_code)
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

            # 选择出当前股票的信息对应的复权信息
            ret_value = ret_value[ret_value['ts_code'] == ts_code]
            local_data.append(ret_value)
        # 注意返回数据要根据@param begin_date和@param end_date过滤
        local_data = local_data[(local_data['trade_date'] >= begin_date) & (local_data['trade_date'] <= end_date)]
        local_data.index = range(len(local_data))  # 重新设置一下index，避免两Series相乘找不到对应位置
        return local_data

    def fetch_adj_factor_pure_database(self, ts_code, begin_date='20180101', end_date=None):
        if end_date is None:
            end_date = datetime.datetime.now()
            end_date = end_date.strftime("%Y%m%d")
        local_data = self.__database.fetch_adj_factor_by_code_date(ts_code, begin_date, end_date)
        return local_data

    def fetch_all_daily_info_until_now(self, trade_date, until_now=True):
        """
        按天获取所有的股票的信息，如果是@param until_now为True的话，那么一直获取到当天为止
        该方法同时会获取相关的复权信息，同时将基本信息和复权信息做处理后写入到Redis缓存当中
        :param stock_code:
        :param trade_date:
        :param until_now:
        :return:
        """
        origin_trade_date = trade_date
        temp_base_info = pandas.DataFrame()
        if trade_date is None:
            trade_date = datetime.datetime.now()
            trade_date = trade_date.strftime("%Y%m%d")
            self.fetch_all_base_one_day(trade_date=trade_date)
        else:
            now_time = datetime.datetime.now()
            now_date = now_time.strftime("%Y%m%d")
            temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
            if trade_date <= now_date and until_now:
                # 首先获取一下指数的日线信息
                self.fetch_index_info_daily(trade_date, now_date)
                temp_base_info = self.fetch_all_base_one_day(trade_date=trade_date)
                temp_date += datetime.timedelta(days=1)
                trade_date = temp_date.strftime("%Y%m%d")
                while trade_date <= now_date:
                    temp_base_info = temp_base_info.append(self.fetch_all_base_one_day(trade_date=trade_date))
                    temp_date += datetime.timedelta(days=1)
                    trade_date = temp_date.strftime("%Y%m%d")
            temp_adj_factor = self.fetch_adj_factor_until_now(trade_date=origin_trade_date)
            # 将数据更新到Redis缓存当中
            self.modify_redis_data_frame(trade_date, temp_base_info=temp_base_info, temp_adj_factor=temp_adj_factor)

    def fetch_adj_factor_until_now(self, trade_date, until_now=True):
        """
        从@param trade_date开始，一直到系统时间为止，获取每一天的所有股票的日交易信息
        :param trade_date:
        :param until_now:
        :return:
        """
        ret_value = pandas.DataFrame(columns=("ts_code", "trade_date", "adj_factor"))
        if trade_date is None:
            trade_date = datetime.datetime.now()
            trade_date = trade_date.strftime("%Y%m%d")
            ret_value = self.__datapull.fetch_adj_factor_by_date(trade_date=trade_date)
            self.__database.write_adj_factor(ret_value)
        else:
            now_time = datetime.datetime.now()
            now_date = now_time.strftime("%Y%m%d")
            temp_date = datetime.date(int(trade_date[0:4]), int(trade_date[4:6]), int(trade_date[6:8]))
            if trade_date <= now_date and until_now:
                while trade_date <= now_date:
                    ret_value = ret_value.append(self.__datapull.fetch_adj_factor_by_date(trade_date=trade_date))
                    temp_date += datetime.timedelta(days=1)
                    trade_date = temp_date.strftime("%Y%m%d")
                # 将数据回写到数据库当中
                self.__database.write_adj_factor(ret_value)
        return ret_value

    def init_adj_factor(self):
        """
        获取所有的股票复权因子，同时写入到数据库当中
        :return:
        """
        all_stock_list = self.fetch_stock_list(where="ts_code not in (select ts_code from adj_factor)", market=None)
        for item in range(len(all_stock_list)):
            ret_data = self.__datapull.fetch_adj_factor_by_code(all_stock_list[item][0])
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
            result = self.__datapull.pull_data(all_stock_list[item][0], start_date='20160101', end_date=end_date)
            self.__database.write_stock_info(result)
            time.sleep(1)
        # 获取一下日线数据
        self.fetch_index_list()
        # 把两个指数的数据获取到了
        self.fetch_index_data('000001.SZ', '20000101', end_date)
        self.fetch_index_data('399001.SZ', '20000101', end_date)

    def init_redis_cache(self, end_date=None):
        """
        纯粹从数据库当中取出股票日交易信息和复权因子，并且计算后复权收盘价，写入到Redis缓存当中
        :return:
        """
        all_stock_list = self.fetch_stock_list()
        for i in range(len(all_stock_list)):
            base_data = self.fetch_base_data_pure_database(stock_code=all_stock_list[i][0], begin_date='20180101',
                                                           end_date=end_date)
            if len(base_data) > 0:
                base_data = base_data.sort_values(by=['trade_date'])
                adj_factor = self.fetch_adj_factor_pure_database(all_stock_list[i][0],
                                                                 begin_date=base_data.at[0, 'trade_date'],
                                                                 end_date=base_data.at[
                                                                     len(base_data) - 1, 'trade_date'])
                base_data['adj_factor'] = adj_factor['adj_factor']
                base_data['af_close'] = base_data['close'] * adj_factor['adj_factor']
                base_data = Calculator.cal_macd_per_stock(base_data)
                self.write_data_frame_to_redis(base_data)

    def fetch_stock_list(self, code=None, market=['主板', '中小板'], where=''):
        """
        获取包含所有股票的列表
        :param code:
        :param market:
        :param where:
        :return:
        """
        local_data = self.__database.fetch_stock_list(code, where=where, market=market)
        if not local_data or (code is not None and len(code) > 0 and not code.isspace() and code not in local_data):
            stock_list = self.__datapull.fetch_stock_list()
            self.__database.write_stock_list(stock_list)
            return stock_list
        else:
            return local_data

    def refresh_stock_list(self):
        """
        更新股票列表-- stock_list更新
        :return:
        """
        self.__database.delete_stock_list()
        stock_list = self.__datapull.fetch_stock_list()
        self.__database.write_stock_list(stock_list)

    @staticmethod
    def process_data(base_data, adj_factor):
        """
        处理一下权重信息
        TODO -- 此处先仅处理后复权，后面再修正
        :param base_data:
        :param adj_factor:
        :return:
        """
        base_data['af_open'] = base_data['open'] * adj_factor['adj_factor']
        return base_data

    def fetch_index_list(self):
        """
        获取股票指数列表，亦即有那些指数
        :param market:
        :return:
        """
        market_list = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'OTH']
        for market_item in market_list:
            data = self.__datapull.fetch_index_list(market_item)
            self.__database.write_index_list(data)

    def fetch_base_data_pure_database(self, stock_code, begin_date, end_date=None):
        """
        纯粹从数据库当中获取相关的股票基本信息，如果没有也不从tushare当中获取
        :param stock_code:
        :param begin_date: 字符串类型
        :param end_date: 字符串类型
        :return: 股票基本信息
        """
        # 首先从redis缓存当中取数据，通常情况下应该已经放入到redis缓存当中了
        # data = self.get_base_info_from_redis(stock_code, begin_date=begin_date, end_date=end_date)
        # 没有取到数据再从数据库当中取数据
        # if data is None or len(data) <= 0:
        if end_date is None:
            end_date = datetime.datetime.now()
            end_date = end_date.strftime("%Y%m%d")
        data = self.get_data_frame_from_redis(stock_code)
        if data is None or len(data) == 0:
            # 发现有一种奇怪的情况：某些情况下Redis缓存当中没有相关的数据，所以直接从数据库取出来的数据没有
            # 后复权价格，导致程序崩溃，所以此处重新计算一下并将其写入到Redis当中
            data = self.__database.fetch_daily_info(stock_code, start_date=begin_date, end_date=end_date)
            if data is not None and not data.empty:
                data = data.sort_values(by=['trade_date'])
                adj_factor = self.fetch_adj_factor_pure_database(stock_code,
                                                                 begin_date=data.at[0, 'trade_date'],
                                                                 end_date=data.at[
                                                                     len(data) - 1, 'trade_date'])
                data['adj_factor'] = adj_factor['adj_factor']
                data['af_close'] = data['close'] * adj_factor['adj_factor']
                self.write_data_frame_to_redis(data)

        # 做下过滤
        data = data[(data['trade_date'] >= begin_date) & (data['trade_date'] <= end_date)]
        data = data.sort_values(by=['trade_date'])
        data.index = range(len(data))
        return data

    def flush_data_to_redis(self):
        """
        将数据库当中存储的所有的股票基本信息(stock_base_info)写入到redis缓存当中
        :return:
        """
        stock_list = self.fetch_stock_list()
        for i in range(len(stock_list)):
            base_data = self.fetch_base_data_pure_database(stock_list[i][0],
                                                           begin_date='00000000', end_date='99991231')
            self.write_base_info_to_redis(stock_list[i][0], base_data)

    def flush_data_frame_to_redis(self):
        """
        将数据库当中存储的所有的股票基本信息(stock_base_info)写入到redis缓存当中
        不同之处在于不是将数据逐行转换成JSON字符串，而是直接调用to_msgpack的形式
        :return:
        """
        stock_list = self.fetch_stock_list()
        for i in range(len(stock_list)):
            base_data = self.fetch_base_data_pure_database(stock_list[i][0],
                                                           begin_date='00000000', end_date='99991231')
            self.write_data_frame_to_redis(base_data)

    def fetch_all_daily_info(self, trade_date=None, until_now=True):
        """
        按天获取所有的股票的信息，如果是@param until_now为True的话，那么一直获取到当天为止
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

    def modify_redis_data_frame(self, start_date, temp_base_info=None, temp_adj_factor=None):
        """
        该方法用于在每天获取当天所有的日交易信息后调用，将最新获取的日交易信息更新到redis当中
        这个方法目前会计算后复权收盘价，并且将复权因子顺便写入到了基本信息的DataFrame当中，并且写入到redis缓存当中
        :return:
        """
        results = temp_base_info if temp_base_info is not None else \
            self.__database.fetch_all_daily_info_by_date(start_date=start_date)
        all_adj_factor = temp_adj_factor if temp_adj_factor is not None else \
            self.__database.fetch_all_adj_factor_by_date(start_date=start_date)
        stock_list = self.fetch_stock_list()
        for i in range(len(stock_list)):
            base_info = self.get_data_frame_from_redis(stock_code=stock_list[i][0])
            temp_base_info = results[results['ts_code'] == stock_list[i][0]]
            temp_adj_factor = all_adj_factor[all_adj_factor['ts_code'] == stock_list[i][0]]
            if not temp_base_info.empty and not temp_adj_factor.empty:
                temp_base_info.index = range(len(temp_base_info))
                temp_adj_factor.index = range(len(temp_adj_factor))
                temp_af_close = temp_base_info['close'] * temp_adj_factor['adj_factor']
                temp_base_info.loc[:, 'af_close'] = temp_af_close
                temp_base_info.loc[:, 'adj_factor'] = temp_adj_factor['adj_factor']
                base_info = base_info.append(temp_base_info)
                base_info.index = range(len(base_info))
                base_info = Calculator.cal_macd_per_stock(base_info)
                self.write_data_frame_to_redis(base_info)

    def fetch_finance_indicator(self, ts_code, start_date, end_date):
        """
        获取股票指标数据
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
        ret_data = self.__datapull.fetch_finance_data(ts_code, start_date, end_date)
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
