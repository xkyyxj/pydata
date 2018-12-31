#! /usr/bin/env python
"""
数据中心，负责获取数据以及将数据同步到本地MySQL数据当中
数据获取是通过Tushare开源平台获取的
"""
import datetime
import pandas
import Data.Database
import Data.DataPull


class DataCenter:

    def __init__(self):
        self.__database = Data.Database.MySQLDB()
        self.__datapull = Data.DataPull.DataPull()

    def fetch_index_data(self, index_code, begin_date="20180101", end_date="20181231"):
        """
        获取指数的日交易信息，
        :param index_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.__database.fetch_daily_info(index_code, begin_date, end_date)

        if local_data.size == 0:
            # 每当获取数据的时候，从tushare上直接获取整年的数据
            temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
            temp_end_date = temp_end_date.strftime("%Y%m%d")
            ret_value = self.__datapull.fetch_stock_index_info(index_code, temp_begin_date, temp_end_date)
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
            # 可能存在一种情况：要求的日期正好该只股票停牌，但是该年份的数据已经写入到数据库当中了
            last_date = self.__database.is_exist_base_data(index_code, end_date[0:4])
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
                self.__database.write_stock_info(after_data)
                after_data = after_data[after_data['trade_date'] <= end_date]
                ret_value = ret_value.merge(after_data, how="outer")

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            is_exist = self.__database.is_exist_base_data(index_code, begin_date[0:4])
            if temp_date > begin_date and not is_exist:
                temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
                temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                before_data = self.__datapull.fetch_stock_index_info(index_code, temp_begin_date, need_date)
                self.__database.write_stock_info(before_data)
                before_data = before_data[before_data['trade_date'] >= begin_date]
                ret_value = before_data.merge(ret_value, how="outer")
        return ret_value

    def fetch_base_data(self, stock_code, begin_date="20180101", end_date="20181231"):
        """
        获取指数的日交易信息，
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
            ret_value = self.__datapull.pull_data(stock_code, temp_begin_date, temp_end_date)
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
            # 可能存在一种情况：要求的日期正好该只股票停牌，但是该年份的数据已经写入到数据库当中了
            last_date = self.__database.is_exist_base_data(stock_code, end_date[0:4])
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
                after_data = self.__datapull.pull_data(stock_code, temp_begin_date, temp_end_date)
                self.__database.write_stock_info(after_data)
                after_data = after_data[after_data['trade_date'] <= end_date]
                ret_value = ret_value.merge(after_data, how="outer")

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            is_exist = self.__database.is_exist_base_data(stock_code, begin_date[0:4])
            if temp_date > begin_date and not is_exist:
                temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
                temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                before_data = self.__datapull.pull_data(stock_code, temp_begin_date, need_date)
                self.__database.write_stock_info(before_data)
                before_data = before_data[before_data['trade_date'] >= begin_date]
                ret_value = before_data.merge(ret_value, how="outer")
        return ret_value
    
    def fetch_all_base_one_day(self, trade_date):
        data = self.__datapull.pull_all_one_day(trade_date)
        self.__database.write_stock_info(data)

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
        local_data.sort_index(axis=1)
        data = self.__datapull.fetch_weight_factor(ts_code, date)
        if local_data.size <= 0 or (data.size > 0 and data.at[0, 'adj_factor'] !=
                                    local_data.at[len(local_data) - 1, 'adj_factor']):
            data = self.__datapull.fetch_weight_factor(ts_code)
            data.sort_index(axis=1)
            insert_data = data[data.trade_date > local_data.at[len(local_data) - 1, 'trade_date']] \
                if local_data.size > 0 else data
            self.__database.write_adj_factor(insert_data)
            ret_data = data[(data.trade_date >= begin_date) & (data.trade_date <= end_date)]
            ret_data.index = range(len(ret_data))   # 重新设置一下index，避免两Series相乘找不到对应位置
            return ret_data
        else:
            ret_data = local_data[(local_data.trade_date >= begin_date) & (local_data.trade_date <= end_date)]
            ret_data.index = range(len(ret_data))  # 重新设置一下index，避免两Series相乘找不到对应位置
            return ret_data

    def fetch_adj_factor_by_date(self, stock_code, begin_date='20180101', end_date='20181231'):
        """
        从数据库当中获取复权因子，根据日期来获取，同获取基本信息差不多
        :param stock_code:
        :param end_date: 结束日期
        :param begin_date: 开始日期
        :param ts_code: 股票tushare编码
        :return:
        """
        local_data = self.__database.fetch_adj_factor(stock_code)

        if local_data.size == 0:
            # 每当获取数据的时候，从tushare上直接获取整年的数据
            temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
            temp_begin_date = temp_begin_date.strftime("%Y%m%d")
            temp_end_date = datetime.date(int(end_date[0:4]), 12, 31)
            temp_end_date = temp_end_date.strftime("%Y%m%d")
            ret_value = self.__datapull.fetch_weight_factor(stock_code, temp_begin_date, temp_end_date)
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
            # 可能存在一种情况：要求的日期正好该只股票停牌，但是该年份的数据已经写入到数据库当中了
            last_date = self.__database.is_exist_adj_factor(stock_code, end_date[0:4])
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
                after_data = self.__datapull.fetch_weight_factor(stock_code, temp_begin_date, temp_end_date)
                self.__database.write_adj_factor(after_data)
                after_data = after_data[after_data['trade_date'] <= end_date]
                ret_value = ret_value.merge(after_data, how="outer")

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")

            is_exist = self.__database.is_exist_adj_factor(stock_code, begin_date[0:4])
            if temp_date > begin_date and not is_exist:
                temp_begin_date = datetime.date(int(begin_date[0:4]), 1, 1)
                temp_begin_date = temp_begin_date.strftime("%Y%m%d")
                before_data = self.__datapull.fetch_weight_factor(stock_code, temp_begin_date, need_date)
                self.__database.write_adj_factor(before_data)
                before_data = before_data[before_data['trade_date'] >= begin_date]
                ret_value = before_data.merge(ret_value, how="outer")
        return ret_value

    def fetch_stock_list(self, code=None, market='主板', where=''):
        local_data = self.__database.fetch_stock_list(code, where=where)
        if not local_data or (code is not None and len(code) > 0 and not code.isspace() and code not in local_data):
            stock_list = self.__datapull.fetch_stock_list()
            self.__database.write_stock_list(stock_list)
            return stock_list
        else:
            return local_data

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

    def fetch_index_list(self, market):
        """
        获取股票指数列表，亦即有那些指数
        :param market:
        :return:
        """
        data = self.__datapull.fetch_index_list(market)
        self.__database.write_index_list(data)
        return data

    def fetch_base_data_pure_database(self, stock_code, begin_date='20180101', end_date='20181231'):
        """
        纯粹从数据库当中获取相关的股票基本信息，如果没有也不从tushare当中获取
        :param stock_code:
        :param begin_date:
        :param end_date:
        :return: 股票基本信息
        """
        data = self.__database.fetch_daily_info(stock_code, start_date=begin_date, end_date=end_date)
        return data



