import pandas as pd


class SelectorStrategy:
    SELECT_WRITE_TABLE = 'period_verify'

    def __init__(self, data_center, strategy=None, table_name=SELECT_WRITE_TABLE):
        if strategy is None:
            strategy = []
        self.strategy = strategy
        self.data_center = data_center
        self.table_name = table_name
        self.pre_scope = None

    def set_pre_scope(self, scope):
        """
        股票编码的列表，从这张列表当中筛选股票
        :param scope:
        :return:
        """
        self.pre_scope = scope

    def set_strategy(self, strategy):
        self.strategy = strategy

    def get_result(self):
        if self.strategy is None or len(self.strategy) == 0:
            return

        if self.pre_scope is None or len(self.pre_scope) == 0:
            self.pre_scope = self.data_center.fetch_stock_list()

        all_result = []
        for item in self.strategy:
            temp_strategy = item(ts_codes=self.pre_scope)
            ret_result = temp_strategy()
            all_result.append(ret_result)
        merge_rst = self.merge_result(all_result)
        merge_rst.drop_duplicates(subset=['ts_code'])
        return merge_rst

    def flush_result_to_db(self, result):
        """
        结果写入到数据库当中
        :param result:
        :return:
        """
        self.data_center.common_write_data_frame(result, self.table_name)

    @staticmethod
    def merge_result(results):
        res = pd.concat(results, axis=0, ignore_index=True)
        return res
