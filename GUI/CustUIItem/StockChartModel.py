from PySide2.QtCore import Signal, QObject

from Data.Result import StockBatchKInfo, SingleKInfo


class StockChartModel(QObject):
    data_changed = Signal()

    def __init__(self, data_center):
        super().__init__()
        self.selected_ts = '000001.SZ'
        self.data_center = data_center

        # 当前选中的K线信息集合
        self.data: StockBatchKInfo = StockBatchKInfo()

    def get_current_stock_k_info(self):
        # 第一步，查询股票名称
        name_query_sql = "select name from stock_list where ts_code='"
        name_query_sql = name_query_sql + self.selected_ts + "'"
        ts_name = self.data_center.common_query(name_query_sql)
        if ts_name is None or len(ts_name) == 0:
            return
        ts_name = ts_name[0]
        # 第二步：查询股票的详细信息
        query_detail_info = "select open, close, high, low, pct_chg, trade_date from stock_base_info where ts_code='"
        query_detail_info = query_detail_info + self.selected_ts + "'"
        ret_data = self.data_center.common_query(query_detail_info)
        stock_batch_info = StockBatchKInfo()
        stock_batch_info.ts_code = self.selected_ts
        stock_batch_info.ts_name = ts_name
        for item in ret_data:
            single_k_info = SingleKInfo()
            single_k_info.open = item[0]
            single_k_info.close = item[1]
            single_k_info.high = item[2]
            single_k_info.low = item[3]
            single_k_info.pct_chg = item[4]
            single_k_info.trade_date = item[5]
            stock_batch_info.add_single_info(single_k_info)
        self.data = stock_batch_info
        return self.data

    def set_curr_selected_stock(self, ts_code):
        self.selected_ts = ts_code
        self.get_current_stock_k_info()
        self.emit_data_change()

    def emit_data_change(self):
        self.data_changed.emit()
        pass
