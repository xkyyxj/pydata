class SingleKInfo:
    def __init__(self):
        self.open = 0
        self.close = 0
        self.high = 0
        self.low = 0
        self.pct_chg = 0


class StockBatchKInfo:
    def __init__(self):
        self.ts_name = ''
        self.ts_code = ''
        self.info_list = []

    def add_single_info(self, single_info: SingleKInfo):
        self.info_list.append(single_info)
