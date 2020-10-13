class SingleKInfo:
    def __init__(self):
        self.open = 0
        self.close = 0
        self.high = 0
        self.low = 0
        self.pct_chg = 0
        # TODO -- 需要敷一下只
        self.trade_date = ''


class StockBatchKInfo:
    def __init__(self):
        self.ts_name = ''
        self.ts_code = ''
        self.info_list = []

    def add_single_info(self, single_info: SingleKInfo):
        self.info_list.append(single_info)


class StockKInfo:
    def __init__(self):
        self.ts_code = ''
        self.ts_name = ''
        self.open = 0
        self.close = 0
        self.high = 0
        self.low = 0
        self.pct_chg = 0
        self.pre_close = 0
        # TODO -- 需要敷一下只
        self.trade_date = ''
