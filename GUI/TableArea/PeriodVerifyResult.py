from Data.DataCenter import DataCenter
from GUI.TableArea.AnaResult import AnaResult
from GUI.TableArea.CommonAnaRst import CommonAnaRst


# TODO -- 待检验
class PeriodVerifyResult(CommonAnaRst):
    def __init__(self, table_meta, table_name):
        super().__init__(table_meta)
        self.table_name = table_name

    def set_table_name(self, table_name):
        self.table_name = table_name

    def init_data_from_db(self):
        # 获取没有结束掉的股票
        query_sql = "select ts_code, in_date, in_price, pk_period_verify from period_verify where finished='0'"
        data_center = DataCenter.get_instance()
        ret_data = data_center.common_query(query_sql)
        # 首先计算一下各个阶段的收益
        period = [3, 7, 30, 60]
        self.calculate_period_win(period, ret_data)
        super().init_data_from_db()

    def calculate_period_win(self, period, calculate_data):
        period_field_map = {
            3: "three_day",
            7: 'seven_day',
            30: 'one_month',
            60: 'two_month'
        }
        if len(calculate_data) == 0:
            return
        for item in calculate_data:
            # 首先查询出最近历史的数据
            data_center = DataCenter.get_instance()
            query_sql = "select close from stock_base_info where ts_code='"
            query_sql = query_sql + item[0] + "' and trade_date > '"
            query_sql = query_sql + item[1].strftime("%Y%m%d") + "' order by trade_date limit 70"
            base_infos = data_center.common_query(query_sql)
            for single_period in period:
                if len(base_infos) >= single_period:
                    target_day_close = base_infos[single_period - 1][0]
                    in_price = item[2]
                    win_pct = (target_day_close - in_price) / target_day_close * 100
                    win_pct = win_pct

                    update_sql = "update " + self.table_name + " set "
                    update_sql = update_sql + period_field_map[single_period] + "='" + str(win_pct) + "'"
                    update_sql = update_sql + " where pk_period_verify='" + str(item[3]) + "'"
                    data_center.common_query(update_sql)

                    # FIXME -- 此处写死吧！！！！
                    if single_period == 60:
                        update_sql = "update " + self.table_name + " set finished=1 where pk_period_verify="
                        update_sql = update_sql + str(item[3]) + "'"
                        data_center.common_query(update_sql)
