import PySide2
from PySide2.QtWidgets import QLCDNumber, QSlider, QWidget, QVBoxLayout, QApplication, QGridLayout
from PySide2.QtCore import Qt, Slot, QLine, QRect, Signal
from PySide2.QtWidgets import QFrame
from PySide2.QtGui import QPaintEvent, QPen, QFont, QKeyEvent, QPalette
from PySide2.QtGui import QPainter
from PySide2.QtGui import QBrush
from PySide2.QtGui import QColor
from PySide2.QtCore import QPoint
from PySide2.QtCore import QSize

from Data.Result import StockKInfo
from GUI.CustUIItem.StockChartModel import StockChartModel


# TODO -- 可以考虑当K线本身宽度比较小的时候，让间隙也小一点
class StockChart(QFrame):
    # 最小、默认、最大K线宽度
    MIN_K_LINE_WIDTH = 6
    DEFAULT_K_LINE_WIDTH = 20
    MAX_K_LINE_WIDTH = 25
    INDEX_TWO_POINT_TIME_DELTA = 45
    # 价格区域宽度
    PRICE_AREA_WIDTH = 60
    # 价格区域对于左侧的边距
    PRICE_MARGIN_LEFT = 5
    # K线之间的间隔
    KLINE_PADDING = 2
    # 价格字体的大小，12个像素
    FONT_SIZE = 12

    mouse_on_changed = Signal(StockKInfo)

    def __init__(self, parent=None):
        super().__init__(parent)
        # 数据模型
        self.model = None
        # K线宽度
        self.each_line_width = self.DEFAULT_K_LINE_WIDTH
        # 当前鼠标位置
        self.curr_mouse_position = QPoint(0, 0)
        # K线的开始和结束下标
        self.start_index = 0
        self.last_index = 0

        pal = QPalette()
        pal.setColor(QPalette.Background, PySide2.QtCore.Qt.white)
        self.setAutoFillBackground(True)
        self.setPalette(pal)

        self.setFocusPolicy(PySide2.QtCore.Qt.ClickFocus)
        # 下面这行代码使得鼠标不按下的时候也能跟踪鼠标移动事件
        self.setMouseTracking(True)

    def set_model(self, model: StockChartModel):
        self.model = model
        self.model.data_changed.connect(self.stock_info_changed)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.begin(self)
        self.paint_k_line(painter, event)
        painter.end()

    def paint_single_k_line(self, painter, x, y, width, height, info):
        delta = info.high - info.low if info.high - info.low else -1
        pen = QPen()
        color = QColor()
        if info.close > info.open or (info.close >= info.open and info.pct_chg > 0):
            color.setRgb(217, 58, 24)
        else:
            color.setRgb(7, 122, 50)
        pen.setColor(color)
        painter.setPen(pen)
        # 计算并绘制上影线部分
        up_delta = info.high - (info.open if info.open > info.close else info.close)
        up_pct = up_delta / delta
        up_start_x = x + width / 2
        up_start_y = y
        up_end_y = y + int(height * up_pct)
        up_line = QLine(up_start_x, up_start_y, up_start_x, up_end_y)
        painter.drawLine(up_line)

        # 计算并绘制实体部分
        main_delta = info.open - info.close if info.open > info.close else info.close - info.open
        main_pct = main_delta / delta
        main_start_y = up_end_y
        main_height = int(main_pct * height)
        main_height = 1 if main_height == 0 else main_height  # 确保主体部分有内容
        main_rect = QRect(x + self.KLINE_PADDING, main_start_y, width - 2 * self.KLINE_PADDING, main_height)
        painter.drawRect(main_rect);
        if info.close < info.open:
            painter.fillRect(main_rect, color)
        else:
            # 此处填充一下背景色，以防分割窗口的横线影响K线的显示
            fill_rect = QRect(x + self.KLINE_PADDING + 1, main_start_y + 1, width - 2 * self.KLINE_PADDING - 1,
                             main_height - 1)
            painter.fillRect(fill_rect, PySide2.QtCore.Qt.white)

        # 计算并绘制下影线部分
        down_start_y = main_start_y + main_height
        down_end_y = y + height
        down_line = QLine(up_start_x, down_start_y, up_start_x, down_end_y)
        painter.drawLine(down_line)

    def paint_k_line(self, painter, event):
        batch_k_line_info = self.model.get_current_stock_k_info()
        # 显示K线区域宽度
        line_area_width = event.rect().width() - self.PRICE_AREA_WIDTH
        line_area_height = event.rect().height()
        real_each_width = self.each_line_width + self.KLINE_PADDING
        # 第一步：统计显示多少根K线
        show_num = line_area_width // real_each_width
        # 第二步：统计显示的开始index以及最后index
        self.calculate_start_last_index(batch_k_line_info, show_num)
        # 第三步：统计一下最大和最小价差
        max_price = 0
        min_price = 10000000
        for i in range(self.start_index, self.last_index + 1):
            if batch_k_line_info.info_list[i].high > max_price:
                max_price = batch_k_line_info.info_list[i].high
            if batch_k_line_info.info_list[i].low < min_price:
                min_price = batch_k_line_info.info_list[i].low
        min_max_delta = max_price - min_price

        # 第三点一步：绘制一下窗格系统，将窗格横向分割成10个小窗格
        pen = QPen()
        color = QColor()
        color.setRgb(212, 208, 208)
        pen.setColor(color)
        painter.setPen(pen)

        each_win_height = line_area_height / 10
        for i in range(1, 11):
            temp_line = QLine(0, each_win_height * i, line_area_width, each_win_height * i)
            painter.drawLine(temp_line)

        # 第四步：开始绘制单根的K线
        x: int = 0
        y: int = 0
        height: int = 0
        for i in range(self.start_index, self.last_index + 1):
            x = real_each_width * (i - self.start_index)
            y = (max_price - batch_k_line_info.info_list[i].high) / min_max_delta * line_area_height
            height = (batch_k_line_info.info_list[i].high - batch_k_line_info.info_list[i].low) / min_max_delta * \
                     line_area_height
            self.paint_single_k_line(painter, x, y, self.each_line_width, height, batch_k_line_info.info_list[i])

        # 第五步：最右侧绘制一个显示栏，用于显示价格(分成四个等份)
        price_font = QFont()
        price_font.setPointSize(self.FONT_SIZE)
        painter.setFont(price_font)
        right_end_line = QLine(line_area_width, 0, line_area_width, event.rect().height())
        painter.drawLine(right_end_line)

        each_price_level_height = event.rect().height() / 4
        for i in range(0, 4):
            cur_price = max_price - min_max_delta * (self.FONT_SIZE + each_price_level_height * i) / \
                        event.rect().height()
            painter.drawText(line_area_width + self.PRICE_MARGIN_LEFT, self.FONT_SIZE + each_price_level_height * i,
                             str(cur_price))

        # 第六步：绘制一下当前鼠标所在K线的位置，加一个十字线，贯穿整个窗口
        k_line_num = self.curr_mouse_position.x() // real_each_width
        # 处理一下，避免垂直的线越过K线显示区，到了右边价格显示区
        if k_line_num > (self.last_index - self.start_index):
            point_x = line_area_width
        else:
            point_x = k_line_num * real_each_width + real_each_width / 2
        horizon_line = QLine(0, self.curr_mouse_position.y(), line_area_width, self.curr_mouse_position.y());
        painter.drawLine(horizon_line)

        vertical_line = QLine(point_x, 0, point_x, event.rect().height())
        painter.drawLine(vertical_line)

        # 绘制一下当前的价格
        price_back_ground_height = self.FONT_SIZE + 4
        mouse_on_price = max_price - min_max_delta * self.curr_mouse_position.y() / event.rect().height()
        mouse_on_price_back = QRect(line_area_width, self.curr_mouse_position.y() - price_back_ground_height + 2,
                                    self.PRICE_AREA_WIDTH, price_back_ground_height)
        painter.drawRect(mouse_on_price_back)
        price_fill_back = QRect(line_area_width + 1, self.curr_mouse_position.y() - price_back_ground_height + 3,
                                self.PRICE_AREA_WIDTH - 2, price_back_ground_height - 2)
        painter.fillRect(price_fill_back, QColor(184, 243, 144))
        painter.drawText(line_area_width + self.PRICE_MARGIN_LEFT, self.curr_mouse_position.y(), str(mouse_on_price))

    def mouseMoveEvent(self, event):
        self.curr_mouse_position = event.pos()
        self.update()
        stock_k_info = StockKInfo()
        # 计算一下当前光标位置下的股票索引
        k_line_num = self.curr_mouse_position.x() // (self.each_line_width + self.KLINE_PADDING)
        batch_k_line_info = self.model.get_current_stock_k_info()
        stock_k_info.ts_code = batch_k_line_info.ts_code
        stock_k_info.ts_name = batch_k_line_info.ts_name
        curr_mouse_on_index = self.start_index + k_line_num
        # 因为右边有价格显示区，所以要避免数组越界的问题
        if curr_mouse_on_index >= len(batch_k_line_info.info_list):
            return

        # 从Model当中获取相应的股票信息
        origin_info = batch_k_line_info.info_list[curr_mouse_on_index]
        stock_k_info.low = origin_info.low
        stock_k_info.high = origin_info.high
        stock_k_info.open = origin_info.open
        stock_k_info.close = origin_info.close
        stock_k_info.pct_chg = origin_info.pct_chg
        stock_k_info.trade_date = origin_info.trade_date

        # 获取一下前一天的价格
        stock_k_info.pre_close = batch_k_line_info.info_list[curr_mouse_on_index - 1].close \
            if curr_mouse_on_index > 0 else 0
        self.mouse_on_changed.emit(stock_k_info)

    def keyReleaseEvent(self, event: QKeyEvent):
        # FIXME -- 每次按动上或者下键，K线的宽度加2？
        if event.key() == PySide2.QtCore.Qt.Key_Left:
            if self.start_index > 0:
                self.start_index = self.start_index - 1
                self.last_index = self.last_index - 1
                self.update()
        elif event.key() == PySide2.QtCore.Qt.Key_Right:
            temp_data = self.model.get_current_stock_k_info()
            if self.last_index < len(temp_data.info_list) - 1:
                self.start_index = self.start_index + 1
                self.last_index = self.last_index + 1
                self.update()
        elif event.key() == PySide2.QtCore.Qt.Key_Up:
            if (self.each_line_width + 2) < self.MAX_K_LINE_WIDTH:
                self.each_line_width = self.each_line_width + 2
            else:
                self.each_line_width = self.MAX_K_LINE_WIDTH
            self.update()
        elif event.key() == PySide2.QtCore.Qt.Key_Down:
            if (self.each_line_width - 2) > self.MIN_K_LINE_WIDTH:
                self.each_line_width = self.each_line_width - 2
            else:
                self.each_line_width = self.MIN_K_LINE_WIDTH
            self.update()

    def resizeEvent(self, event):
        self.update()

    def calculate_start_last_index(self, batch_k_line_info, show_num):
        if self.last_index == 0:
            self.last_index = len(batch_k_line_info.info_list) - 1
            self.start_index = self.last_index - show_num
            self.start_index = 0 if self.start_index < 0 else self.start_index
        else:
            self.start_index = self.last_index - show_num
            self.start_index = 0 if self.start_index < 0 else self.start_index

    @Slot()
    def stock_info_changed(self):
        self.start_index = 0
        self.last_index = 0
        self.repaint()
