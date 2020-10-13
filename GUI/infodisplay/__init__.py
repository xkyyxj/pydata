import PySide2
from PySide2.QtCore import QPoint, Slot
from PySide2.QtWidgets import QWidget, QDialog

from Data.Result import StockKInfo
from ui_config.InfoDisplay import Ui_InfoDisplay


class InfoDisplay(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = Ui_InfoDisplay()
        self.ui.setupUi(self)
        self.setWindowFlag(PySide2.QtCore.Qt.FramelessWindowHint)

        self.m_startPoint = QPoint(0, 0)
        self.m_windowPoint = QPoint(0, 0)
        self.m_move = False

    def mousePressEvent(self, event):
        # 当鼠标左键点击时.
        if event.button() == PySide2.QtCore.Qt.LeftButton:
            m_move = True
            # 记录鼠标的世界坐标.
            self.m_startPoint = event.globalPos()
            # 记录窗体的世界坐标.
            self.m_windowPoint = self.frameGeometry().topLeft()

    def mouseMoveEvent(self, event):
        if event.buttons() and PySide2.QtCore.Qt.LeftButton:
            # 移动中的鼠标位置相对于初始位置的相对位置.
            relative_pos = event.globalPos() - self.m_startPoint
            # 然后移动窗体即可.
            self.move(self.m_windowPoint + relative_pos)

    def mouseReleaseEvent(self, event):
        if event.button() == PySide2.QtCore.Qt.LeftButton:
            # 改变移动状态.
            self.m_move = False

    @Slot(StockKInfo)
    def stock_info_changed(self, info: StockKInfo):
        self.ui.ts_code.setText(info.ts_code)
        self.ui.max_price.setText(str(info.high))
        self.ui.min_price.setText(str(info.low))
        self.ui.open_price.setText(str(info.open))
        self.ui.close_price.setText(str(info.close))
        self.ui.pct_chg.setText(str(info.pct_chg))
        self.ui.date.setText(str(info.trade_date))

        # 计算一下振幅
        wave = (info.high - info.low) / info.pre_close * 100 if info.pre_close > 0 else -1
        wave_str = str(wave) + "%" if wave > 0 else "0%"
        self.ui.wave.setText(wave_str)
