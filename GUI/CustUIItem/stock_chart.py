from PySide2.QtWidgets import QLCDNumber, QSlider, QWidget, QVBoxLayout, QApplication, QGridLayout
from PySide2.QtCore import Qt
from PySide2.QtWidgets import QFrame
from PySide2.QtGui import QPaintEvent
from PySide2.QtGui import QPainter
from PySide2.QtGui import QBrush
from PySide2.QtGui import QColor
from PySide2.QtCore import QPoint
from PySide2.QtCore import QSize


class StockChart(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        pass

    def paintEvent(self, event):
        painter = QPainter(self)
        width = self.width()
        height = self.height()
        painter.setBrush(QBrush(QColor(0xFF, 0xFF, 0xFF)))  # 设置图刷的颜色
        painter.drawRect(0, 0, width, height)
