from PySide2.QtCore import QAbstractTableModel


class MainTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)

    def rowCount(self, parent=None, *args, **kwargs):
        return 0