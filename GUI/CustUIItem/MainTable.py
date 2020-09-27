from PySide2.QtWidgets import QTableView


class MainTable(QTableView):
    def __init__(self, parent=None):
        super().__init__(parent)
