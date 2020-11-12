# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'mainwindow.ui'
##
## Created by: Qt User Interface Compiler version 5.15.1
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide2.QtCore import *
from PySide2.QtGui import *
from PySide2.QtWidgets import *

from GUI.CustUIItem.stock_chart import StockChart
from GUI.CustUIItem.MainTable import MainTable

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(867, 472)
        MainWindow.setCursor(QCursor(Qt.SizeHorCursor))
        MainWindow.setMouseTracking(True)
        MainWindow.setTabletTracking(True)
        MainWindow.setAcceptDrops(True)
        self.actionhao = QAction(MainWindow)
        self.actionhao.setObjectName(u"actionhao")
        icon = QIcon()
        icon.addFile(u":/images/create.png", QSize(), QIcon.Normal, QIcon.Off)
        self.actionhao.setIcon(icon)
        self.actioncalculate = QAction(MainWindow)
        self.actioncalculate.setObjectName(u"actioncalculate")
        icon1 = QIcon()
        icon1.addFile(u":/images/calculator.svg", QSize(), QIcon.Normal, QIcon.Off)
        self.actioncalculate.setIcon(icon1)
        self.actioncalculate.setShortcutVisibleInContextMenu(True)
        self.actionrefresh = QAction(MainWindow)
        self.actionrefresh.setObjectName(u"actionrefresh")
        self.actiondaily_info = QAction(MainWindow)
        self.actiondaily_info.setObjectName(u"actiondaily_info")
        self.centralWidget = QWidget(MainWindow)
        self.centralWidget.setObjectName(u"centralWidget")
        self.gridLayout = QGridLayout(self.centralWidget)
        self.gridLayout.setSpacing(6)
        self.gridLayout.setContentsMargins(11, 11, 11, 11)
        self.gridLayout.setObjectName(u"gridLayout")
        self.splitter = QSplitter(self.centralWidget)
        self.splitter.setObjectName(u"splitter")
        self.splitter.setOrientation(Qt.Horizontal)
        self.splitter.setChildrenCollapsible(False)
        self.treeView = QTreeView(self.splitter)
        self.treeView.setObjectName(u"treeView")
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(1)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.treeView.sizePolicy().hasHeightForWidth())
        self.treeView.setSizePolicy(sizePolicy)
        self.treeView.setMinimumSize(QSize(100, 0))
        self.splitter.addWidget(self.treeView)
        self.widget = QWidget(self.splitter)
        self.widget.setObjectName(u"widget")
        sizePolicy1 = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        sizePolicy1.setHorizontalStretch(8)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.widget.sizePolicy().hasHeightForWidth())
        self.widget.setSizePolicy(sizePolicy1)
        self.verticalLayout = QVBoxLayout(self.widget)
        self.verticalLayout.setSpacing(6)
        self.verticalLayout.setContentsMargins(11, 11, 11, 11)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setSpacing(6)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.label = QLabel(self.widget)
        self.label.setObjectName(u"label")

        self.horizontalLayout.addWidget(self.label)

        self.lineEdit = QLineEdit(self.widget)
        self.lineEdit.setObjectName(u"lineEdit")

        self.horizontalLayout.addWidget(self.lineEdit)

        self.comboBox = QComboBox(self.widget)
        self.comboBox.addItem("")
        self.comboBox.addItem("")
        self.comboBox.setObjectName(u"comboBox")
        self.comboBox.setMaximumSize(QSize(69, 16777215))
        self.comboBox.setEditable(False)

        self.horizontalLayout.addWidget(self.comboBox)


        self.verticalLayout.addLayout(self.horizontalLayout)

        self.splitter_2 = QSplitter(self.widget)
        self.splitter_2.setObjectName(u"splitter_2")
        self.splitter_2.setOrientation(Qt.Vertical)
        self.openGLWidget = StockChart(self.splitter_2)
        self.openGLWidget.setObjectName(u"openGLWidget")
        sizePolicy2 = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(8)
        sizePolicy2.setHeightForWidth(self.openGLWidget.sizePolicy().hasHeightForWidth())
        self.openGLWidget.setSizePolicy(sizePolicy2)
        self.splitter_2.addWidget(self.openGLWidget)
        self.tableView = MainTable(self.splitter_2)
        self.tableView.setObjectName(u"tableView")
        sizePolicy3 = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy3.setHorizontalStretch(0)
        sizePolicy3.setVerticalStretch(1)
        sizePolicy3.setHeightForWidth(self.tableView.sizePolicy().hasHeightForWidth())
        self.tableView.setSizePolicy(sizePolicy3)
        self.splitter_2.addWidget(self.tableView)

        self.verticalLayout.addWidget(self.splitter_2)

        self.splitter.addWidget(self.widget)

        self.gridLayout.addWidget(self.splitter, 0, 0, 1, 1)

        MainWindow.setCentralWidget(self.centralWidget)
        self.menuBar = QMenuBar(MainWindow)
        self.menuBar.setObjectName(u"menuBar")
        self.menuBar.setGeometry(QRect(0, 0, 867, 23))
        self.menu = QMenu(self.menuBar)
        self.menu.setObjectName(u"menu")
        self.menu_2 = QMenu(self.menuBar)
        self.menu_2.setObjectName(u"menu_2")
        MainWindow.setMenuBar(self.menuBar)
        self.statusBar = QStatusBar(MainWindow)
        self.statusBar.setObjectName(u"statusBar")
        MainWindow.setStatusBar(self.statusBar)
        self.mainToolBar = QToolBar(MainWindow)
        self.mainToolBar.setObjectName(u"mainToolBar")
        MainWindow.addToolBar(Qt.TopToolBarArea, self.mainToolBar)

        self.menuBar.addAction(self.menu.menuAction())
        self.menuBar.addAction(self.menu_2.menuAction())
        self.menu.addAction(self.actionhao)
        self.menu.addAction(self.actioncalculate)
        self.menu.addAction(self.actionrefresh)
        self.menu.addAction(self.actiondaily_info)
        self.mainToolBar.addAction(self.actionhao)
        self.mainToolBar.addAction(self.actioncalculate)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.actionhao.setText(QCoreApplication.translate("MainWindow", u"hao", None))
        self.actioncalculate.setText(QCoreApplication.translate("MainWindow", u"calculate", None))
        self.actionrefresh.setText(QCoreApplication.translate("MainWindow", u"\u5237\u65b0\u5217\u8868", None))
        self.actiondaily_info.setText(QCoreApplication.translate("MainWindow", u"\u65e5\u7ebf\u4fe1\u606f", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"TextLabel", None))
        self.comboBox.setItemText(0, QCoreApplication.translate("MainWindow", u"K\u7ebf\u56fe", None))
        self.comboBox.setItemText(1, QCoreApplication.translate("MainWindow", u"\u5206\u65f6\u56fe", None))

        self.menu.setTitle(QCoreApplication.translate("MainWindow", u"\u6587\u4ef6", None))
        self.menu_2.setTitle(QCoreApplication.translate("MainWindow", u"\u5e2e\u52a9", None))
    # retranslateUi

