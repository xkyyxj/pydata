# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'infodisplay.ui'
##
## Created by: Qt User Interface Compiler version 5.15.1
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide2.QtCore import *
from PySide2.QtGui import *
from PySide2.QtWidgets import *


class Ui_InfoDisplay(object):
    def setupUi(self, InfoDisplay):
        if not InfoDisplay.objectName():
            InfoDisplay.setObjectName(u"InfoDisplay")
        InfoDisplay.resize(200, 241)
        self.widget = QWidget(InfoDisplay)
        self.widget.setObjectName(u"widget")
        self.widget.setGeometry(QRect(22, 12, 144, 213))
        self.verticalLayout = QVBoxLayout(self.widget)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.ts_code = QLabel(self.widget)
        self.ts_code.setObjectName(u"ts_code")

        self.verticalLayout.addWidget(self.ts_code)

        self.date = QLabel(self.widget)
        self.date.setObjectName(u"date")
        self.date.setStyleSheet(u"color: rgb(239, 41, 41);\n"
"font: 13pt \"Cantarell\";")

        self.verticalLayout.addWidget(self.date)

        self.horizontalLayout_2 = QHBoxLayout()
        self.horizontalLayout_2.setObjectName(u"horizontalLayout_2")
        self.max_l = QLabel(self.widget)
        self.max_l.setObjectName(u"max_l")

        self.horizontalLayout_2.addWidget(self.max_l)

        self.max_price = QLabel(self.widget)
        self.max_price.setObjectName(u"max_price")

        self.horizontalLayout_2.addWidget(self.max_price)


        self.verticalLayout.addLayout(self.horizontalLayout_2)

        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.min_l = QLabel(self.widget)
        self.min_l.setObjectName(u"min_l")

        self.horizontalLayout_3.addWidget(self.min_l)

        self.min_price = QLabel(self.widget)
        self.min_price.setObjectName(u"min_price")

        self.horizontalLayout_3.addWidget(self.min_price)


        self.verticalLayout.addLayout(self.horizontalLayout_3)

        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setObjectName(u"horizontalLayout_4")
        self.open_l = QLabel(self.widget)
        self.open_l.setObjectName(u"open_l")

        self.horizontalLayout_4.addWidget(self.open_l)

        self.open_price = QLabel(self.widget)
        self.open_price.setObjectName(u"open_price")

        self.horizontalLayout_4.addWidget(self.open_price)


        self.verticalLayout.addLayout(self.horizontalLayout_4)

        self.horizontalLayout_5 = QHBoxLayout()
        self.horizontalLayout_5.setObjectName(u"horizontalLayout_5")
        self.close_l = QLabel(self.widget)
        self.close_l.setObjectName(u"close_l")

        self.horizontalLayout_5.addWidget(self.close_l)

        self.close_price = QLabel(self.widget)
        self.close_price.setObjectName(u"close_price")

        self.horizontalLayout_5.addWidget(self.close_price)


        self.verticalLayout.addLayout(self.horizontalLayout_5)

        self.horizontalLayout_6 = QHBoxLayout()
        self.horizontalLayout_6.setObjectName(u"horizontalLayout_6")
        self.pct_chg_l = QLabel(self.widget)
        self.pct_chg_l.setObjectName(u"pct_chg_l")
        self.pct_chg_l.setStyleSheet(u"font: 13pt \"Cantarell\";\n"
"color: rgb(239, 41, 41);")

        self.horizontalLayout_6.addWidget(self.pct_chg_l)

        self.pct_chg = QLabel(self.widget)
        self.pct_chg.setObjectName(u"pct_chg")
        self.pct_chg.setStyleSheet(u"font: 13pt \"Cantarell\";\n"
"color: rgb(239, 41, 41);")

        self.horizontalLayout_6.addWidget(self.pct_chg)


        self.verticalLayout.addLayout(self.horizontalLayout_6)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.wave_l = QLabel(self.widget)
        self.wave_l.setObjectName(u"wave_l")

        self.horizontalLayout.addWidget(self.wave_l)

        self.wave = QLabel(self.widget)
        self.wave.setObjectName(u"wave")

        self.horizontalLayout.addWidget(self.wave)


        self.verticalLayout.addLayout(self.horizontalLayout)


        self.retranslateUi(InfoDisplay)

        QMetaObject.connectSlotsByName(InfoDisplay)
    # setupUi

    def retranslateUi(self, InfoDisplay):
        InfoDisplay.setWindowTitle(QCoreApplication.translate("InfoDisplay", u"Dialog", None))
        self.ts_code.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.date.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.max_l.setText(QCoreApplication.translate("InfoDisplay", u"\u6700\u9ad8\u4ef7:", None))
        self.max_price.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.min_l.setText(QCoreApplication.translate("InfoDisplay", u"\u6700\u4f4e\u4ef7:", None))
        self.min_price.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.open_l.setText(QCoreApplication.translate("InfoDisplay", u"\u5f00\u76d8\u4ef7:", None))
        self.open_price.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.close_l.setText(QCoreApplication.translate("InfoDisplay", u"\u6536\u76d8\u4ef7:", None))
        self.close_price.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.pct_chg_l.setText(QCoreApplication.translate("InfoDisplay", u"\u6da8\u8dcc\u5e45:", None))
        self.pct_chg.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
        self.wave_l.setText(QCoreApplication.translate("InfoDisplay", u"\u632f\u5e45:", None))
        self.wave.setText(QCoreApplication.translate("InfoDisplay", u"TextLabel", None))
    # retranslateUi

