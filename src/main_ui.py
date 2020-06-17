import datetime
import multiprocessing as mt
import re
import sys

import numpy as np
from PyQt5 import QtWidgets, uic
from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtWidgets import QFileDialog

from src.const.ebest import TODAY
from src.handler import Handler, pd
from src.logic.stock_logic import StockLogic


class PandasModel(QAbstractTableModel):

    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parnet=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None


class Ui(QtWidgets.QMainWindow):
    def __init__(self):
        super(Ui, self).__init__()
        uic.loadUi('main.ui', self)

        self.handler = Handler()
        self.stock_logic = StockLogic()

        self.handler.login()
        if not self.handler.check_table_exists():
            self.construct_db()



        self.setEvent()

        # print(type(self.handler.get_current_info()))
        # df = pd.DataFrame(self.handler.get_current_info())
        # model = PandasModel(df)
        # self.currentStockTableView.setModel(model)

        self.show()
        last_date = self.handler.get_last_date()
        if last_date != TODAY and \
                datetime.datetime.today().weekday() < 5 and \
                datetime.datetime.now().hour >= 19:
            self.statusBar.setText('DB 업데이트를 시작합니다.')
            self.handler.update_daily_stock()
            self.statusBar.setText('DB 업데이트를 완료했습니다.')

    def setEvent(self):
        self.filtering = {}

        self.manageCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.marginCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.stopTradeCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.untrustCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.spacCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.remainCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.etfCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.preferredCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        self.etnCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        # self.tradeValueCheckBox.stateChanged.connect(self.checkBoxFilteringAction)
        # self.tradeVolumeCheckBox.stateChanged.connect(self.checkBoxFilteringAction)

        self.importButton1.clicked.connect(self.importData1)
        self.importButton2.clicked.connect(self.importData2)
        self.importButton3.clicked.connect(self.importData3)
        self.exportButton1.clicked.connect(self.exportData1)
        self.exportButton2.clicked.connect(self.exportData2)
        self.exportButton3.clicked.connect(self.exportData3)
        self.updateStock.clicked.connect(self.update)

    def importData1(self):
        try:
            self.statusBar.setText('데이터를 임포트 중입니다.')
            left = self.leftInput.text()
            right = self.rightInput.text()
            previousDay = self.previousSpinBox.value()
            inputType = str(self.inputTypeComboBox.currentText())
            interval = str(self.intervalListBox.currentText())
            compareType = str(self.compareTypeListBox.currentText())
            upTo = self.upToSpinBox.value()
            lowOrUp = str(self.lowOrUpComboBox.currentText())
            lowOrUp2 = str(self.lowOrUpCheckBox2.currentText())
            tradeValue = True if self.tradeValueCheckBox.isChecked() else False
            tradeVolume = True if self.tradeVolumeCheckBox.isChecked() else False
            tradeSpinBox = self.tradeSpinBox.value()
            tradeVolumeLeftSpinBox = self.tradeVolumeLeftSpinBox.value()
            tradeVolumeRightSpinBox = self.tradeVolumeRightSpinBox.value()
            compareWithIn = self.compareWithinBox.value()
            withinSpinBox = self.withinSpinBox.value()
            compareTypeListBox = str(self.compareTypeListBox.currentText())
            boxRangeLeftSpinBox = self.boxRangeLeftSpinBox.value()
            boxRangeRightSpinBox = self.boxRangeRightSpinBox.value()
            compareTypeComboBox = str(self.compareTypeListBox2.currentText())
            averageRangeComboBox_2 = str(self.averageRangeComboBox_2.currentText())

            self.filtering[inputType] = [int(left), int(right)]
            filtered_stock_list = self.handler.filter(self.filtering, previousDay)

            filtering = {
                'previous': withinSpinBox,
                'compare_with': [compareTypeListBox, upTo, lowOrUp],
                'value': [tradeSpinBox, lowOrUp2],
                'total_stock_count': [tradeVolumeLeftSpinBox, tradeVolumeRightSpinBox]
            }

            survived_shcode = filtered_stock_list.shcode.tolist()
            filtered_stock_df = self.stock_logic.get_stock_standardbar(filtering)
            filtered_stock_df.reset_index(inplace=True)
            filtered_stock_df = filtered_stock_df[filtered_stock_df['shcode'].isin(survived_shcode)]
            filtering = {
                'previous': -(compareWithIn+1),
                'type': compareTypeComboBox,
                'range': [boxRangeLeftSpinBox, boxRangeRightSpinBox],
                'avg_range': int(re.search(r'\d+', averageRangeComboBox_2).group())
            }
            self.currentFilteredData = self.stock_logic.get_stock_filtered_average_compared(filtered_stock_df,
                                                                                            filtering)

            model = PandasModel(self.currentFilteredData)
            self.standardTableView.setModel(model)
            self.statusBar.setText('데이터 임포트가 완료되었습니다')
        except Exception as e:
            print(e)
            self.statusBar.setText('필터 입력을 잘못하셨습니다')

    def importData2(self):
        try:
            self.statusBar.setText('데이터 임포트 중입니다.')
            withinSpinBox = self.withinSpinBox_2.value()
            averageRange = str(self.averageRangeComboBox.currentText())
            previous = self.compareWithinBox_2.value()

            original = self.handler.select_all_history_day()
            df = original.copy()

            df = df.groupby('shcode').tail(withinSpinBox)
            window = int(averageRange)
            df['avg'] = df.groupby('shcode')['close'].apply(lambda x: x.rolling(window=window).mean())
            df.dropna(inplace=True)
            df['touch_count'] = np.where(df['close'] > df['avg'], True, False)

            # df['breakthrough_count'] = df.apply(lambda x: x.high > x.avg, axis=1)
            df['breakthrough_count'] = np.where(df['high'] > df['avg'], True, False)
            df = df.groupby('shcode')[['touch_count', 'breakthrough_count']].sum()
            company_info = self.handler.select_merged_company_info_without_risk_info()[['shcode', 'hname']]
            df = pd.merge(company_info, df, on=['shcode'])
            df['이평선'] = averageRange
            df.rename(columns={'shcode': '종목코드', 'hname': '종목명', 'touch_count': '터치횟수', 'breakthrough_count': '돌파시도횟수'},
                      inplace=True)
            self.filteredAverage = df
            model = PandasModel(self.filteredAverage)
            self.tableView_2.setModel(model)

            # compared = original.copy()
            #
            # first_day = compared.groupby('shcode')['date'].min()
            # first_day_list = list(zip(first_day.index, first_day))
            # prev = compared.shift(periods=1)
            # compared['prev_percent'] = ((compared['close'] / prev['close']) - 1) * 100
            # compared = compared.iloc[~compared.index.isin(first_day_list)]
            #
            #
            # compared = compared.groupby('shcode').tail(previous)
            # compared.reset_index(inplace=True)
            # compared['date'] = pd.to_datetime(compared['date'])
            # compared = compared.set_index('date')
            #
            # week = compared.groupby(['shcode', pd.Grouper(freq='W-MON')]).sum()
            # month = compared.groupby(['shcode', pd.Grouper(freq='M')]).sum()
            # week['avg'] = week.groupby('shcode')['close'].apply(lambda x: x.rolling(window=window).mean())
            # month['avg'] = month.groupby('shcode')['close'].apply(lambda x: x.rolling(window=window).mean())
            # print(week)
            # print(month)

            self.statusBar.setText('임포트가 완료되었습니다.')
        except Exception as e:
            print(e)
            self.statusBar.setText('필터 입력을 잘못하셨습니다')

    def importData3(self):
        try:
            self.statusBar.setText('데이터를 임포트 중입니다.')

            self.currentStockData = pd.DataFrame(self.handler.get_current_info(),
                                                 columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol',
                                                          'value'])
            company_info = self.handler.select_merged_company_info_without_risk_info()[['shcode', 'hname']]
            self.currentStockData = pd.merge(company_info, self.currentStockData, on=['shcode'])
            self.currentStockData.rename(columns={'shcode': '종목코드', 'hname': '종목명', 'date': '날짜', 'open': '시가',
                                                  'high':'고가', 'low': '저가', 'close': '종가', 'jdiff_vol': '거래량','value': '거래대금(백만)'},
                                         inplace=True)
            model = PandasModel(self.currentStockData)
            self.currentStockTableView.setModel(model)
            self.statusBar.setText('데이터 임포트가 완료되었습니다')
        except Exception as e:
            print(e)
            self.statusBar.setText('필터 입력을 잘못하셨습니다')

    def exportData1(self):
        try:
            self.currentDirectory = str(QFileDialog.getExistingDirectory(self, "Select Directory"))
            df = pd.DataFrame(self.currentFilteredData)
            t = datetime.datetime.today()
            filename = "{0}년{1}월{2}일_{3}시{4}분.xlsx".format(t.year, t.month, t.day, t.hour, t.minute)
            filename = self.currentDirectory + "\\" + filename
            # df.sort_values(by=['stock_cnt'], ascending=True, inplace=True)
            df.to_excel(filename, index=False)
            self.statusBar.setText('저장되었습니다.')
        except Exception as e:
            print(e)
            self.statusLabel.setText('파일 위치 권한 혹은 엑셀 포맷을 잘못 지정하였습니다. 다시 저장해주세요')

    def exportData2(self):
        try:
            self.currentDirectory = str(QFileDialog.getExistingDirectory(self, "Select Directory"))
            df = pd.DataFrame(self.filteredAverage)
            t = datetime.datetime.today()
            filename = "{0}년{1}월{2}일_{3}시{4}분.xlsx".format(t.year, t.month, t.day, t.hour, t.minute)
            filename = self.currentDirectory + "\\" + filename
            # df.sort_values(by=['stock_cnt'], ascending=True, inplace=True)
            df.to_excel(filename, index=False)
            self.statusBar.setText('저장되었습니다.')
        except Exception as e:
            print(e)
            self.statusLabel.setText('파일 위치 권한 혹은 엑셀 포맷을 잘못 지정하였습니다. 다시 저장해주세요')

    def exportData3(self):
        try:
            self.currentDirectory = str(QFileDialog.getExistingDirectory(self, "Select Directory"))
            df = pd.DataFrame(self.currentStockData)
            t = datetime.datetime.today()
            filename = "{0}년{1}월{2}일_{3}시{4}분.xlsx".format(t.year, t.month, t.day, t.hour, t.minute)
            filename = self.currentDirectory + "\\" + filename
            # df.sort_values(by=['stock_cnt'], ascending=True, inplace=True)
            df.to_excel(filename, index=False)
            self.statusBar.setText('저장되었습니다.')
        except Exception as e:
            print(e)
            self.statusLabel.setText('파일 위치 권한 혹은 엑셀 포맷을 잘못 지정하였습니다. 다시 저장해주세요')

    def update(self):
        last_date = self.handler.get_last_date()
        if last_date != TODAY and \
                datetime.datetime.today().weekday() < 5 and \
                datetime.datetime.now().hour >= 19:
            self.statusBar.setText("데이터 베이스를 업데이트 중입니다.")
            self.handler.update_daily_stock()
            self.statusBar.setText("데이터 베이스를 업데이트 하였습니다.")
        else:
            self.statusBar.setText("데이터 베이스 업데이트 조건이 아닙니다")

    def checkBoxFilteringAction(self):
        if self.manageCheckBox.isChecked():
            self.filtering['관리종목'] = True
        else:
            self.filtering['관리종목'] = False
        if self.marginCheckBox.isChecked():
            self.filtering['불성실공시기업'] = True
        else:
            self.filtering['불성실공시기업'] = False
        if self.stopTradeCheckBox.isChecked():
            self.filtering['ETF'] = True
        else:
            self.filtering['ETF'] = False
        if self.untrustCheckBox.isChecked():
            self.filtering['ETN'] = True
        else:
            self.filtering['ETN'] = False
        if self.spacCheckBox.isChecked():
            self.filtering['증거금100'] = True
        else:
            self.filtering['증거금100'] = False
        if self.remainCheckBox.isChecked():
            self.filtering['스팩'] = True
        else:
            self.filtering['스팩'] = False
        if self.etfCheckBox.isChecked():
            self.filtering['우선주'] = True
        else:
            self.filtering['우선주'] = False
        if self.preferredCheckBox.isChecked():
            self.filtering['거래정지'] = True
        else:
            self.filtering['거래정지'] = False
        if self.etnCheckBox.isChecked():
            self.filtering['정리매매'] = True
        else:
            self.filtering['정리매매'] = False
        print(self.filtering)

    def entry(self, gubun):
        self.handler.login()

        # ETN, ETF, 스팩을 추가적으로 알 수 있음 ( 우선주는 패턴을 찾아야함)
        self.handler.insert_stock_list_history_data(gubun)

    def construct_db(self):
        p1 = mt.Process(target=self.entry, args=(2,))
        p1.start()
        p2 = mt.Process(target=self.entry, args=(3,))
        p2.start()
        p3 = mt.Process(target=self.entry, args=(4,))
        p3.start()

        self.handler.insert_company_info()
        self.handler.insert_company_with_risk_all()
        self.handler.insert_company_margin()


app = QtWidgets.QApplication(sys.argv)
window = Ui()
app.exec_()
