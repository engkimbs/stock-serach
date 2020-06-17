from src.api.xing_TR_request import *
from src.db.db_manager import DbManager


class Handler:

    _instance = None

    @classmethod
    def _getInstance(cls):
        return cls._instance

    @classmethod
    def instance(cls):
        cls._instance = cls()
        cls.instance = cls._getInstance
        return cls._instance

    def __init__(self):
        self.db = DbManager.instance()
        self.api = TRRequest.instance()

    def check_table_exists(self):
        return self.db.check_table_exists()

    def get_company_list(self):
        return self.api.get_company_list()

    def get_current_info(self, shcode_list=None):
        lastdate = self.db.get_last_date()
        if lastdate == TODAY:
            return self.db.get_current_info(shcode_list)
        return self.api.get_current_info(shcode_list)

    def insert_company_with_risk_all(self):
        company_risk = self.api.get_warning_company_with_t1404() + self.api.get_high_risk_company_with_t1405()
        self.db.insert_company_with_risk(company_risk)

    def insert_company_margin(self):
        company_margin = self.api.get_company_margin_with_t1411()
        self.db.insert_company_margin(company_margin)

    def insert_company_info(self):
        company_list = self.api.get_company_list()
        self.db.insert_company_info(company_list)

    def insert_stock_list_history_data(self, gubun):
        company_list = self.api.get_company_list()
        for company in company_list:
            shcode = company[0]
            history_data = self.api.get_history_data(shcode, gubun)
            history_data = pd.DataFrame.from_records(
                data=[[shcode] + data for data in history_data],
                columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value']
            )
            if gubun == 2:
                self.db.insert_company_stock_history_day(history_data)
            elif gubun == 3:
                self.db.insert_company_stock_history_week(history_data)
            elif gubun == 4:
                self.db.insert_company_stock_history_month(history_data)

    def login(self):

        ID = "kykk0010"
        PASSWORD = "tnpdlem12"

        pythoncom.CoInitialize()
        xa_session = winAPI.DispatchWithEvents("XA_Session.XASession", XASessionEvents)
        if xa_session.IsConnected():
            xa_session.DisconnectServer()

        xa_session.ConnectServer("demo.ebestsec.co.kr", SERVER_PORT)
        xa_session.Login(ID, PASSWORD, "", SERVER_PORT, SHOW_CERTIFICATE_ERROR_DIALOG)

        while XASessionEvents.login_state is STAND_BY:
            pythoncom.PumpWaitingMessages()
        XASessionEvents.login_state = STAND_BY

    def get_daily_stock(self):
        company_list = self.api.get_company_list()
        company_shcode = [element[0] for element in company_list]

        company_shcode_with_50 = [company_shcode[i:i + 50] for i in range(0, len(company_shcode), 50)]
        return self.api.get_current_price_of_list(company_shcode_with_50)

    def get_last_date(self):
        return self.db.get_last_date()

    def update_daily_stock(self):
        last_date = self.db.get_last_date()
        if last_date != TODAY and \
                dt.datetime.today().weekday() < 5 and \
                dt.datetime.now().hour >= 19:
            company_list = self.api.get_company_list()
            company_shcode = [element[0] for element in company_list]

            company_shcode_with_50 = [company_shcode[i:i+50] for i in range(0, len(company_shcode), 50)]
            company_current_list = self.api.get_current_price_of_list(company_shcode_with_50)

            self.db.insert_current_stock(company_current_list)

    def select_all_history_day(self):
        ret = self.db.select_all_history_day()
        return pd.DataFrame(ret.fetchall(), columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value'])

    def select_all_history_day_with_total_count(self):
        ret1 = self.select_all_history_day()
        ret2 = self.select_merged_company_info_without_risk_info()[['shcode', 'hname', 'total_stock_count']]
        return pd.merge(ret1, ret2, on=['shcode'])

    def select_merged_company_info_without_risk_info(self):
        ret = self.db.select_company_info()
        company_info = pd.DataFrame(ret.fetchall(), columns=['shcode', 'expcode', 'hname', 'etfgubun', 'gubun', 'bu12gubun', 'spac_gubun', 'total_stock_count'])
        ret = self.db.select_company_margin()
        company_margin = pd.DataFrame(ret.fetchall(),
                                    columns=['shcode', 'hname', 'jkrate', 'sjkrate'])
        merged = pd.merge(company_info, company_margin, on=['shcode', 'hname'])

        # select *
        # from company_info where
        # shcode
        # not like
        # '%0' and bu12gubun = '01';
        def cond(row):
            if row['shcode'].endswith('0') == False and row['bu12gubun'] == '01':
                return 'T'
            else:
                return 'F'

        merged['preferred'] = merged.apply(cond, axis=1)
        return merged

    def select_company_with_risk(self):
        ret = self.db.select_company_with_risk()
        return pd.DataFrame(ret.fetchall(), columns=['shcode', 'hname', 'warning_date', 'type'])

    def select_company_price_with_previous(self, previous):
        ret = self.db.select_company_price_with_previous(previous)
        return pd.DataFrame(ret.fetchall(), columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value'])

    def select_previous_bar_date(self, previous_bar):
        previous_bar += 1
        previous_bar = -previous_bar
        ret = self.db.get_company_info_with_shcode("005930")
        df = pd.DataFrame(ret.fetchall(),
                            columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value'])
        return df.iloc[previous_bar]['date']
    """
    관리종목, 불성실공시기업, ETF, 증거금 100% 종목, 스팩, 우선주, 거래정지, 정리매매, ETN, 종가, 시가, 고가, 저가, 
    0일전 ( 0 봉전), 거래대금, 상장 주식수 대비, 

    이평선 골드 크로스( 이동평균선)
    """
    # 이거 전에 0 봉전 필터링을 통해
    def filter(self, filtering: dict, previous: int):
        company_price_with_previous = self.select_company_price_with_previous(previous)
        company_with_risk = self.select_company_with_risk()
        company_info = self.select_merged_company_info_without_risk_info()
        company_info = pd.merge(company_info, company_price_with_previous, on=['shcode'])
        ret = company_info.copy()
        shcode_list = []
        for filter_item in filtering:
            if filter_item == "관리종목" and filtering[filter_item]:
                shcode_list = company_with_risk[company_with_risk['type'] == '관리종목'].shcode.to_list()
            elif filter_item == "불성실공시기업" and filtering[filter_item]:
                shcode_list = company_with_risk[company_with_risk['type'] == '불성실공시기업'].shcode.to_list()
            elif filter_item == "ETF" and filtering[filter_item]:
                shcode_list = company_info[company_info['etfgubun'] == '1'].shcode.to_list()
            elif filter_item == "ETN" and filtering[filter_item]:
                shcode_list = company_info[company_info['etfgubun'] == '2'].shcode.to_list()
            elif filter_item == "증거금100" and filtering[filter_item]:
                shcode_list = company_info[company_info['sjkrate'] == 100].shcode.to_list()
            elif filter_item == "스팩" and filtering[filter_item]:
                shcode_list = company_info[company_info['spac_gubun'] == 'Y'].shcode.to_list()
            elif filter_item == "우선주" and filtering[filter_item]:
                shcode_list = company_info[company_info['preferred'] == 'T'].shcode.to_list()
            elif filter_item == "거래정지" and filtering[filter_item]:
                shcode_list = company_with_risk[company_with_risk['type'] == '매매정지'].shcode.to_list()
            elif filter_item == "정리매매" and filtering[filter_item]:
                shcode_list = company_with_risk[company_with_risk['type'] == '정리매매'].shcode.to_list()
            elif filter_item == "종가" and filtering[filter_item]:
                left = filtering[filter_item][0]
                right = filtering[filter_item][1]
                shcode_list = company_info[~company_info['close'].between(left, right)].shcode.to_list()
            elif filter_item == "시가" and filtering[filter_item]:
                left = filtering[filter_item][0]
                right = filtering[filter_item][1]
                shcode_list = company_info[~company_info['open'].between(left, right)].shcode.to_list()
            elif filter_item == "고가" and filtering[filter_item]:
                left = filtering[filter_item][0]
                right = filtering[filter_item][1]
                shcode_list = company_info[~company_info['high'].between(left, right)].shcode.to_list()
            elif filter_item == "저가" and filtering[filter_item]:
                left = filtering[filter_item][0]
                right = filtering[filter_item][1]
                shcode_list = company_info[~company_info['low'].between(left, right)].shcode.to_list()
            ret = ret[~ret['shcode'].isin(shcode_list)]
        return ret