"""
t8436 
gubun : 0 - 전체, 1 - 코스피, 2 - 코스닥

t1404
gubun : 0 - 전체, 1 - 코스피, 2 - 코스닥
jongchk : 1 - 관리, 2 - 불성실공시, 3 - 투자유의, 4 - 투자환기
cts_shcode : 
처음 조회시는 Space
연속 조회시에 이전 조회한 OutBlock의 cts_shcode 값으로 설정

t1405
gubun : 0 - 전체, 1 - 코스피, 2 - 코스닥
jongchk : 1 - 투자경고, 2 - 매매정지, 3 - 정리매매, 4 - 투자주의, 
          5 - 투자위험, 6 - 위험예고, 7 - 단기과열지정, 8 - 단기과열지정예고
cts_shcode :
처음 조회시는 Space
연속 조회시에 이전 조회한 OutBlock의 cts_shcode 값으로 설정
"""
import datetime as dt
import pandas as pd

from src.api.xing_query import *


class TRRequest:

    _instance = None

    @classmethod
    def _getInstance(cls):
        return cls._instance

    @classmethod
    def instance(cls):
        cls._instance = cls()
        cls.instance = cls._getInstance
        return cls._instance

    def get_company_margin_with_t1411(self):
        return query_XA_dataset_with_occurs("t1411", {"gubun": 0, "jongchk": "", "jkrate": "", "shcode": "", "idx": ""},
                                            ["shcode", "hname", "jkrate", "sjkrate"])

    def get_high_risk_company_with_t1405(self):
        circuit_breakers = query_XA_dataset_with_sequence("t1405", {"gubun": 0, "jongchk": 1, "cts_shcode": ""},
                                                          ["shcode", "hname", "date"])
        circuit_breakers = [stock + ['매매정지'] for stock in circuit_breakers]
        disposal_stock = query_XA_dataset_with_sequence("t1405", {"gubun": 0, "jongchk": 2, "cts_shcode": ""},
                                                        ["shcode", "hname", "date"])
        disposal_stock = [stock + ['정리매매'] for stock in disposal_stock]
        return circuit_breakers + disposal_stock

    def get_warning_company_with_t1404(self):
        management_stocks = query_XA_dataset_with_sequence("t1404", {"gubun": 0, "jongchk": 1, "cts_shcode": ""},
                                                           ["shcode", "hname", "date"])
        management_stocks = [stock + ['관리종목'] for stock in management_stocks]
        unfaithful_disclosure_company = query_XA_dataset_with_sequence("t1404",
                                                                       {"gubun": 0, "jongchk": 2, "cts_shcode": ""},
                                                                       ["shcode", "hname", "date"])
        unfaithful_disclosure_company = [stock + ['불성실공시기업'] for stock in unfaithful_disclosure_company]
        return management_stocks + unfaithful_disclosure_company

    def get_KOSPI_company_list(self):
        return query_XA_dataset("t8436", {"gubun": 1},
                                ['shcode', 'expcode', 'hname', 'etfgubun', 'gubun', 'bu12gubun', 'spac_gubun', 'total_stock_count'])

    def get_KOSDAQ_company_list(self):
        return query_XA_dataset("t8436", {"gubun": 2},
                                ['shcode', 'expcode', 'hname', 'etfgubun', 'gubun', 'bu12gubun', 'spac_gubun', 'total_stock_count'])

    def get_history_data(self, shcode, gubun, sdate='20000101'):
        return query_XA_dataset(TR="t8413",
                                field_data={
                                    "shcode": shcode,
                                    "gubun": gubun,
                                    "qrycnt": 5,
                                    "sdate": sdate,
                                    "edate": dt.datetime.today().strftime("%Y%m%d"),
                                    'cts_date': '',
                                    'comp_yn': 'N'},
                                out_field_list=['date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value'],
                                block_type=1)

    def update_stock_data(self, company_code_list, gubun, db_manager):
        ret = {}
        count = 0

        for item in company_code_list.iteritems():
            count += 1
            shcode = item[1]
            history_data = self.get_history_data(shcode, gubun)
            history_data = pd.DataFrame.from_records(
                data=[[shcode] + data for data in history_data],
                columns=['shcode', 'date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value']
            )
            db_manager.insert_company_stock_history_day(history_data)

        return ret

    def get_current_info(self, shcode_list=None):
        if not shcode_list:
            company_list = self.get_company_list()
            company_shcode = [element[0] for element in company_list]
            company_shcode_with_50 = [company_shcode[i:i + 50] for i in range(0, len(company_shcode), 50)]
        else:
            company_shcode_with_50 = [shcode_list[i:i + 50] for i in range(0, len(shcode_list), 50)]
        return self.get_current_price_of_list(company_shcode_with_50)

    def get_current_price(self, nrec, shcode_list):
        return query_XA_dataset("t8407",
                                field_data={
                                    'nrec': nrec,
                                    'shcode': shcode_list
                                },
                                out_field_list=['shcode', 'price', 'open', 'high', 'low', 'volume', 'value'],
                                block_type=1)

    def get_current_price_of_list(self, company_shcode_with_50):
        ret = []
        for item in company_shcode_with_50:
            current_data = self.get_current_price(len(item), ''.join(item))
            current_data = [current[:1] + [dt.datetime.today().strftime("%Y%m%d")] + current[1:]
                            for current in current_data]
            ret = ret + current_data
        return ret

    def get_company_list(self):
        KOSPI_code = self.get_KOSPI_company_list()
        KOSDAQ_code = self.get_KOSDAQ_company_list()
        KR_LIST = KOSPI_code + KOSDAQ_code
        # for company in KR_LIST:
        #     shcode = company[0]
        #     response = requests.get('https://finance.naver.com/item/main.nhn?code=' + shcode)
        #     tree = lxml.html.fromstring(response.text)
        #
        #     total_stock_count = tree.xpath('//div[@id="tab_con1"]//em')[2].xpath('./text()')[0]
        #     total_stock_count = total_stock_count.replace(',', '')
        #     company.append(total_stock_count)

        return KR_LIST


    # def update_daily_stock(self, db):
    #     last_date = db.get_last_date()
    #     if last_date != TODAY and \
    #             dt.datetime.today().weekday() < 5 and \
    #             dt.datetime.now().hour >= 19:
    #         company_list = self.get_company_list()
    #         company_shcode = [element[0] for element in company_list]
    #
    #         company_shcode_with_50 = [company_shcode[i:i+50] for i in range(0, len(company_shcode), 50)]
    #         company_current_list = self.get_current_price_of_list(company_shcode_with_50)
    #
    #         db.insert_current_stock(company_current_list)
