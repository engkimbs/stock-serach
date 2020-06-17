import math
import multiprocessing as mt

from src.db.db_manager import *
from src.handler import Handler
from src.util.stock_log import StockLog

handler = Handler.instance()
log = StockLog.instance()

def login():
    ID = "kykk0010"
    PASSWORD = "tnpdlem12"

    xa_session = winAPI.DispatchWithEvents("XA_Session.XASession", XASessionEvents)
    if xa_session.IsConnected():
        xa_session.DisconnectServer()

    xa_session.ConnectServer("demo.ebestsec.co.kr", SERVER_PORT)
    xa_session.Login(ID, PASSWORD, "", SERVER_PORT, SHOW_CERTIFICATE_ERROR_DIALOG)

    while XASessionEvents.login_state is STAND_BY:
        pythoncom.PumpWaitingMessages()
    XASessionEvents.login_state = STAND_BY


def entry(gubun):
    login()

    # ETN, ETF, 스팩을 추가적으로 알 수 있음 ( 우선주는 패턴을 찾아야함)
    handler.insert_stock_list_history_data(gubun)


def construct_db():

    p1 = mt.Process(target=entry, args=(2,))
    p1.start()
    p2 = mt.Process(target=entry, args=(3,))
    p2.start()
    p3 = mt.Process(target=entry, args=(4,))
    p3.start()

    handler.insert_company_info()
    handler.insert_company_with_risk_all()
    handler.insert_company_margin()


def main():

    if not handler.check_table_exists():
        construct_db()

    login()

    handler.update_daily_stock()
    #print(handler.filter({}, 0))


    #print(handler.select_merged_company_info_without_risk_info())

    # ret = handler.select_company_with_risk()
    # print(ret)
    # print(ret)
    # print(type(ret))
    # start = time.time()
    # print(ret['shcode'])
    # print(ret['shcode'].str.endswith('0'))
    # print(ret[~ret['shcode'].str.endswith('0')])
    # print(ret[ret.bu12gubun == '01'])

    # print(handler.get_current_info())

    # print(handler.update_daily_stock())
    # elapsed = time.time() - start
    # print(elapsed)


    # for item in tree.xpath('//div[@id="tab_con1"]//em'):
    #     print(item.xpath('./text()'))

    # db.insert_company_info()

    #
    #
    # db_manager = DbManager()
    # company_list = get_company_list()
    # print(company_list)
    # print(len(company_list))
    #
    # # company_shcode = [element[0] for element in company_list]
    # # print(company_shcode)
    # # print(len(company_shcode))
    # #
    # # ETN, ETF, 스팩을 추가적으로 알 수 있음 ( 우선주는 패턴을 찾아야함)
    # company_df = pd.DataFrame(company_list,
    #                           columns=['shcode', 'expcode', 'hname', 'etfgubun', 'gubun', 'bu12gubun', 'spac_gubun'])
    # print(get_stock_list_history_data(company_df['shcode']))


    # company_df['preferred'] = np.where(company_df['shcode'].str.endswith('0') == False, 'T', 'F')
    # company_df['etfgubun'].replace({'0': 'Normal', '1': 'ETF', '2': 'ETN'}, inplace=True)
    # company_df['gubun'].replace({'1': 'KOSPI', '2': 'KOSDAQ'}, inplace=True)

    # company_list = get_company_list()
    # company_shcode = [element[0] for element in company_list]
    #
    # company_shcode_with_50 = [company_shcode[i:i+50] for i in range(0, len(company_shcode), 50)]
    # company_current_list = get_current_price_of_list(company_shcode_with_50)
    # # company_current_df = pd.DataFrame(company_current_list,
    # #                                   columns=['shcode', 'date', 'close', 'open', 'high', 'low', 'jdiff_vol', 'value'])
    # # print(company_current_df)
    # db.insert_current_stock(company_current_list)

    # update_daily_stock()


if __name__ == "__main__":

    start = time.time()
    main()
    elapsed = time.time() - start
    print(str(elapsed) + ' s')
    print(str(math.ceil(elapsed / 60)) + ' m')