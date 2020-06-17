from sqlalchemy import *

from src.api.xing_TR_request import *
from src.util import stock_util


class DbManager:

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
        self.engine = create_engine('sqlite:///test.db', echo=True)
        self.meta = MetaData()
        self.conn = self.engine.connect()
        self.create_table()

    def create_table(self):
        self.company_margin = Table(
            'company_margin', self.meta,
            Column('shcode', String, primary_key=True),
            Column('hname', String),
            Column('jkrate', Integer),
            Column('sjkrate', Integer)
        )

        self.company_with_risk = Table(
            'company_with_risk', self.meta,
            Column('shcode', String, primary_key=True),
            Column('hname', String),
            Column('date', String),
            Column('type', String)
        )

        self.company_info = Table(
            'company_info', self.meta,
            Column('shcode', String, primary_key=True),
            Column('expcode', String),
            Column('hname', String),
            Column('etfgubun', String),
            Column('gubun', String),
            Column('bu12gubun', String),
            Column('spac_gubun', String),
            Column('total_stock_count', Integer)
        )

        self.company_stock_history_day = Table(
            'company_stock_history_day', self.meta,
            Column('shcode', String),
            Column('date', String),
            Column('open', Integer),
            Column('open', Integer),
            Column('high', Integer),
            Column('low', Integer),
            Column('close', Integer),
            Column('jdiff_vol', Integer),
            Column('value', Integer),
        )

        self.company_stock_history_week = Table(
            'company_stock_history_week', self.meta,
            Column('shcode', String),
            Column('date', String),
            Column('open', Integer),
            Column('open', Integer),
            Column('high', Integer),
            Column('low', Integer),
            Column('close', Integer),
            Column('jdiff_vol', Integer),
            Column('value', Integer),
        )

        self.company_stock_history_month = Table(
            'company_stock_history_month', self.meta,
            Column('shcode', String),
            Column('date', String),
            Column('open', Integer),
            Column('open', Integer),
            Column('high', Integer),
            Column('low', Integer),
            Column('close', Integer),
            Column('jdiff_vol', Integer),
            Column('value', Integer),
        )

        self.meta.create_all(self.engine)

    def check_table_exists(self):
        ret = self.conn.execute(self.company_info.select()).fetchone()
        if ret:
            return True
        return False

    def insert_company_margin(self, company_margin):
        company_current_df = pd.DataFrame(company_margin,
                                          columns=['shcode', 'hname', 'jkrate', 'sjkrate'])
        company_current_df.to_sql('company_margin', con=self.engine, if_exists='append', index=False)

    def insert_company_info(self, company_list):
        company_df = pd.DataFrame(company_list,
                                  columns=['shcode', 'expcode', 'hname', 'etfgubun', 'gubun', 'bu12gubun', 'spac_gubun', 'total_stock_count'])
        company_df.to_sql('company_info', con=self.engine, if_exists='append', index=False)

    def insert_company_stock_history_day(self, company_stock_history_day):
        company_stock_history_day.to_sql('company_stock_history_day', con=self.engine, if_exists='append',
                                         index=False)

    def insert_company_stock_history_week(self, company_stock_history_week):
        company_stock_history_week.to_sql('company_stock_history_week', con=self.engine, if_exists='append',
                                          index=False)

    def insert_company_stock_history_month(self, company_stock_history_month):
        company_stock_history_month.to_sql('company_stock_history_month', con=self.engine, if_exists='append',
                                           index=False)

    def insert_current_stock(self, company_current_stock):
        company_current_df = pd.DataFrame(company_current_stock,
                                          columns=['shcode', 'date', 'close', 'open', 'high', 'low', 'jdiff_vol', 'value'])
        company_current_df.to_sql('company_stock_history_day', con=self.engine, if_exists='append', index=False)

    # def select_stock_yesterday(self, company_shcode_with_50):
    #     ret = self.conn.execute(
    #         select([self.company_stock_history_day])
    #             .where(self.company_stock_history_day.c.date == stock_util.get_valid_stock_previous_day(dt.datetime.now()))).fetchall()

    def insert_company_with_risk(self, company_with_risk):
        company_df = pd.DataFrame(company_with_risk,
                                  columns=['shcode', 'hname', 'date', 'type'])
        company_df.to_sql('company_with_risk', con=self.engine, if_exists='append', index=False)

    def get_last_date(self):
        last_date = self.conn.execute(
            select([func.max(self.company_stock_history_day.c.date)])
                .where(self.company_stock_history_day.c.shcode == '005930')).fetchone()[0]
        return last_date

    def get_company_info_with_shcode(self, shcode_list):
        if type(shcode_list) is str:
            shcode_list = [shcode_list]

        ret = self.conn.execute(
            select([self.company_stock_history_day],
                   self.company_stock_history_day.c.shcode.in_(shcode_list)
                   ).order_by(asc(self.company_stock_history_day.c.date)))
        return ret

    def get_current_info(self, shcode_list):
        if type(shcode_list) is str:
            shcode_list = [shcode_list]

        if not shcode_list:
            ret = self.conn.execute(
                select([self.company_stock_history_day])
                    .where(self.company_stock_history_day.c.date == TODAY)).fetchall()
            return [info for info in ret]
        ret = self.conn.execute(
            select([self.company_stock_history_day],
                   self.company_stock_history_day.c.shcode.in_(shcode_list)
                   )
                .where(self.company_stock_history_day.c.date == TODAY)).fetchall()
        return [info for info in ret]

    def select_company_price_with_previous(self, previous):
        previous_day = stock_util.nth_previous_day(previous)
        ret = self.conn.execute(
            select([self.company_stock_history_day])
            .where(self.company_stock_history_day.c.date == previous_day)
                .order_by(asc(self.company_stock_history_day.c.shcode))
        )
        return ret

    def select_all_history_day(self):
        ret = self.conn.execute(
            select([self.company_stock_history_day])
            .order_by(asc(self.company_stock_history_day.c.date)
        ))
        return ret

    def select_company_info(self):
        return self.conn.execute(
            select([self.company_info])
        )

    def select_company_margin(self):
        return self.conn.execute(
            select([self.company_margin])
        )

    def select_company_with_risk(self):
        return self.conn.execute(
            select([self.company_with_risk])
        )