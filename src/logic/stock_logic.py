from pandas import DataFrame
import numpy as np

from src.api.xing_TR_request import TRRequest
from src.db.db_manager import DbManager, pd
from src.handler import Handler


class StockLogic:
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
        self.handler = Handler()
        self.stock = self.handler.select_all_history_day_with_total_count()
        self.data_series = self.stock[self.stock['shcode']=='005930']['date']
        # self.db = DbManager.instance()
        # self.api = TRRequest.instance()

    def get_stock_standardbar(self, filtering:dict):
        return self._get_stock_standardbar(filtering)

    def get_stock_filtered_average(self):
        return self._get_filtered_stock_average()

    def get_stock_filtered_average_compared(self, stock:DataFrame, filtering:dict):
        return self._get_compared_bar(stock, filtering)

    def _get_stock_standardbar(self, filtering):
        # filtering = {
        #     'previous': 150,    # 인풋값일 때는 +1 을 해야함
        #     'compare_with': ['전일종가대비종가', 10, '이상'],
        #     'value': [50, '이상'],
        #     'total_stock_count':  [0, 1000]
        # }
        return self._filter_with_stock_standardbar(filtering)

    def _get_compared_bar(self, stock_standardbar:DataFrame, filtering:dict):
        df = self.stock.copy()

        stock_standardbar.reset_index(inplace=True)
        stock_standardbar.rename(columns={
            'shcode': '종목코드',
            'hname': '종목명',
            'date': '생성일',
            'open': '시가',
            'close': '종가',
            'high': '고가',
            'low': '저가',
            'jdiff_vol': '주식거래량',
            'value': '거래대금',
            'total_stock_count':'총발행량',
            'prev_percent': '상승률'
        }, inplace=True)

        previous = filtering['previous']
        df = df.groupby('shcode').nth(previous)
        df.reset_index(inplace=True)
        df.rename(columns={
            'shcode':'종목코드',
            'date': '비교일',
            'close': '비교종가',
            'open': '비교시가',
            'high': '비교고가',
            'low': '비교저가',
        }, inplace=True)


        df = pd.merge(stock_standardbar, df, left_on=['종목코드'], right_on=['종목코드'])
        percent = self.stock.copy()
        first_day = percent.groupby('shcode')['date'].min()
        first_day_list = list(zip(first_day.index, first_day))
        percent.set_index(['shcode', 'date'], inplace=True)

        prev = percent.shift(periods=1)
        percent['전일종가대비상승률'] = ((percent['close'] / prev['close']) - 1) * 100
        percent = percent.iloc[~percent.index.isin(first_day_list)]
        percent.reset_index(inplace=True)
        percent = percent[['shcode', 'date', '전일종가대비상승률']]

        df = pd.merge(df, percent, left_on=['종목코드', '비교일'], right_on=['shcode', 'date'])


        avg_range = filtering['avg_range']
        gold_cross = self.stock.copy()
        gold_cross['avg'] = gold_cross.groupby('shcode')['close']\
            .apply(lambda x: x.rolling(window=avg_range).mean())
        gold_cross.dropna(inplace=True)
        gold_cross = gold_cross[['shcode', 'date', 'avg']]
        df = pd.merge(df, gold_cross, left_on=['종목코드', '비교일'], right_on=['shcode', 'date'])
        df = df[df['비교종가'] >= df['avg']]


        def calc_loc(low, close, current):
            if current <= low:
                return (current-low)/low * 100
            # close 와 low 가 같을시에는?
            elif close == low:
                return 999999
            else:
                return (current-low)/(close-low) * 100

        df.dropna(inplace=True)
        range = filtering['range']
        type = filtering['type']
        if type == "종가":
            df['주가위치(%)'] = df.apply(lambda x: calc_loc(x['저가'], x['종가'], x['비교종가']), axis=1)
        elif type == "시가":
            df['주가위치(%)'] = df.apply(lambda x: calc_loc(x['저가'], x['종가'], x['비교시가']), axis=1)
        elif type == "고가":
            df['주가위치(%)'] = df.apply(lambda x: calc_loc(x['저가'], x['종가'], x['비교고가']), axis=1)
        elif type == "저가":
            df['주가위치(%)'] = df.apply(lambda x: calc_loc(x['저가'], x['종가'], x['비교저가']), axis=1)
        df = df[df['주가위치(%)'].between(range[0], range[1])]

        date_series = self.stock[self.stock['shcode'] == '005930']['date']
        std_date, current_date = df['생성일'], df['비교일']
        start_idx = [date_series[date_series == date].index[0] for date in std_date]
        current_idx = [date_series[date_series == date].index[0] for date in current_date]

        period_list = [x1-x2 for (x1,x2) in zip(current_idx, start_idx)]

        df['period'] = pd.Series(period_list)
        df['발행주식수대비(%)'] = df['주식거래량']/df['총발행량']*100
        df = df[['종목코드', '생성일', '거래대금', '발행주식수대비(%)', '종가', '상승률', '주가위치(%)', 'period', '전일종가대비상승률']]

        return df


    def _filter_with_stock_standardbar(self,filtering: dict):
        df = self.stock.copy()
        first_day = df.groupby('shcode')['date'].min()
        first_day_list = list(zip(first_day.index, first_day))
        df.set_index(['shcode', 'date'], inplace=True)
        df.total_stock_count = df.total_stock_count.astype(float)

        # df = pd.pivot_table(df, index=['shcode', 'date'])
        for filter_item in filtering:
            if filter_item == "previous":
                previous = filtering[filter_item]
                df = df.groupby('shcode').tail(previous)
            elif filter_item == "compare_with":
                compare_with = filtering[filter_item][0]
                percent = filtering[filter_item][1]
                type = filtering[filter_item][2]
                if compare_with == "전일종가대비종가":
                    # first_day = df.groupby('shcode')['date'].min()
                    # first_day_list = list(zip(first_day.index, first_day))
                    # df.set_index(['shcode', 'date'], inplace=True)
                    # df = pd.pivot_table(df, index=['shcode', 'date'])

                    # 각 종목의 맨 처음 날짜는 전일 데이터가 없으므로 삭제
                    prev = df.shift(periods=1)
                    df['prev_percent'] = ((df['close'] / prev['close']) - 1) * 100
                    df = df.iloc[~df.index.isin(first_day_list)]
                    if type == '이상':
                        df = df[df['prev_percent'] >= percent]
                    else:
                        df = df[df['prev_percent'] <= percent]

                elif compare_with == "시가대비종가":
                    df['prev_percent'] = ((df['close'] / df['open']) - 1) * 100
                    if type == '이상':
                        df = df[df['prev_percent'] >= percent]
                    else:
                        df = df[df['prev_percent'] <= percent]
                elif compare_with == "저가대비고가":
                    df['prev_percent'] = ((df['high'] / df['low']) - 1) * 100
                    if type == '이상':
                        df = df[df['prev_percent'] >= percent]
                    else:
                        df = df[df['prev_percent'] <= percent]
                elif compare_with == "시가대비고가":
                    df['prev_percent'] = ((df['high'] / df['open']) - 1) * 100
                    if type == '이상':
                        df = df[df['prev_percent'] >= percent]
                    else:
                        df = df[df['prev_percent'] <= percent]
                elif compare_with == "종가대비고가":
                    df['prev_percent'] = ((df['high'] / df['close']) - 1) * 100
                    if type == '이상':
                        df = df[df['prev_percent'] >= percent]
                    else:
                        df = df[df['prev_percent'] <= percent]
            elif filter_item == "value":
                value = filtering[filter_item][0]
                # DB의 거래대금 단위는 백만, input 단위는 억원
                value *= 100
                type = filtering[filter_item][1]
                if type == '이상':
                    df = df[df['value'] >= value]
                else:
                    df = df[df['value'] <= value]
            elif filter_item == "total_stock_count":
                left = filtering[filter_item][0]
                right = filtering[filter_item][1]
                df = df[(df['jdiff_vol'] >= df['total_stock_count'] * left / 100)
                        & (df['jdiff_vol'] <= df['total_stock_count'] * right / 100)]


        g = df.groupby('shcode')
        ret = pd.concat([g.tail(1)]).drop_duplicates()
        # prev = df.shift(periods=1)
        # df['prev_percent'] = ((df['close'] / prev['close']) - 1) * 100
        return ret

    def _get_filtered_stock_average(self):
        MARGIN = 1.5
        avg = self.stock.copy()

        pass

    def _get_compared_average_stock_bar(self):
        pass