import pandas as pd
from pandas import DataFrame
from src.handler import Handler
from src.logic.stock_logic import StockLogic

handler = Handler()
stock_logic = StockLogic()
handler.login()
test = handler.select_all_history_day_with_total_count()
first_day = test.groupby('shcode')['date'].min()
a, b = stock_logic.get_stock_standardbar_and_compared_bar()
print(a)
# left = filtering[filter_item][0]
#                 right = filtering[filter_item][1]
#                 total_stock_count = company_info[company_info['shcode'] == df]
#                 left = df['total_stock_count'] * left / 100
#                 right = df['total_stock_count'] * right / 100
#                 df = df[df['jdiff_vol'].between(left, right)]



#
# first_day = df.groupby('shcode')['date'].min()
# print(first_day)
# print(type(first_day))
# print(list(zip(first_day.index, first_day)))
# first_day_list = list(zip(first_day.index, first_day))
# df.set_index(['shcode', 'date'], inplace=True)
# df = pd.pivot_table(df, index=['shcode', 'date'])
#
# print(df.iloc[~df.index.isin(first_day_list)])
# first_day = df['date'].min()
# df.set_index(['shcode', 'date'], inplace=True)
# print(df)
#
# df = pd.pivot_table(df, index=['shcode', 'date'])
#
# df['avg'] = df.groupby(level='shcode')['close'].apply(lambda x: x.rolling(window=2, min_periods=1).mean())
# print(df)
#
# print(df)
#
# prev = df.shift(periods=1)
# df['prev_percent'] = ((df['close']/prev['close'])-1)*100
# first_day_df = df.xs(first_day, level=1, drop_level=False)
#
# ret = pd.concat([df, first_day_df]).drop_duplicates(keep=False)
# print(ret)
# print(ret.loc['005930'])


# filtering = {
#     'previous': 3,
#     'compare_with': ['전일종가대비종가', 10, '이상']
# }
# df = _filter_with_stock_standardbar(filtering, df)
# print(df)