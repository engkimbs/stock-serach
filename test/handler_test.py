from src.handler import Handler

import pandas as pd

handler = Handler.instance()

filtering = {
    '시가': [5000, 10000]
}
df = handler.filter(filtering, 0)
print(df)
# df1 = handler.select_company_price_with_previous(5)
# df2 = handler.select_merged_company_info_without_risk_info()
#
# print(df1.columns)
# print(df2.columns)
#
# merged = pd.merge(df1, df2, on=['shcode'])
# df = merged[merged['open'] < 1000]
# print(df)

df.to_excel('temp.xlsx')

# print(df.iloc[-1])

#
# df = handler.select_company_with_risk()
#
# print(df[df['type'] != '정리매매'])
#
# df = df[df['type'] != '정리매매']
#
# #df.to_excel('temp.xlsx')
#
# print(df['shcode'])
#
#
# company_info = handler.select_merged_company_info_without_risk_info()
# #print(company_info.set_index('shcode', inplace=True))
#
# #
# print(df.shcode.tolist())
# b = ['000200', '005930']
# # print(type(df.shcode.tolist()))
# print(company_info[~company_info['shcode'].isin(df.shcode.tolist())])
# print(company_info[company_info['shcode'].isin(df.shcode.tolist())])

