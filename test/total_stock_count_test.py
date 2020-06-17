import requests
import lxml.html

from src.api.xing_TR_request import *
from src.handler import Handler

handler = Handler()
print(handler.get_company_list())

response = requests.get('https://finance.naver.com/item/main.nhn?code=005930')
print(response.text)
tree = lxml.html.fromstring(response.text)
print(tree.xpath('//div[@id="tab_con1"]//em'))
for item in tree.xpath('//div[@id="tab_con1"]//em'):
    print(item.xpath('./text()')[0])