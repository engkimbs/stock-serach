from src.const.ebest import *


class XASessionEvents:
    login_state = STAND_BY

    def OnLogin(self, code, msg):
        XASessionEvents.login_state = RECEIVED
        print(msg)

    def OnDisconnect(self, code, msg):
        pass


class XAQueryEvents:
    query_state = STAND_BY

    def OnReceiveData(self, code):
        XAQueryEvents.query_state = RECEIVED
        print(code)

    def OnReceiveMessage(self, error, nMessageCode, szMessage):
        print(szMessage)
        pass



