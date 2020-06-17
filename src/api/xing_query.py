import time

import pythoncom
import win32com.client as winAPI

from src.api.xing_event import *


def request_to_server(xa_query, successive=False):
    time.sleep(DELAY)
    while True:
        ret = xa_query.Request(successive)
        print("ret: " + str(ret))
        if ret == TRANSACTION_REQUEST_EXCESS or ret == -34:
            time.sleep(DELAY)
        else:
            break
    while XAQueryEvents.query_state is STAND_BY:
        pythoncom.PumpWaitingMessages()
    XAQueryEvents.query_state = STAND_BY


def query_XA_dataset(TR: str, field_data: dict, out_field_list: list, occurs: int = 0, block_type: int = 0) -> list:
    IN_BLOCK = TR + "InBlock"
    if block_type == 0:
        OUT_BLOCK = TR + "OutBlock"
    else:
        OUT_BLOCK = TR + "OutBlock1"
    xa_query = winAPI.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEvents)
    xa_query.ResFileName = "C:\\eBEST\\xingAPI\\Res\\" + TR + ".res"
    for (field_name, input_value) in field_data.items():
        xa_query.SetFieldData(IN_BLOCK, field_name, occurs, input_value)

    request_to_server(xa_query)

    result = []
    count = xa_query.GetBlockCount(OUT_BLOCK)
    for idx in range(count):
        block_data = []
        for field_name in out_field_list:
            block_data.append(xa_query.GetFieldData(OUT_BLOCK, field_name, idx))
        result.append(block_data)

    return result


def query_XA_dataset_with_sequence(TR: str, field_data: dict, out_field_list: list, occurs: int = 0) -> list:
    IN_BLOCK = TR + "InBlock"
    OUT_BLOCK = TR + "OutBlock"
    OUT_BLOCK1 = TR + "OutBlock1"
    xa_query = winAPI.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEvents)
    xa_query.ResFileName = "C:\\eBEST\\xingAPI\\Res\\" + TR + ".res"

    for (field_name, input_value) in field_data.items():
        xa_query.SetFieldData(IN_BLOCK, field_name, occurs, input_value)

    request_to_server(xa_query)

    result = []
    while True:

        for idx in range(xa_query.GetBlockCount(OUT_BLOCK1)):
            block_data = []
            for field_name in out_field_list:
                block_data.append(xa_query.GetFieldData(OUT_BLOCK1, field_name, idx))
            result.append(block_data)

        cts_shcode = xa_query.GetFieldData(OUT_BLOCK, "cts_shcode", 0)
        if not cts_shcode:
            break

        field_data["cts_shcode"] = cts_shcode
        for (field_name, input_value) in field_data.items():
            xa_query.SetFieldData(IN_BLOCK, field_name, occurs, input_value)
        request_to_server(xa_query, True)

    return result


def query_XA_dataset_with_occurs(TR: str, field_data: dict, out_field_list: list, occurs: int = 0) -> list:
    IN_BLOCK = TR + "InBlock"
    OUT_BLOCK = TR + "OutBlock"
    OUT_BLOCK1 = TR + "OutBlock1"
    xa_query = winAPI.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEvents)
    xa_query.ResFileName = "C:\\eBEST\\xingAPI\\Res\\" + TR + ".res"

    for (field_name, input_value) in field_data.items():
        xa_query.SetFieldData(IN_BLOCK, field_name, occurs, input_value)

    request_to_server(xa_query)

    result = []
    while True:
        row_count = xa_query.GetBlockCount(OUT_BLOCK1)
        if row_count == 0:
            break

        for i in range(0, row_count):
            block_data = []
            for field_name in out_field_list:
                block_data.append(xa_query.GetFieldData(OUT_BLOCK1, field_name, i))
            result.append(block_data)

        next = xa_query.GetFieldData(OUT_BLOCK, "idx", 0)
        field_data["idx"] = next
        for (field_name, input_value) in field_data.items():
            xa_query.SetFieldData(IN_BLOCK, field_name, occurs, input_value)
        request_to_server(xa_query, True)

    return result