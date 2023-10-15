import ConfigTryd as tdt

from Connection import *
from Insertion import *


from multiprocessing import Pool
from multiprocessing import Process
import time
import requests

import datetime

import os
from os import listdir
from os.path import isfile, join

import numpy as np
import pandas as pd
from numba import jit

import operator as op

print("OK")

stocks_and_tables = [['INDV23','INDV23_20230829']]

def exec_neg(stock_table_pair):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((tdt.HOST, tdt.PORT))
    while True:
        last_lines = TradingTimesTradesFeed(socket_request_neg(s,stock_table_pair[0]))
        InsertionNegData(last_lines,stock_table_pair[1])




if __name__ == '__main__':
    import multiprocessing
    from multiprocessing import Pool, Value
    import ctypes

    with Pool(len(stocks_and_tables)) as p:
        p.map(exec_neg,stocks_and_tables)