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

stocks_and_tables = [['INDV23','BOOK_INDV23_20230829']]
all_tables =  ['BOOK_INDV23_20230829']


def exec_book(stocks_and_tables,all_tables):

    keys = gen_keys_book()
    
    agents = gen_agents(stocks_and_tables)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((tdt.HOST, tdt.PORT))
    with Pool(int(tdt.BOOK_LEVELS*len(stocks_and_tables))) as p:
        while True:

            arr = p.map(socket_request_book_max_asset, agents)
            for t in all_tables:
                arr_ = [x[0] for x in arr if x[1]==t]

                try:
                    InsertionBookData(keys,arr_,t)
                except Exception as ex:
                    print(ex)

if __name__ == '__main__':
   
    import multiprocessing
    from multiprocessing import Pool, Value
    import ctypes

    exec_book(stocks_and_tables,all_tables)