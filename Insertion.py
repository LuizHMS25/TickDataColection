from questdb.ingress import Sender, IngressError
import sys

import pandas as pd


################### BOOK #####################
                
def InsertionBookData(keys,t,data,table_name):
    try:
        with Sender('localhost', int(9009)) as sender:
            dicts = {}
            for k in keys:
                dicts[k] = data[keys.index(k)]
                

            sender.row(
                            table_name,
                            symbols={
                                'hora': t,
                                },
                            columns=dicts) 
            sender.flush()

    except IngressError as e:     
        sys.stderr.write(f'Got error: {e}\n')


################### NEG #####################

def InsertionNegData(TimesTradesList,table_name_):
    if len(TimesTradesList)!=0:
        df = pd.DataFrame(TimesTradesList)
        df.columns = ['id','hora','preco','qtde','cpa','vda','agressor']
        try:
            with Sender('localhost', int(9009)) as sender:
                sender.dataframe(df, table_name=table_name_)

        except IngressError as e:
                sys.stderr.write(f'Got error: {e}\n')
