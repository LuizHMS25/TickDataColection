import socket
import ConfigTryd as tdt


################################## BOOK ########################################


#========================================#

# Função para gerar keys, necessária para o dicionário da classe que insere no QuestDB

#========================================#

# Função para gerar keys, necessária para o dicionário da classe que insere no QuestDB

def gen_keys_book():
    keys = []

    for i in range(0,tdt.BOOK_LEVELS):
        keys.append("CorrBid_"+str(i+1))
        keys.append("QtdBid_"+str(i+1))
        keys.append("Bid_"+str(i+1))
        keys.append("Ask_"+str(i+1))
        keys.append("QtdAsk_"+str(i+1))
        keys.append("CorrAsk_"+str(i+1))
    return keys

#========================================#

# Função para gerar a lista de sockets e requests, mecessária para execução via multiprocessing

    
def gen_agents(asset_table_data):
    agents = []
    for k in range(0,len(asset_table_data)):
        for i in range(0,tdt.BOOK_LEVELS):
            for j in range(0,6):
                agents.append([socket.socket(socket.AF_INET, socket.SOCK_STREAM),str(i),str(j),asset_table_data[k][0],asset_table_data[k][1]])
                agents[-1][0].setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) #Acelerar o socket
                agents[-1][0].connect((tdt.HOST, tdt.PORT))
    return agents


#========================================#

# Conversão de strings para fazer a requisição via socket


def ByteConvertBook(ativo, tipo_book, linha_book, coluna):
    #Livro de Ofertas Analítico          - Parametro: tipo_book = 0
    #Indice as Colunas do Livro Analitico - (Usar somente os numeros)
    # Corretora =0	Qtd de Cpa=1	Cpa=2	Vda=3	Qtd de Vda=4	Corretora=5

    #Livro de Ofertas Agrupado por Preço - Parametro: tipo_book = 1  
    # Oft=0	Qtd=1	Cpa=2	Vda=3	Qtd=4	Oft=5  

    #Exemplo da string do Livro de Ofertas Analítico:
    #"LVL2$S|tipo_book|ativo|linha_book|coluna#"
    #"LVL2$S|0|DOLU20|0|2#" - Nesse exemplo estou buscando o Livro de Ofertas Analítico do Dolar, solicitando 
    # a (1º) primeira linha do Book e a coluna de compra que nesse caso é o 2 
    return str.encode("LVL2$S|"+tipo_book+"|"+ativo+"|"+linha_book+"|"+coluna+"#")

#========================================#

# Encapsulamento de sockets e tratamento das requisições. Necessário para o mutiprocessing.
# Obs.: a versão atual utiliza a função socket_request_max, mas mantivemos
# a socket_request para registrar.
    
    
def socket_request_book_max_asset(tp):
    s = tp[0]
    book_level = tp[1]
    col = tp[2]
    
    asset = tp[3]
    table = tp[4]
    
    cmd_str = ByteConvertBook(asset, "1", book_level, col)
        
    s.sendall(cmd_str)
    rec = s.recv(8192)

    try:
        f = float(rec.decode("utf-8").replace("LVL2!","").replace("#","").replace(",",".").split(";")[-1])
    except:
        f = np.nan

    return f, table


################################## NEG #########################################

#========================================#

def ByteConvertNeg(dataInfo,asset):
    return str.encode(dataInfo + asset + '#')

#========================================#

def socket_request_neg(s,asset):
    s.sendall(ByteConvertNeg(tdt.NEGOCIO_COMPLETO,asset) )
                
    # Evita perdas de negócios quando a transmissão pelo socket ultrapassa 8192 caracteres 
    # ------------------------------------------------------------------------------------
    data = b''
    chunk = s.recv(8192)
    if len(chunk) >= 8192:
        data = data + chunk
    else:
        data = data + chunk

    return data.decode()

def TradingTimesTradesFeed(strDados):

        TimesTradesList=[]

        aItem = []
        try:
            aItem =strDados.split('|')
        except Exception as ex:
            print('Erro na linha 21 - Metodo: OutputData: ', ex)
        
        #id do Times&Trades
        if ( (len(aItem)==8) and (aItem[1].find('@') == -1) ):
            try:
                TimesTradesList.append([aItem[1], aItem[2].rjust(10), float(aItem[3].replace('.','').replace(',','.').rjust(10)), int(aItem[4].rjust(4)), int(aItem[5].rjust(5)), int(aItem[6].rjust(5)), aItem[7].replace('#','')])
            except Exception as ex:
                print('Erro na linha 29 - Metodo: OutputData: ', ex)

        elif (len(aItem)==8):
            try:
                id      = aItem[1].split('@')
                hora    = aItem[2].split('@')
                preco   = aItem[3].split('@')
                qtde    = aItem[4].split('@')
                cpa     = aItem[5].split('@')
                vda     = aItem[6].split('@')
                agressor= aItem[7].split('@')
                
                if(len(id)==len(agressor)):
                    for i in np.arange(len(id)):
                        try:
                            TimesTradesList.append([id[i], hora[i].rjust(10), float(preco[i].replace('.','').replace(',','.').rjust(10)), int(qtde[i].rjust(4)), int(cpa[i].rjust(5)), int(vda[i].rjust(5)), agressor[i].replace('#','')])        
                        except Exception as ex:
                            print('Erro na linha 49 - Metodo: OutputData: ', ex)
            except Exception as ex:
                print('Erro na linha 51 - Metodo: OutputData: ', ex)
        return TimesTradesList