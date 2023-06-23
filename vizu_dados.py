from multiprocessing.managers import RemoteError
import pandas as pd 
import os
import dask.dataframe as dd
import time

#txt = open("logd.dat", encoding = "ISO-8859-1")
#print (txt.read())
names = ["hora", "INFO", "ID", "LOGD", "msg_erro", "codigo"]
dtypes = {'data':object, 'hora':object, 'INFO':object, 'ID':object, 'LOGD':object, 'msg_erro':object, "codigo":object}
#df = dd.read_table("o00407-0157000060150.dat", encoding = "ISO-8859-1",dtype=dtypes, header=None, names=names)
df = dd.read_table("*-*.dat", encoding = "ISO-8859-1", dtype=dtypes, names=names)

#df = df.rename(columns={"Início das operações do logd":"msg_erro"})
print(df.head())

#inicio = time.time()
#print("value_count INFO:")
#print(df.INFO.value_counts().compute())
#fim = time.time()
#print(fim - inicio)

#inicio = time.time()
#print(df.map_partitions(type).compute())
#fim = time.time()
#print(fim - inicio)

inicio = time.time()
df_erro = df.query('INFO == "ERRO"').compute()
print(df_erro.head())

print("value_count ERRO:")
print(df_erro["msg_erro"].value_counts().loc[lambda x : x<2])
fim = time.time()
print(fim - inicio)
###################################
inicio = time.time()
df_alerta = df.query('INFO == "ALERTA"').compute()
print(df_alerta.head())

print("value_count ALERTA:")
print(df_alerta["msg_erro"].value_counts())
fim = time.time()
print(fim - inicio)
###################################
inicio = time.time()
df_externo = df.query('INFO == "EXTERNO"').compute()
print(df_externo.head())

print("value_count EXTERNO:")
print(df_externo["msg_erro"].value_counts())
fim = time.time()
print(fim - inicio)
