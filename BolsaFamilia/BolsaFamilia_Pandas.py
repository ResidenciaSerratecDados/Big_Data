#upload de arquivo csv ou excel

import pandas as pd
import time

arquivo1=pd.read_csv("/content/202501_NovoBolsaFamilia.csv", sep=";", encoding="latin-1")
arquivo2=pd.read_csv("/content/202502_NovoBolsaFamilia.csv", sep=";", encoding="latin-1")

inicio_tempo=time.time()
df_pandas=pd.concat([arquivo1,arquivo2])
display(df_pandas.head()) #imprimir os 5 primeiros

print("Tempo de execução com Pandas", time.time()-inicio_tempo, "segundos")