import polars as pl
import time

arquivo1=pl.read_csv("/content/202501_NovoBolsaFamilia.csv", separator=";", encoding="latin-1")
arquivo2=pl.read_csv("/content/202502_NovoBolsaFamilia.csv", separator=";", encoding="latin-1")

inicio_tempo=time.time()
df_polars=pl.concat([arquivo1,arquivo2])
display(df_polars.head()) #imprimir os 5 primeiros

print("Tempo de execução com Pandas", time.time()-inicio_tempo, "segundos")