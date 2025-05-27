import pandas as pd
import polars as pl
import time
import psutil

arquivo1=pl.read_csv("/content/202501_NovoBolsaFamilia.csv", separator=";", encoding="latin-1")

inicio_tempo=time.time()
df_polars1=pl.concat([arquivo1])

parquet1=df_polars1.write_parquet("/content/202501_NovoBolsaFamilia.parquet")
print("Tempo de execução com Pandas", time.time()-inicio_tempo, "segundos")
print(parquet1)

arquivo2=pl.read_csv("/content/202502_NovoBolsaFamilia.csv", separator=";", encoding="latin-1")

inicio_tempo=time.time()
df_polars2=pl.concat([arquivo2])

parquet2=df_polars2.write_parquet("/content/202502_NovoBolsaFamilia.parquet")
print("Tempo de execução com Pandas", time.time()-inicio_tempo, "segundos")
print(parquet2)

# Exibir uso de memória
processo=psutil.Process()
print('Uso de CPU:', processo.cpu_percent(),'%')
print('Uso de Memória:',processo.memory_info().rss/(1024**2),'MB')