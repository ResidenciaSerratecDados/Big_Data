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

#Concatenar arquivos parquet
df_0125=pl.read_parquet('/content/202501_NovoBolsaFamilia.parquet')
df_0225=pl.read_parquet('/content/202501_NovoBolsaFamilia.parquet')
df_010225=pl.concat([df_0125,df_0225])
display(df_010225.head(7000000))

resultado=(df_010225.filter(pl.col("NOME MUNICÍPIO")=="RIO DE JANEIRO")) # Filter directly on df_010225
display(resultado)

display(df_010225.select("CÓDIGO MUNICÍPIO SIAFI"))
display(df_010225.filter(pl.col("MÊS COMPETÊNCIA")==202501))
display(df_010225.select(pl.col("UF")).describe())
display(df_010225.sort("NOME MUNICÍPIO"))
display(df_010225.group_by('UF').agg(pl.col('NOME MUNICÍPIO').value_counts()))

df_010225=df_010225.slice(1)
display(df_010225.head())

df_010225=df_010225.with_columns(pl.lit(0).alias('DK'))
display(df_010225.head())

df_010225=df_010225.drop(['DK'])
display(df_010225.head())

# Exibir uso de memória
processo=psutil.Process()
print('Uso de CPU:', processo.cpu_percent(),'%')
print('Uso de Memória:',processo.memory_info().rss/(1024**2),'MB')