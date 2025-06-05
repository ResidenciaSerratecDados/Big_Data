import pandas as pd
import polars as pl
import time
from IPython.display import display
import psutil

#Concatenar arquivos parquet
df_0125=pl.read_parquet('/content/202501_NovoBolsaFamilia.parquet')
df_0225=pl.read_parquet('/content/202501_NovoBolsaFamilia.parquet')
df_010225=pl.concat([df_0125,df_0225])
display(df_010225.head(7000000))

# LAZY:
df_010225 = pl.DataFrame(df_010225).lazy()  
# EXPLAIN:
df_010225.filter(pl.col("NOME MUNICÍPIO")=="RIO DE JANEIRO").explain() 
# COLLECT:
resultado=(df_010225.filter(pl.col("NOME MUNICÍPIO")=="RIO DE JANEIRO").collect()) 
display(resultado)

# Exemplo de Aplicação para Atividades futuras:
df_010225 = pl.DataFrame(df_010225).lazy()
consulta = (
    df_010225.filter(pl.col("NOME MUNICÍPIO") == "RIO DE JANEIRO")
    .group_by("NOME FAVORECIDO")
    .agg(pl.col("VALOR PARCELA").count())             
)
print(consulta.explain())
resultado = consulta.collect()

# Análise Estatística com LazyEvaluation:
consulta = (
    df_010225
    .filter(pl.col("NOME MUNICÍPIO") == "RIO DE JANEIRO")
    .with_columns(pl.col("VALOR PARCELA").str.replace(",", ".").cast(pl.Float64))
    .group_by("NOME FAVORECIDO")
    .agg(pl.col("VALOR PARCELA").mean().alias("MÉDIA VALOR PARCELA"),
         pl.col("VALOR PARCELA").sum().alias("SOMA VALOR PARCELA"))
)
print(consulta.explain())
resultado_estatisticas = consulta.collect()
display(resultado_estatisticas)