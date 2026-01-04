import pandas as pd

def model(dbt, session):
    # 1. Cargar la referencia (Esto devuelve un DataFrame de Snowpark)
    dbt.config(materialized='table')
    df_snowpark = dbt.ref("stg_app__customers")

    # 2. Convertir a Pandas (Trae los datos a memoria del worker)
    df = df_snowpark.to_pandas()
    # ---------------------------------------------------------

    # Opción B: Si la lógica fuera más compleja, defines una función
    def calcular_score(fila):
        if fila['REGION'] == 'EMEA':
            return 10
        elif fila['REGION'] == 'APAC':
            return 5
        return 0
    # 
    df['score'] = df.apply(calcular_score, axis=1)

    # ---------------------------------------------------------
    # 3. Retornar el DataFrame final
    # ---------------------------------------------------------
    return df