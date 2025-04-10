import pandas as pd

def filter_df_by_brand(df: pd.DataFrame):

    # Definir las marcas principales
    main_brands = ['Bajaj', 'Honda', 'Vento', 'TVS', 'CF Moto', 'Italika']

    # Crear una función para categorizar las marcas
    def categorizar_marca(marca):
        if marca in main_brands:
            return marca
        else:
            return 'Otros'

    # Aplicar la categorización
    df['brand_categoria'] = df['brand'].apply(categorizar_marca)

    # Agrupar por la categoría de marca
    df_agrupado = df.groupby('brand_categoria').agg({
        'original_cost': 'sum',
        'distribute_cost': 'sum',
        'total_cost': 'sum'
    }).reset_index()

    # Ordenar por costo total en orden descendente
    df_agrupado = df_agrupado.sort_values(by='total_cost', ascending=False)

    return df_agrupado
