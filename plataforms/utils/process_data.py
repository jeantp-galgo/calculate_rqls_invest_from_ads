import pandas as pd

def get_value_counts_per_brand(df):
    # Traemos las primeras 18 posiciones (primera página de la PLP)
    df = df.sort_values(by="relevance", ascending=False).head(18)
    df_counts_per_brand = df["brand"].value_counts().reset_index()
    total_models = int(df_counts_per_brand['count'].sum())
    return df_counts_per_brand, total_models


# # Crear un diccionario con los datos
# data = {
#     'brand': ['Bajaj', 'Honda', 'Vento', 'TVS', 'CF Moto', 'Italika'],
#     'count': [11, 1, 5, 1, 0, 0]
# }

# # Convertir a DataFrame
# df_marcas = pd.DataFrame(data)

# # Calcular el total de modelos como una variable aparte
# total_modelos = int(df_marcas['count'].sum())

# # Mostrar el DataFrame
# df_marcas