import pandas as pd
from utils.calculate_utils import *


def calculate_invest_per_week(df: pd.DataFrame):
    """
     Calcula el costo total por semana y año
    """
    df_resumen = df.groupby(['Marca', 'Semana', 'Año'])['Cost'].sum().reset_index()
    df_resumen_semana_año = df_resumen.sort_values(['Año', 'Semana', 'Cost'], ascending=[True, True, False])
    return df_resumen_semana_año

def calculate_invest_from_plataforms_per_brand(df_meta_ads: pd.DataFrame, df_google_ads: pd.DataFrame, show_result: bool = False):
    """
        Calcula el costo total por semana y año de ambas plataformas
    """

    # Unir ambos dataframes
    df_combined = pd.concat([df_google_ads, df_meta_ads])
    # Agrupar por Marca, Semana y Año, y sumar el costo total
    resumen_total = df_combined.groupby(['Marca', 'Semana', 'Año'])['Cost'].sum().reset_index()

    # Ordenar el resultado
    resumen_total = resumen_total.sort_values(['Año', 'Semana', 'Cost'], ascending=[True, True, False])

    # Calcular costo total
    costo_total = df_combined['Cost'].sum()

    # Mostrar resultado
    if show_result:
        # print(f"Costo total: {costo_total}")
        display(resumen_total)
    return resumen_total

def calculate_generic_invest(df: pd.DataFrame, week: int):
    """
        Calcular inversión para la marca Genérica en una semana específica
    """

    # Seleccion de la semana a calcular inversión
    invest_total_week = df.copy()
    invest_total_week = invest_total_week[invest_total_week["Semana"] == week]

    # Obtener el valor de inversión para la Genérica
    valor_cost_generica = int(invest_total_week[invest_total_week["Marca"] == "Generica"]["Cost"].values[0])
    print(f"Inversión genérica semana {week} es: {valor_cost_generica}")
    return valor_cost_generica

def calculate_generic_invest_distribution(df: pd.DataFrame, generic_invest: int, total_models: int, show_result: bool = False):
    """
        Calcular distribución de inversión para la marca Genérica en una semana específica
    """
    
    # Crear un nuevo DataFrame para la distribución del valor de Generica entre las marcas
    df_distribucion = df.copy()

    # Calcular el valor asignado a cada marca basado en la fórmula: valor_cost_generica * (total_modelo_marca / total_modelos)
    df_distribucion['Cost'] = df_distribucion['count'].apply(lambda x: generic_invest * (x / total_models))

    # Redondear los valores para mejor visualización
    df_distribucion['Cost'] = df_distribucion['Cost'].round(2)

    fila_total = pd.DataFrame({
    'brand': ['Total'],
    'count': [total_models],
    'Cost': [generic_invest]
    })

    # Concatenar el DataFrame original con la fila de total
    df_distribucion_total = pd.concat([df_distribucion, fila_total], ignore_index=True)

    # Mostrar el resultado
    if show_result:
        display(df_distribucion_total)

    return df_distribucion_total

def distribute_generic_invest(df_invest_total: pd.DataFrame, week: int, df_total_distribution: pd.DataFrame, show_result: bool = False):
    # Copias para evitar mensajes de advertencia
    df_resumen = df_invest_total.copy()
    df_distribucion = df_total_distribution.copy()

    # Filtramos por semana
    df_resumen = df_resumen[df_resumen["Semana"] == week]

    # Renombramos las columnas para evitar confusiones
    df_resumen = df_resumen.rename(columns={'Marca': 'brand', 'Cost': 'original_cost'})
    df_distribucion = df_distribucion.rename(columns={'Cost': 'distribute_cost'})

    # Fusionamos los DataFrames por la marca
    df_suma_costos = pd.merge(
        df_resumen[['brand', 'original_cost']], 
        df_distribucion[['brand', 'distribute_cost']], 
        on='brand', 
        how='outer'
    )

    # Reemplazamos los valores NaN por 0
    df_suma_costos = df_suma_costos.fillna(0)

    # Calculamos la suma total de costos para cada marca
    df_suma_costos['total_cost'] = df_suma_costos['original_cost'] + df_suma_costos['distribute_cost']

    # Ordenamos el DataFrame por costo total en orden descendente
    df_suma_costos = df_suma_costos.sort_values(by='total_cost', ascending=False)

    # Filtrar las filas de "Generica" y "Total"
    df_sin_generica_total = df_suma_costos[~df_suma_costos['brand'].isin(['Generica', 'Total'])]

    df_sin_generica_total = filter_df_by_brand(df_sin_generica_total)

    original_cost = df_sin_generica_total['original_cost'].sum()
    distribute_cost = df_sin_generica_total['distribute_cost'].sum()
    total_cost = df_sin_generica_total['total_cost'].sum()

    fila_total = pd.DataFrame({
    'brand_categoria': ['Total'],
    'original_cost': [original_cost],   
    'distribute_cost': [distribute_cost],
    'total_cost': [total_cost]
    })

    df_sin_generica_total = pd.concat([df_sin_generica_total, fila_total], ignore_index=True)

    if show_result:
        display(df_sin_generica_total)

    return df_sin_generica_total