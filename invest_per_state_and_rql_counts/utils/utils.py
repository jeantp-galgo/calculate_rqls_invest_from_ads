import pandas as pd

def filter_by_date_range(df, date_range):
    """
    Filtra un DataFrame por un rango de fechas proporcionado.
    
    Args:
        df: DataFrame a filtrar
        date_range: Lista de fechas en formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame filtrado que solo contiene filas con fechas en date_range
    """
    df['day'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d')
    df = df[df['day'].isin(date_range)]
    return df