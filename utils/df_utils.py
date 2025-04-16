import pandas as pd

def fill_na_columns(df, columns_to_fill):
    for column in columns_to_fill["columns"]:
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].fillna(columns_to_fill["value"])
        df[column] = df[column].astype(columns_to_fill["data_type"])
    return df

def set_week_and_year(df, date_column = 'Date'):
    """
    Agrega columnas con el número de semana y año
    """
    df[date_column] = pd.to_datetime(df[date_column])
    df['Semana'] = df[date_column].dt.isocalendar().week
    df['Año'] = df[date_column].dt.year
    return df