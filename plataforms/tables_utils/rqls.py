import pandas as pd

def filter_by_date_range(df: pd.DataFrame, date_range: list[str]) -> pd.DataFrame:
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

def filter_no_null_values(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "MX RQL Online (By LS)--All Users"] = df.loc[:, "MX RQL Online (By LS)--All Users"].astype(float)
    df = df[df["MX RQL Online (By LS)--All Users"] != 0]
    df.loc[:, "MX RQL Online (By LS)--All Users"] = df.loc[:, "MX RQL Online (By LS)--All Users"].astype(int)

    return df

def filter_rql_per_state(df: pd.DataFrame) -> pd.DataFrame:
    rql_resumen_per_state = df.groupby(['state'])['rql'].sum().reset_index()
    return rql_resumen_per_state

def filter_rql_per_brand(df: pd.DataFrame) -> pd.DataFrame:
    rql_resumen_per_brand = df.groupby(['brand'])['rql'].sum().reset_index()
    return rql_resumen_per_brand

def get_main_brands():
    return ["Bajaj", "Honda", "TVS", "Italika", "Vento", "Suzuki", "Yamaha", "CF Moto"]

def group_other_brands(df: pd.DataFrame, main_brands: list[str]) -> pd.DataFrame:
    """
    Agrupa las marcas que no son principales en "Otro"
    """
    marcas_no_principales = df[~df["brand"].isin(main_brands)]
    if not marcas_no_principales.empty:
        costo_otros = marcas_no_principales["rql"].sum()
        # Eliminar las marcas no principales
        df = df[df["brand"].isin(main_brands)]

        # Agregar la fila "Otro" con la suma de los costos
        otro_registro = pd.DataFrame({"brand": ["Otro"], "rql": [costo_otros]})
        df = pd.concat([df, otro_registro], ignore_index=True)

    return df

def generar_tabla_final_marcas(df: pd.DataFrame) -> pd.DataFrame:

    main_brands = get_main_brands()

    # Se agrupan las marcas que no son principales
    df_validated = group_other_brands(df, main_brands)

    return df_validated

def get_df_share_rqls_per_brand(df_rqls_per_brand: pd.DataFrame) -> pd.DataFrame:
    df_rqls_per_brand_copy = df_rqls_per_brand.copy()
    df_rqls_per_brand_copy["Share RQLs"] = df_rqls_per_brand_copy["rql"] / df_rqls_per_brand_copy["rql"].sum()
    df_rqls_per_brand_copy["Share RQLs"] = df_rqls_per_brand_copy["Share RQLs"].apply(lambda x: f"{x:.0%}")
    df_rqls_per_brand_copy = df_rqls_per_brand_copy[["brand", "Share RQLs"]]

    return df_rqls_per_brand_copy

def get_df_cost_per_rql(df_investment_per_brands: pd.DataFrame, df_rqls_per_brand_main_brands: pd.DataFrame) -> pd.DataFrame:
    df_cost_per_rql = pd.merge(df_investment_per_brands, df_rqls_per_brand_main_brands, 
                            left_on = "Marca",
                            right_on = "brand",
                            how="left")
    df_cost_per_rql["CpRQL"] = df_cost_per_rql["Cost"] / df_cost_per_rql["rql"]
    df_cost_per_rql["CpRQL"] = df_cost_per_rql["CpRQL"].round(0).apply(lambda x: f"${int(x)}")
    df_cost_per_rql = df_cost_per_rql[["Marca", "CpRQL"]]
    return df_cost_per_rql

