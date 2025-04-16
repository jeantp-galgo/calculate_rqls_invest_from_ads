import pandas as pd

def df_no_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna un DataFrame sin las filas de totales."""
    return df.iloc[:-1]

def get_df_invest_total_per_brands(df: pd.DataFrame) -> pd.DataFrame:
    df = df_no_totals(df)
    return df[["brand_categoria", "original_cost"]]

def get_df_distribute_invest(df: pd.DataFrame) -> pd.DataFrame:
    df = df_no_totals(df)
    return df[["brand_categoria","distribute_cost", "total_cost"]]

def get_others_brands(df_distribute_invest: pd.DataFrame, df_total_investment_plataforms_per_brand: pd.DataFrame) -> pd.DataFrame:
    marcas_a_excluir = df_distribute_invest["brand_categoria"].tolist()
    # Filtrar el dataframe para excluir esas marcas
    df_total_investment_plataforms_per_brand = df_total_investment_plataforms_per_brand[~df_total_investment_plataforms_per_brand["Marca"].isin(marcas_a_excluir)]
    df_total_investment_plataforms_per_brand = df_total_investment_plataforms_per_brand[df_total_investment_plataforms_per_brand["Marca"] != "Generica"]
    df_total_investment_plataforms_per_brand = df_total_investment_plataforms_per_brand.drop(columns = ["Semana", "Año"])

    return df_total_investment_plataforms_per_brand

def prepapre_df_cost_distribution(df_distribute_invest: pd.DataFrame) -> pd.DataFrame:
    df_distribute_invest = df_distribute_invest.drop(columns = ["distribute_cost"])
    # Se renombra para poder juntarlo con la tabla de las marcas que no tienen costos distribuidos
    df_distribute_invest = df_distribute_invest.rename(columns={"brand_categoria": "Marca", "total_cost": "Cost"})

    return df_distribute_invest

def concat_df_cost_distribution(df_distribute_invest: pd.DataFrame, df_total_investment_plataforms_per_brand: pd.DataFrame) -> pd.DataFrame:
    df_concat = pd.concat([df_distribute_invest, df_total_investment_plataforms_per_brand])

    return df_concat

def get_main_brands():
    return ["Bajaj", "Honda", "TVS", "Italika", "Vento", "Suzuki", "Yamaha", "CF Moto"]

def validate_brands(df, main_brands):
    """
    Confirma que todas las marcas principales estén presentes en el DF, sino, crea el registro y lo deja en 0
    """
    
    for marca in main_brands:
        if marca not in df["Marca"].values:
            # Si la marca no existe, crear un registro con valor 0
            nuevo_registro = pd.DataFrame({"Marca": [marca], "Cost": [0]})
            df = pd.concat([df, nuevo_registro], ignore_index=True)

    return df

def group_other_brands(df, main_brands):
    """
    Agrupa las marcas que no son principales en "Otro"
    """
    marcas_no_principales = df[~df["Marca"].isin(main_brands)]
    if not marcas_no_principales.empty:
        costo_otros = marcas_no_principales["Cost"].sum()
        # Eliminar las marcas no principales
        df = df[df["Marca"].isin(main_brands)]

        # Agregar la fila "Otro" con la suma de los costos
        otro_registro = pd.DataFrame({"Marca": ["Otro"], "Cost": [costo_otros]})
        df = pd.concat([df, otro_registro], ignore_index=True)

    return df

def generar_tabla_final(df):

    main_brands = get_main_brands()

    # Se quita la marca de 'otros' ya que de acá se distribuyó los costos
    df_only_brands = df[df["Marca"] != "Otros"]

    df_brands_validated = validate_brands(df_only_brands, main_brands)

    df_validated = group_other_brands(df_brands_validated, main_brands)

    return df_validated

def generate_invest_total(df_generic_invest_distribution: pd.DataFrame, df_total_investment_plataforms_per_brand: pd.DataFrame) -> pd.DataFrame:
    # df_invest_total_per_brands = get_df_invest_total_per_brands(df_generic_invest_distribution)
    df_distribute_invest = get_df_distribute_invest(df_generic_invest_distribution)

    df_other_brands = get_others_brands(df_distribute_invest, df_total_investment_plataforms_per_brand)

    df_distribute_invest = prepapre_df_cost_distribution(df_distribute_invest)

    df_concat = concat_df_cost_distribution(df_distribute_invest, df_other_brands)
    tabla_xd =  generar_tabla_final(df_concat)

    return tabla_xd


def get_df_share_investment_plataforms_per_brand(df_total_investment_plataforms_per_brand: pd.DataFrame) -> pd.DataFrame:
    df_total_investment_plataforms_per_brand_copy = df_total_investment_plataforms_per_brand.copy()
    df_total_investment_plataforms_per_brand_copy["Share Inversión"] = df_total_investment_plataforms_per_brand_copy["Cost"] / df_total_investment_plataforms_per_brand_copy["Cost"].sum()
    df_total_investment_plataforms_per_brand_copy["Share Inversión"] = df_total_investment_plataforms_per_brand_copy["Share Inversión"].apply(lambda x: f"{x:.0%}")
    df_total_investment_plataforms_per_brand_copy = df_total_investment_plataforms_per_brand_copy[["Marca", "Share Inversión"]]

    return df_total_investment_plataforms_per_brand_copy


