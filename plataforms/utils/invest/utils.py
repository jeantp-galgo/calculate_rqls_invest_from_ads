import pandas as pd

from utils.utils import get_main_brands

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

    # Se agrupan las marcas que no son principales
    df_validated = group_other_brands(df_brands_validated, main_brands)

    return df_validated