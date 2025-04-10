# import sys
# sys.path.append('../')
from mongodb.utils import MongoDBFunctions
import pandas as pd

def verificar_stock(lista_stock):
    if not isinstance(lista_stock, list):
        return False
    return any(item == True or (isinstance(item, str) and item.lower() == 'true') for item in lista_stock)

def get_data_general(show_result = False):
    """
    Obtiene los datos generales de las publicaciones
    """
    mongo_func = MongoDBFunctions()
    db_data_general = mongo_func.publications_data_aggregation("publications")
    df_data_general = pd.DataFrame(db_data_general)

    # Filtrar por país
    df_data_general = df_data_general[df_data_general["country"] == "MX"]

    # Crear el stock final apartir del stock
    df_data_general['stock_final'] = df_data_general['stock'].apply(verificar_stock)

    # Filtrar por publicaciones bases, las cuales son las que no tienen forwarding_code y tienen stock
    df_data_general = df_data_general[(df_data_general["forwarding_code"].isna()) &
                                    (df_data_general["stock_final"] == True)]
    
    df_data_general = df_data_general[["brand", "model" ,"relevance"]]

    if show_result:
        display(df_data_general.head())
    return df_data_general


