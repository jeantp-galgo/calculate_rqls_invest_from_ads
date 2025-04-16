import pandas as pd
from datetime import datetime
from utils.api_call import get_rql_data
from plataforms.tables_utils.rqls import *
from utils.df_utils import set_week_and_year
from plataforms.investment_analyzer import InvestmentAnalyzer

class RQLSAnalyzer:
    def __init__(self, date_range: list[str]):
        self.date_range = date_range
        self.investment_analyzer = InvestmentAnalyzer  # Recibe una instancia existente

        self.geo_path = r"C:\Users\JTRUJILLO\Documents\Galgo\Scripts\Análisis\rql_inversiones\Geolocalización\MX equivalencias geo regiones - Equivalencias_Medios.csv"
        self.df_geo = pd.read_csv(self.geo_path)

        self.df_rqls_amplitude = None
        self.df_rqls_per_brand = None
        self.df_rqls_per_state = None
        self.df_rqls_amplitude_geo_data = None

        # Dataframes outputs | Tabla de RQLs por marca y shares de RQLs por marca
        self.df_rqls_per_brand_main_brands = None
        self.df_rqls_per_brand_share = None
        self.df_cprql = None

        # Acceso a los datos de inversión desde InvestmentAnalyzer
        self.df_investment_per_brands = None
        if self.investment_analyzer and hasattr(self.investment_analyzer, 'df_investment_per_brands'):
            self.df_investment_per_brands = self.investment_analyzer.df_investment_per_brands


    def load_data(self) -> None:
        """Carga y procesa los datos iniciales de Meta y Google Ads."""
        self.df_rqls_amplitude = get_rql_data()
        self.df_geo = pd.read_csv(self.geo_path)

    def pre_process_data(self) -> None:
        df_rqls_amplitude_filtered_by_date_range = filter_by_date_range(self.df_rqls_amplitude, self.date_range)
        self.df_rqls_amplitude = filter_no_null_values(df_rqls_amplitude_filtered_by_date_range)

    def merge_geo_data(self) -> None:
        df_rql_per_state_and_brand_merged = pd.merge(self.df_rqls_amplitude, self.df_geo, left_on="state", right_on="Amplitude_ips", how = "left")
        df_rql_per_state_and_brand_merged.drop(columns = ["Time", "publication_code", "Amplitude_ips"], inplace = True)
        df_rql_per_state_and_brand_merged.rename(columns = {"MX RQL Online (By LS)--All Users": "rql"}, inplace = True)
        self.df_rqls_amplitude_geo_data = df_rql_per_state_and_brand_merged[["day", "state", "brand", "rql", "Meta", "GAds", "TikTok"]]
        # self.df_rqls_amplitude_geo_data = set_week_and_year(self.df_rqls_amplitude_geo_data, "day")

    def filter_data(self) -> None:
        self.df_rqls_per_state = filter_rql_per_state(self.df_rqls_amplitude_geo_data)
        self.df_rqls_per_brand = filter_rql_per_brand(self.df_rqls_amplitude_geo_data)

    def group_other_brands(self) -> None:
        self.df_rqls_per_brand_main_brands = generar_tabla_final_marcas(self.df_rqls_per_brand)

    def get_share_rqls_per_brand(self) -> None:
        self.df_rqls_per_brand_share = get_df_share_rqls_per_brand(self.df_rqls_per_brand_main_brands)
        
    def start_analysis(self) -> None:
        """Inicia el análisis de inversión."""
        self.load_data()
        self.pre_process_data()
        self.merge_geo_data()
        self.filter_data()
        self.group_other_brands()
        self.get_share_rqls_per_brand()
