import pandas as pd
from datetime import datetime
from utils.classify_ads import classify_ads
from utils.df_utils import set_week_and_year
from plataforms.utils.calculate import *
from utils.mongodb_call import get_data_general
from plataforms.utils.process_data import get_value_counts_per_brand

class InvestmentAnalyzer:
    def __init__(self, meta_ads_path: str, google_ads_path: str, week: int):
        # Ruta de los archivos de Meta y Google Ads
        self.meta_ads_path = meta_ads_path
        self.google_ads_path = google_ads_path
        self.geo_data_path = r"C:\Users\JTRUJILLO\Documents\Galgo\Scripts\Análisis\rql_inversiones\Geolocalización\MX equivalencias geo regiones - Equivalencias_Medios.csv"

        # DataFrames inputs | Meta y Google Ads
        self.df_meta_ads = None
        self.df_google_ads = None

        # DataFrames inputs | Marcas en la PLP
        self.df_counts_per_brand = None

        # DataFrames de inversión por semana
        self.df_meta_investment_per_weeks = None
        self.df_google_investment_per_weeks = None
        self.df_generic_invest_distribution = None
        self.df_total_investment_plataforms_per_brand = None

        # DataFrames con geolocalización
        self.df_meta_investment_with_geo_data = None
        self.df_google_investment_with_geo_data = None

        # Variables de apoyo
        self.generic_invest = None
        self.total_models = None
        self.week = week
        self.year = 2025

        # DataFrames contar modelos por marca en la PLP
        self.df_data_mx = None

        # Dataframes outputs | Tabla de inversión y shares de inversión por marca
        self.df_investment_per_brands = None
        self.df_shares_investment_per_brand = None

        self.df_investment_per_plataforms_per_brand = None
        self.df_total_investment_per_plataforms_per_brand = None

    def get_df_generic_invest_distribution(self) -> pd.DataFrame:
        """Retorna el DataFrame de la distribución de la inversión genérica."""
        return self.df_generic_invest_distribution
    
    def get_df_total_investment_plataforms_per_brand(self, week: int = None) -> pd.DataFrame:
        """
        Retorna el DataFrame de la inversión total por marca y plataforma. 
        Si se especifica una semana, retorna el DataFrame con la semana especificada.
        Si no se especifica una semana, retorna el DataFrame con todas las semanas.
        """
        if week:
            return self.df_total_investment_plataforms_per_brand[self.df_total_investment_plataforms_per_brand['Semana'] == week]
        else:
            return self.df_total_investment_plataforms_per_brand

    def load_data(self) -> None:
        """Carga y procesa los datos iniciales de Meta y Google Ads."""
        # Cargar datos de Meta
        self.df_meta_ads = pd.read_csv(self.meta_ads_path)
        self.df_meta_ads['Marca'] = self.df_meta_ads['Ad name'].apply(classify_ads)
        self.df_meta_ads = set_week_and_year(self.df_meta_ads)

        # Cargar datos de Google
        self.df_google_ads = pd.read_csv(self.google_ads_path)
        self.df_google_ads['Marca'] = self.df_google_ads['Ad group name'].apply(classify_ads)
        self.df_google_ads = set_week_and_year(self.df_google_ads)
    
    def calculate_platform_investments(self) -> None:
        """Calcula las inversiones por plataforma y la inversión total."""
        self.df_meta_investment_per_weeks = calculate_invest_per_week(self.df_meta_ads)
        self.df_google_investment_per_weeks = calculate_invest_per_week(self.df_google_ads)
        self.invest_total = calculate_invest_from_plataforms_per_brand(
            self.df_meta_investment_per_weeks, 
            self.df_google_investment_per_weeks, 
        )

    # TODO: Hacer filtro de la semana elegida
    def calculate_invest_from_plataforms_per_brand(self) -> None:
        """Calcula las inversiones por plataforma y la inversión total."""
        self.df_total_investment_plataforms_per_brand = calculate_invest_from_plataforms_per_brand(
            self.df_meta_investment_per_weeks, 
            self.df_google_investment_per_weeks, 
        )

    def load_geo_data(self) -> None:
        """Carga los datos de geolocalización."""
        self.df_geo_data = pd.read_csv(self.geo_data_path)

    def merge_geo_data(self) -> None:
        """Merge de los datos de geolocalización."""
        self.df_meta_investment_with_geo_data = pd.merge(self.df_meta_investment_per_weeks, self.df_geo_data[["Amplitude_ips", "Meta"]], 
                                                    left_on="Region", 
                                                    right_on="Meta", 
                                                    how = "left")
        
        self.df_google_investment_with_geo_data = pd.merge(self.df_google_investment_per_weeks, self.df_geo_data[["Amplitude_ips", "GAds"]], 
                                                    left_on="Region", 
                                                    right_on="GAds", 
                                                    how = "left")

    def total_investment_per_brand_per_plataform(self) -> None:
        df_meta_investment_per_weeks = self.df_meta_investment_with_geo_data.copy()
        df_google_investment_per_weeks = self.df_google_investment_with_geo_data.copy()

        df_meta_investment_per_weeks.rename(columns={"Cost": "cost_meta"}, inplace=True)
        df_google_investment_per_weeks.rename(columns={"Cost": "cost_gads"}, inplace=True)

        """Merge de los datos de Meta y Google Ads."""
        df_merged = pd.merge(df_meta_investment_per_weeks[["Marca", "Semana", "Año", "Amplitude_ips", "cost_meta"]], 
                            df_google_investment_per_weeks[["Marca", "Semana", "Año", "Amplitude_ips", "cost_gads"]], 
                            on = ["Marca", "Semana", "Año", "Amplitude_ips"], 
                            how = "outer") # Ya que puede que en Meta no hayan estados (amplitude_ips) que GAds si contenga
        df_merged["cost_meta"].fillna(0, inplace=True)
        df_merged["cost_gads"].fillna(0, inplace=True)
        self.df_investment_per_plataforms_per_brand = df_merged

    def calculate_invest_per_plataforms_per_brand(self) -> None:
        """Calcula las inversiones por plataforma y la inversión total."""
        self.df_total_investment_per_plataforms_per_brand = calculate_invest_per_plataforms_per_brand(self.df_investment_per_plataforms_per_brand, self.week)

    def calculate_generic_invest(self) -> None:
        """Calcula la inversión genérica."""
        self.generic_invest = calculate_generic_invest(
            self.df_total_investment_plataforms_per_brand, 
            self.week,
        )

    def calculate_generic_invest_distribution(self) -> None:
        """Calcula la distribución de la inversión genérica."""
        df_data_mx = get_data_general()
        self.df_counts_per_brand, self.total_models = get_value_counts_per_brand(df_data_mx)

        """Calcula la distribución de la inversión genérica."""
        self.df_generic_invest_distribution = calculate_generic_invest_distribution(
            self.df_counts_per_brand, 
            self.generic_invest,
            self.total_models,
        )

    def distribute_generic_invest(self) -> None:
        """Distribuye la inversión genérica."""
        self.df_generic_invest_distribution = distribute_generic_invest(
            self.df_total_investment_plataforms_per_brand, 
            self.week,
            self.df_generic_invest_distribution,
        )

    def start_analysis(self) -> None:
        """Inicia el análisis de inversión."""
        self.load_data()
        self.load_geo_data()
        self.calculate_platform_investments()
        self.calculate_invest_from_plataforms_per_brand()
        self.calculate_generic_invest()
        self.calculate_generic_invest_distribution()
        self.distribute_generic_invest()
        self.merge_geo_data()
        self.total_investment_per_brand_per_plataform()
        self.calculate_invest_per_plataforms_per_brand()
