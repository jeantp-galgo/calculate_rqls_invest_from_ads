# Análisis de RQLs e Inversiones

Este proyecto analiza la relación entre las inversiones publicitarias y los RQLs generados para diferentes marcas dentro del marketplace de Galgo México.

## Descripción General

El sistema procesa datos de inversión publicitaria de Meta (Facebook) y Google Ads, junto con datos de RQLs recopilados a través de Amplitude, para generar análisis que permiten entender la efectividad de las campañas publicitarias por marca y región geográfica.

## Estructura del Proyecto

El proyecto se organiza principalmente en los siguientes componentes:

### Analizadores Principales

- **InvestmentAnalyzer**: Procesa y analiza datos de inversión publicitaria de Meta y Google Ads.
- **RQLSAnalyzer**: Procesa y analiza datos de RQLs obtenidos a través de Amplitude.

### Flujo de Trabajo

El flujo de trabajo típico se implementa en el notebook `rql_invest.ipynb`, que integra ambos analizadores para generar informes completos.

## Funcionalidades Principales

### InvestmentAnalyzer

- Carga y procesa datos de inversión de Meta y Google Ads
- Calcula inversiones por plataforma, marca y semana
- Distribuye inversiones genéricas entre marcas según su representación en la plataforma
- Integra datos geográficos para análisis regional
- Genera tablas de inversión total y por plataforma

### RQLSAnalyzer

- Carga y procesa datos de RQLs desde Amplitude
- Filtra datos por rango de fechas y elimina valores nulos
- Integra datos geográficos para análisis regional
- Genera tablas de RQLs por marca y por estado
- Calcula participación (share) de RQLs por marca

### Análisis Integrado

El notebook `rql_invest.ipynb` combina ambos analizadores para:

- Calcular el costo por RQL (CpRQL) para cada marca
- Visualizar la distribución de inversión y RQLs por marca
- Analizar la efectividad de las campañas publicitarias por región

## Requisitos

- Python 3.11+
- Pandas
- MongoDB (para algunas consultas de datos)
- Acceso a datos de Amplitude, Meta Ads y Google Ads

## Uso Típico

1. Configurar las rutas a los archivos de datos de Meta y Google Ads
2. Definir el rango de fechas para el análisis de RQLs
3. Ejecutar el notebook `rql_invest.ipynb` para generar los análisis
4. Revisar las tablas generadas para obtener insights sobre la efectividad de las inversiones

## Ejemplo de Resultados

- Tabla de inversión por marca
- Tabla de RQLs por marca
- Costo por RQL (CpRQL) por marca
- Distribución geográfica de RQLs e inversiones

## Notas Adicionales

El sistema está diseñado para analizar datos semanales y puede filtrar por semana específica para análisis más detallados. La integración de datos geográficos permite entender la efectividad regional de las campañas publicitarias.
