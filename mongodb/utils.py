import mongodb.mongodb_connection as connection
from mongodb.pipelines.extract_variation_data import extract_variation_data
from mongodb.pipelines.extract_publications_data import extract_publications_data
from mongodb.pipelines.extract_info_titles import extract_info_titles_data
from mongodb.pipelines.extract_contentful_gateway import aggregation_contentful_gateway

class MongoDBFunctions:
    def __init__(self):
        self.client = connection.connect_mongodb()

    def list_collections(self):
        db = self.client.db_marketplace
        return db.list_collection_names()
    
    def select_collection(self, collection_name):
        db = self.client.db_marketplace
        collection = db[collection_name]
        return collection
    
    def seleccionar_entryregistries(self):
        db = self.client["contentful-gateway-prod"]  # Usa la notación de diccionario
        coleccion = db["entryregistries"]
        return coleccion
    
    def contentful_gateway_data(self):
        pipeline = aggregation_contentful_gateway()
        coleccion = self.seleccionar_entryregistries()
        resultados = list(coleccion.aggregate(pipeline))
        return resultados

    def variation_data_aggregation(self, collection_name, countryCode, vehicle_type):
        if countryCode not in ["MX", "CO", "CL", "PE"] or vehicle_type not in ["MOTORCYCLES", "CARS"]:
            print(f"Country code {countryCode} or vehicle type {vehicle_type} not supported")
            return None
        else:

            pipeline = extract_variation_data(countryCode, vehicle_type)
            collection = self.select_collection(collection_name)
            results = list(collection.aggregate(pipeline))  

        return results
    
    def publications_data_aggregation(self, collection_name):
        pipeline = extract_publications_data()
        collection = self.select_collection(collection_name)
        results = list(collection.aggregate(pipeline))
        return results
    
    def publications_title_data_aggregation(self, collection_name, countryCode, vehicle_type, condition):
        pipeline = extract_info_titles_data(countryCode, vehicle_type, condition)
        collection = self.select_collection(collection_name)
        results = list(collection.aggregate(pipeline))      
        return results

