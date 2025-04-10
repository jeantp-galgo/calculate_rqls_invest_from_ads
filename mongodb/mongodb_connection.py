from pymongo import MongoClient
from dotenv import load_dotenv # Cargar variables de entorno
import socks
import socket
import os
load_dotenv()

proxy_host = "localhost"
proxy_port = 9090

def connect_mongodb():
    socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
    socket.socket = socks.socksocket
    client = MongoClient(generate_url(os.getenv("DB_USERNAME"), os.getenv("DB_PASSWORD")))
    return client
    
def generate_url(username, password):
    return f"mongodb://{username}:{password}@shared-shard-00-00.eh6ne.mongodb.net:27017,shared-shard-00-01.eh6ne.mongodb.net:27017,shared-shard-00-02.eh6ne.mongodb.net:27017/?replicaSet=atlas-48izhi-shard-0&ssl=true&authSource=admin"
