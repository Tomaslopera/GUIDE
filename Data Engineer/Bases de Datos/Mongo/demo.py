# pip install pymongo
from pymongo import MongoClient
from datetime import datetime
import pprint

# Reemplaza por tu IP de Windows, por ejemplo "172.23.96.1"
client = MongoClient("localhost:27017")

db = client["demoDB"]
collection = db["colecccion"]

docs = [
    {"nombre": "Ana", "edad": 29, "vip": True, "balance": 1050.75, "tags": ["cliente","premium"], "registro": datetime(2023,5,10)},
    {"nombre": "Luis", "edad": 42, "vip": False, "balance": 0, "tags": [], "registro": datetime(2024,1,20)},
    {"nombre": "María", "edad": 35, "balance": 250.50, "tags": ["nuevo"], "registro": datetime(2025,7,1)},
    {"nombre": "Carlos", "edad": 28, "vip": False, "balance": 500, "tags": ["cliente"], "registro": datetime(2022,12,15)},
    {"nombre": "Lucía", "edad": 31, "vip": True, "balance": None, "registro": datetime(2025,6,30)},
]

# Inserta documentos
result = collection.insert_many(docs)
print("IDs insertados:", result.inserted_ids)

print("\n=== 1. Todos los documentos ===")
for doc in collection.find():
    pprint.pprint(doc)

print("\n=== 2. Consulta: solo VIP (vip: True) ===")
for doc in collection.find({"vip": True}):
    pprint.pprint(doc)

print("\n=== 3. Consulta: edad mayor que 30 ===")
for doc in collection.find({"edad": {"$gt": 30}}):
    pprint.pprint(doc)

print("\n=== 4. Consulta: balance existente y >0 ===")
for doc in collection.find({"balance": {"$exists": True, "$gt": 0}}):
    pprint.pprint(doc)

print("\n=== 5. Consulta: documentos con tag 'cliente' ===")
for doc in collection.find({"tags": "cliente"}):
    pprint.pprint(doc)

print("\n=== 6. Consulta agregada: promedio de balance por vip ===")
pipeline = [
    {"$match": {"balance": {"$gt": 0}}},
    {"$group": {"_id": "$vip", "avgBalance": {"$avg": "$balance"}}}
]
agg = collection.aggregate(pipeline)
for entry in agg:
    pprint.pprint(entry)

print("\n=== 7. Contar documentos registrados antes del 2025 ===")
count = collection.count_documents({"registro": {"$lt": datetime(2025,1,1)}})
print("Cantidad:", count)
