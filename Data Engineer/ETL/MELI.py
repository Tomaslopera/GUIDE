from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import pandas as pd
import re

from pymongo import MongoClient
from datetime import datetime
import pprint

PRICE_RE      = re.compile(r'\$\s*\d{1,3}(?:\.\d{3})+(?:,\d+)?')
DISCOUNT_RE   = re.compile(r'(\d{1,2})\s*%\s*OFF', re.I)
SENTENCE_SPLIT_RE = re.compile(r'\.(?=\s*[A-ZÁÉÍÓÚÑ])')

# Extracción de Datos
def extractor_mercadolibre(cantidad, buscador):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    driver.get('https://www.mercadolibre.com.co/')

    # Espera hasta que el input esté presente
    wait = WebDriverWait(driver, 10)
    search_box = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cb1-edit"]')))

    # Escribe en el campo de búsqueda
    search_box.send_keys(buscador)

    # Espera hasta que el botón esté presente y haz clic
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/header/div/div[2]/form/button')))
    search_button.click()

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ui-search-layout__item')))

    # Parsear con BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # Selecciona todos los items
    items = soup.select('li.ui-search-layout__item')

    contador = 0
    productos = []

    for item in items:
        link = item.select_one('a.poly-component__title')
        driver.switch_to.new_window()
        driver.get(link["href"])
        
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.ui-pdp-container.ui-pdp-container--pdp')))
        
        # Parsear con BeautifulSoup
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        price = soup.select_one('div.ui-pdp-price__second-line')    
        cuotas = soup.select_one('div.ui-pdp-price__subtitles')
        informacion = soup.select_one('ul.ui-vpp-highlighted-specs__features-list')
        
        m_price = PRICE_RE.search(price.text) if price else None
        m_discount = DISCOUNT_RE.search(price.text) if price else None
        m_informacion = SENTENCE_SPLIT_RE.split(informacion.text) if informacion else None
        
        producto = {
            "Nombre": link.text.strip() if link else 'No name found',
            "precio": m_price.group(0) if m_price else 'No price found',
            "descuento": m_discount.group(0) if m_discount else 'No discount found',
            "cuotas": cuotas.text if cuotas else 'No cuotas found',
            "Informacion": m_informacion if m_informacion else 'No informacion found'
        }

        productos.append(producto)

        contador += 1
        
        if contador >= cantidad:
            break

    driver.quit()
    return productos

# Datos Extraídos
productos = extractor_mercadolibre(20, 'computador')
df = pd.DataFrame(productos)

# Transformación Datos
df["Precio"] = df["precio"].str.replace(r'[^\d]', '', regex=True).astype(int)
df["Descuento (%)"] = df["descuento"].str.extract(r'(\d+)', expand=False).fillna('0').astype(int)
df["Cuotas"] = df["cuotas"].str.extract(r'(\d+)\s*cuotas', expand=False).fillna('0').astype(int)
df["Valor Cuotas"] = df["cuotas"].str.extract(r"de\s*\$([\d\.]+)", expand=False).fillna('0').str.replace(',', '.').str.replace('.', '').astype(int)
df["Interes (%)"] = df["cuotas"].str.extract(r'Interés de\s*\$?\s*([\d.,]+)', expand=False).fillna('0').str.replace('.', '').str.replace(',', '.').astype(float)
df.drop(columns=["precio", "descuento", "cuotas"], inplace=True)

# Guardar Datos Transformados
client = MongoClient("localhost:27017")

db = client["Meli"]
collection = db["coleccion"]

result = collection.insert_many(df.to_dict('records'))
print("IDs insertados:", result.inserted_ids)

## Consultas

# ¿Cuál es el producto más caro y más barato?
mas_caro = collection.find_one(sort=[("Precio", -1)], projection={"_id":0, "Nombre":1, "Precio":1, "Informacion":1})
mas_barato = collection.find_one(sort=[("Precio", 1)], projection={"_id":0, "Nombre":1, "Precio":1, "Informacion":1})

print("Más caro:", mas_caro)
print("Más barato:", mas_barato)

# ¿Cuál es el precio promedio de los productos?
precio_prom = list(collection.aggregate([
    {"$group": {"_id": None, "precio_promedio": {"$avg": "$Precio"}}}
]))
print(precio_prom)

# ¿Cuantos productos tienen descuento y cuál es el máximo descuento?
consulta = list(collection.aggregate([
    {"$match": {"Descuento (%)": {"$gt": 0}}},
    {"$group": {
        "_id": None,
        "productos_con_descuento": {"$sum": 1},
        "max_descuento": {"$max": "$Descuento (%)"}
    }}
]))
print(consulta)

# Buscar los productos con Intel o SSD dentro de la información
consulta = list(collection.find(
    {"Informacion": {"$regex": "(Intel|SSD)", "$options": "i"}},
    {"_id":0, "Nombre":1, "Precio":1, "Descuento (%)":1, "Informacion":1}
))
for prod in consulta:
    print(prod)