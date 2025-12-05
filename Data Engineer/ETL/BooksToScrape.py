from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re
import hashlib
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, engine
import sqlalchemy.dialects.postgresql as pg


def extract_book_details():
    categorias = ["travel_2", "mystery_3", "historical-fiction_4", "sequential-art_5", "classics_6", "philosophy_7", "romance_8", "womens-fiction_9", "fiction_10", "childrens_11", "religion_12", "nonfiction_13", "music_14", "default_15", "science-fiction_16", "sports-and-games_17", "add-a-comment_18", "fantasy_19", "new-adult_20", "young-adult_21", "science_22", "poetry_23", "paranormal_24", "art_25", "psychology_26", "autobiography_27", "parenting_28", "adult-fiction_29", "humor_30", "horror_31", "history_32", "food-and-drink_33", "christian-fiction_34", "business_35", "biography_36", "thriller_37", "contemporary_38", "spirituality_39", "academic_40", "self-help_41", "historical_42", "christian_43", "suspense_44", "short-stories_45", "novels_46", "health_47", "politics_48", "cultural_49", "erotica_50", "crime_51"]
    results = []

    for i in range(len(categorias)):
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)

        try:
            base_cat = f'https://books.toscrape.com/catalogue/category/books/{categorias[i]}/index.html'
            driver.get(base_cat)
            wait = WebDriverWait(driver, 10)

            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.page-header.action')))
            categoria = driver.find_element(By.CSS_SELECTOR, 'div.page-header.action').text.strip()

            category_books = []

            while True:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#default section div ol')))
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                items = soup.select('li.col-xs-6.col-sm-4.col-md-3.col-lg-3')

                for item in items:
                    a = item.select_one('article.product_pod h3 a') or item.select_one('.image_container a')
                    book_url = urljoin(driver.current_url, a.get('href'))
                    driver.switch_to.new_window('tab')
                    driver.get(book_url)
                    
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.product_main')))
                    book_soup = BeautifulSoup(driver.page_source, 'html.parser')

                    title = book_soup.select_one('div.product_main h1').get_text(strip=True)
                    price = book_soup.select_one('div.product_main p.price_color').get_text(strip=True)
                    availability = book_soup.select_one('div.product_main p.instock.availability').get_text(" ", strip=True)
                    
                    rating_p = book_soup.select_one("p.star-rating")
                    rating_word = None
                    if rating_p:
                        classes = rating_p.get("class", [])
                        for w in ["One", "Two", "Three", "Four", "Five"]:
                            if w in classes:
                                rating_word = w
                                break
                    
                    desc_div = book_soup.select_one('#product_description')
                    if desc_div:
                        desc_p = desc_div.find_next_sibling('p')
                        description = desc_p.get_text(strip=True) if desc_p else None
                    else:
                        description = None

                    info = {}
                    table = book_soup.select_one('table.table.table-striped')
                    if table:
                        for row in table.select('tr'):
                            th = row.find('th').get_text(strip=True)
                            td = row.find('td').get_text(strip=True)
                            info[th] = td

                    category_books.append({
                        "titulo": title,
                        "precio": price,
                        "disponibilidad": availability,
                        "url": book_url,
                        "descripcion": description,
                        "info": info,
                        "rating_stars": rating_word
                    })

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

                next_link = soup.select_one("li.next a")
                if next_link:
                    next_url = urljoin(driver.current_url, next_link["href"])
                    driver.get(next_url)
                else:
                    break
                
            results.append({"categoria": categoria, "libros": category_books})

        finally:
            driver.quit()

    return results

def transform_data(results):
    libros = []
    for cat in results:
        categoria = cat["categoria"]
        for b in cat["libros"]:
            info = b.get("info") or {}
            libros.append({
                "category": categoria,
                "title": b.get("titulo"),
                "price": b.get("precio"),                   
                "disponibility": b.get("disponibilidad"),
                "url": b.get("url"),
                "description": b.get("descripcion"),
                "calification": b.get("rating_stars"),
                "upc": info.get("UPC"),
                "product_type": info.get("Product Type"),
                "price_excl_tax": info.get("Price (excl. tax)"),
                "price_incl_tax": info.get("Price (incl. tax)"),
                "tax": info.get("Tax"),
                "availability_info": info.get("Availability"),
                "n_reviews": info.get("Number of reviews"),
            })

    df = pd.DataFrame(libros)
    df = df.dropna()
    
    df_categorias = df[["category"]].drop_duplicates().reset_index(drop=True)
    df_categorias["category_id"], _ = pd.factorize(df_categorias["category"], sort=False)
    df_categorias = df_categorias[["category_id", "category"]]
    
    df_libros = df.copy()
    df_libros["book_id"], _ = pd.factorize(df_libros["upc"], sort=False)
    df_libros["category_id"] = df_libros["category"].map(df_categorias.set_index("category")["category_id"])
    df_libros = df_libros.drop(columns=["category", "price", "disponibility", "price_excl_tax", "price_incl_tax", "tax", "availability_info", "n_reviews"])
    df_libros = df_libros[["book_id", "category_id", "title", "upc", "product_type", "url", "description"]]
    
    df_info_libros = df[["upc", "price", "disponibility", "price_excl_tax", "price_incl_tax", "tax", "availability_info", "n_reviews", "calification"]].copy()
    df_info_libros["book_id"], _ = pd.factorize(df_libros["upc"], sort=False)
    df_info_libros["price"] = df_info_libros["price"].str.replace("£", "").astype(float)
    df_info_libros["stock"] = df_info_libros["disponibility"].str.extract(r'(\d+)').astype(float).fillna(0).astype(int)
    df_info_libros["disponibility"] = np.where(df_info_libros["stock"] > 0, True, False)
    df_info_libros["price_excl_tax"] = df_info_libros["price_excl_tax"].str.replace("£", "").apply(float)
    df_info_libros["price_incl_tax"] = df_info_libros["price_incl_tax"].str.replace("£", "").astype(float)
    df_info_libros["tax"] = df_info_libros["tax"].str.replace("£", "").astype(float)
    df_info_libros["n_reviews"] = df_info_libros["n_reviews"].astype(int)
    df_info_libros["calification"] = df_info_libros["calification"].map({"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5})

    df_price_libros = df_info_libros[["book_id", "price", "price_excl_tax", "price_incl_tax", "tax"]]
    df_availability_libros = df_info_libros[["book_id", "disponibility", "stock", "availability_info"]]
    df_reviews_libros = df_info_libros[["book_id", "calification", "n_reviews"]]
    
def load_data(df_categorias, df_libros, df_price_libros, df_availability_libros, df_reviews_libros):
    engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/Books_Norm", future=True)
    
    categories_dtype = {
    "category_id": pg.INTEGER(),
    "category": pg.TEXT(),
    }
    book_dtype = {
        "book_id": pg.INTEGER(),
        "category_id": pg.INTEGER(),
        "title": pg.TEXT(),
        "upc": pg.TEXT(),
        "product_type": pg.TEXT(),
        "url": pg.TEXT(),
        "description": pg.TEXT(),
    }
    price_dtype = {
        "book_id": pg.INTEGER(),
        "price": pg.REAL,
        "price_excl_tax": pg.REAL,
        "price_incl_tax": pg.REAL,
        "tax": pg.REAL,
    }
    disp_dtype = {
        "book_id": pg.INTEGER(),
        "disponibility": pg.BOOLEAN(),
        "stock": pg.INTEGER(),
        "availability_info": pg.TEXT(),
    }
    reviews_dtype = {
        "book_id": pg.INTEGER(),
        "calification": pg.INTEGER(),
        "n_reviews": pg.INTEGER(),
    }
    
    with engine.begin() as conn:
        df_categorias.to_sql("categories", conn, if_exists="append", index=False, dtype=categories_dtype)
        df_libros.to_sql("book", conn, if_exists="append", index=False, dtype=book_dtype)
        df_price_libros.to_sql("book_price", conn, if_exists="append", index=False, dtype=price_dtype)
        df_availability_libros.to_sql("book_availability", conn, if_exists="append", index=False, dtype=disp_dtype)
        df_reviews_libros.to_sql("book_review", conn, if_exists="append", index=False, dtype=reviews_dtype)
        conn.commit()
        conn.close()


data = extract_book_details()
df_categorias, df_libros, df_price_libros, df_availability_libros, df_reviews_libros = transform_data(data)
load_data(df_categorias, df_libros, df_price_libros, df_availability_libros, df_reviews_libros)