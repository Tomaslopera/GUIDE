from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# brew install chromium-chromedriver
# which chromedriver (/opt/homebrew/bin/chromedriver)
# ChromeDriverManager().install()
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
driver.get('https://www.mercadolibre.com.co/')


# Espera hasta que el input esté presente
wait = WebDriverWait(driver, 10)
search_box = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cb1-edit"]')))

# Escribe en el campo de búsqueda
search_box.send_keys('computador')

# Espera hasta que el botón esté presente y haz clic
search_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/header/div/div[2]/form/button')))
search_button.click()

# Espera a que carguen los resultados
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ui-search-layout__item')))

# Parsear con BeautifulSoup
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# Selecciona todos los items
items = soup.select('li.ui-search-layout__item')

for item in items:
    # Selecciona el enlace del título con clase poly-component__title
    link = item.select_one('a.poly-component__title') 
    if link:
        titulo = link.text.strip()
        href = link.get('href')
        print('Título:', titulo)
        print('URL:', href)

driver.quit()