from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager


service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
driver.get('https://www.amazon.com/-/es/ref=nav_logo')


# Espera hasta que el input esté presente
wait = WebDriverWait(driver, 10)
search_box = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="twotabsearchtextbox"]')))

# Escribe en el campo de búsqueda
search_box.send_keys('macbook pro')
search_box.send_keys(Keys.ENTER)

# Espera a que carguen los resultados
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.puisg-col-inner')))

# Parsear con BeautifulSoup
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# Selecciona todos los items
items = soup.select('div.puisg-col-inner')

for item in items:
    # Selecciona el enlace del título con clase poly-component__title
    links = soup.select('a.a-link-normal[href] > h2[aria-label]')

    for h2 in links:
        titulo = h2['aria-label']
        href = h2.find_parent('a')['href']

        # Completar URL si es relativa
        if href.startswith('/'):
            href = 'https://www.amazon.com' + href

        print('Título:', titulo)
        print('URL:', href)

driver.quit()