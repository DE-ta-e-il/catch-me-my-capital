import os
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

options = webdriver.ChromeOptions()
download_dir = os.path.abspath("./downloads")
chrome_prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
}

options.add_experimental_option("prefs", chrome_prefs)
options.add_argument("--headless")
options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
options.add_argument("--no-sandbox")  # Required for running as root in containers
options.add_argument("--disable-gpu")  # Disable GPU (useful
options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
)  # Mimic real browser

driver = webdriver.Chrome(options=options)

driver.get(
    "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506"
)

download_button = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located(
        (By.XPATH, '//*[@id="UNIT-WRAP0"]/div/p[2]/button[2]')
    )
)

download_button.click()

time.sleep(3)

input_field = driver.find_element(By.ID, "trdDd")

input_field.clear()
input_field.send_keys("20240103")


search_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, '//*[@id="jsSearchButton"]'))
)

search_button.click()


time.sleep(3)
download_button = driver.find_element(
    By.XPATH, '//*[@id="UNIT-WRAP0"]/div/p[2]/button[2]'
)
download_button.click()
