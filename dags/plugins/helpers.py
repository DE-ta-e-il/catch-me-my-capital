# TODO: can chrome.service use webdriver_manager's ChromeDriverManager().install()?

import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service


def rename_downloaded(path, filename):
    """
    c_ prefix and the '.csv' will be added to the filename
    """
    files = os.listdir(path)
    f_path = ""
    target_path = ""
    for f in files:
        if f[-3:] == "csv" and f[0] != "c":
            f_path = os.path.join(path, f)
            target_path = os.path.join(path, f"c_{filename}.csv")
            os.rename(f_path, target_path)
            break
    return target_path


def get_chrome_driver(
    path, driver_path="/opt/airflow/plugins/chromedriver-linux64/chromedriver"
):
    chrome_prefs = {
        "download.default_directory": path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", chrome_prefs)
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
    options.add_argument("--no-sandbox")  # Required for running as root in containers
    options.add_argument("--disable-gpu")  # Disable GPU (useful
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )  # Mimic real browser
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    return driver
