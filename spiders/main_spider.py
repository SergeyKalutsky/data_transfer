import os
import re
import csv
from time import sleep
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium import webdriver

class MainSpider():
    def __init__(self) -> None:
        self.driver = self._open_driver(
            headless=False, disable_automation=True)
        self.output_dir = os.getcwd()
        self._prepare_crawl()
        
    def _prepare_crawl(self) -> None:
        self.url = 'https://algoritmikakz.s20.online/'
        self.driver.get(self.url)
        sleep(4)

        self._process_login()

    def _process_login(self) -> None:
        self.login = 'almaty@algoritmika.club'
        self.password = '1Qaz2Wsxqq'

        self.driver.find_element(
            by=By.XPATH, value='//input[@id="loginform-username"]').send_keys(self.login)
        self.driver.find_element(
            by=By.XPATH, value='//input[@id="loginform-password"]').send_keys(self.password)
        self.driver.find_element(
            by=By.XPATH, value='//button[@type="submit"]').click()
        sleep(5)

    def _open_driver(self, headless: bool, disable_automation: bool) -> webdriver:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        options.add_argument('--disable-dev-shm-usage')

        if headless:
            options.add_argument('--headless')

        if disable_automation:
            options.add_argument(
                '--disable-blink-features=AutomationControlled')
            options.add_experimental_option(
                'excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=options)
        return driver

    def _append_csv(self, data, columns, f_name, remove_nonascii=False, verbose=True):
        filename = os.path.join(self.output_dir, f_name) + '.csv'
        if not os.path.exists(filename):
            with open(filename, 'a', encoding='utf-8') as f:
                csv.writer(f).writerow(columns)
        if remove_nonascii:
            data = [re.sub(r'[^\x00-\x7f]', r'', item) for item in data]
        with open(filename, 'a', encoding='utf-8') as f:
            csv.writer(f).writerow(data)
        if verbose:
            print(data)

    def _check_exists_by_xpath(self, xpath: str) -> bool:
        try:
            self.driver.find_element(by=By.XPATH, value=xpath)
        except NoSuchElementException:
            return False
        return True
