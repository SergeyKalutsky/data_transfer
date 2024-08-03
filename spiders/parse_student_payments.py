from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
from main_spider import MainSpider
import pandas as pd


class Crawler(MainSpider):
    def __init__(self) -> None:
        super().__init__()

    def _parse_page(self, soup, city_id, student_id):
        tbody = soup.find('table').tbody
        for tr in tbody.find_all('tr'):
            if not tr.find('td', {'data-col-seq': 1}):
                continue
            date = tr.find('td', {'data-col-seq': 1}).text
            amount = tr.find('td', {'data-col-seq': 3}).text
            terminal = tr.find('td', {'data-col-seq': 4}).text
            cols = ['city_id', 'student_id', 'date', 'amount',  'terminal']
            data = [city_id, student_id, date, amount, terminal]
            self._append_csv(data, cols, 'student_payments')

    def crawl(self) -> None:
        clients = pd.read_csv('cleaned/clients.csv')
        for _, row in clients.iterrows():
            student_id = row['student_id']
            for city_id in [1, 10, 7]:
                url = f'https://algoritmikakz.s20.online/company/{city_id}/pay/index?PaySearch%5Bf_customer_id%5D={student_id}'
                print(url)
                self.driver.get(url)
                sleep(2)
                soup = BeautifulSoup(self.driver.page_source, features="lxml")
                self._parse_page(soup, city_id, row['student_id'])


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
