from bs4 import BeautifulSoup
from time import sleep
from main_spider import MainSpider


class Crawler(MainSpider):
    def __init__(self) -> None:
        super().__init__()

    def _parse_clients(self, city_id):
        soup = BeautifulSoup(self.driver.page_source, features="lxml")
        tbody = soup.find('table').tbody
        for tr in tbody.find_all('tr'):
            id_ = tr.find('td', {'data-col-seq': 1}).text
            name = tr.find('td', {'data-col-seq': 3}).text
            gender = tr.find('td', {'data-col-seq': 4}).text
            bo_link = tr.find('td', {'data-col-seq': 5}).text
            columns = ['city_id', 'student_id', 'name', 'admin', 'bo_link']
            data = [city_id, id_, name, gender, bo_link]
            self._append_csv(data, columns, 'students')

    def _parse_groups(self, city_id):
        soup = BeautifulSoup(self.driver.page_source, features="lxml")
        tbody = soup.find('table').tbody
        for tr in tbody.find_all('tr'):
            id_ = tr.find('td', {'data-col-seq': 1}).text
            name = tr.find('td', {'data-col-seq': 2}).text
            date_start = tr.find('td', {'data-col-seq': 3}).text
            admin = tr.find('td', {'data-col-seq': 4}).text
            bo_link = tr.find('td', {'data-col-seq': 5}).text
            columns = ['city_id', 'group_id', 'name',  'date_start', 'admin', 'bo_link']
            data = [city_id, id_, name, date_start, admin, bo_link]
            self._append_csv(data, columns, 'groups')
            
    def _parse_teachers(self, city_id):
        soup = BeautifulSoup(self.driver.page_source, features="lxml")
        tbody = soup.find('table').tbody
        for tr in tbody.find_all('tr'):
            id_ = tr.find('td', {'data-col-seq': 1}).text
            name = tr.find('td', {'data-col-seq': 2}).text
            gender = tr.find('td', {'data-col-seq': 3}).text
            contacts = tr.find('td', {'data-col-seq': 4}).text
            cities = tr.find('td', {'data-col-seq': 5}).text
            columns = ['city_id', 'id_', 'name', 'gender',  'contacts', 'cities']
            data = [city_id, id_, name, gender, contacts, cities]
            self._append_csv(data, columns, 'teachers')

    def crawl_clients(self):
        for city_id in range(9, 13):
            page = 1
            while True:
                url = f'https://algoritmikakz.s20.online/company/{city_id}/customer/index?page={page}'
                self.driver.get(url)
                self._parse_clients(city_id)
                page += 1
                sleep(2)
                if self._check_exists_by_xpath("//li[@class='next disabled']"):
                    break
            sleep(3)

    def crawl_groups(self):
        for city_id in range(1, 13):
            page = 1
            while True:
                url = f'https://algoritmikakz.s20.online/company/{city_id}/group/index?page={page}'
                self.driver.get(url)
                self._parse_groups(city_id)
                page += 1
                if input('continue(y/n)') == 'n':
                    break
            sleep(3)
    
    def crawl_teachers(self):
        for city_id in range(1, 13):
            page = 1
            while True:
                url = f'https://algoritmikakz.s20.online/company/{city_id}/employee/index?page={page}'
                self.driver.get(url)
                self._parse_teachers(city_id)
                page += 1
                if input('continue(y/n)') == 'n':
                    break
            sleep(3)


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl_teachers()
