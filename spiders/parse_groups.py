from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
from main_spider import MainSpider
import pandas as pd

class Crawler(MainSpider):
    def __init__(self) -> None:
        super().__init__()
        self.page_id = 0

    def parse_page(self, group_id):
        soup = BeautifulSoup(self.driver.page_source, features="lxml")
        self._parse_admin(soup, group_id)
        self._parse_reg_schedule(soup, group_id)
        self._parse_schedule_items(soup, group_id)
        self._parse_group_students(soup, group_id)

    def _parse_admin(self, soup, group_id):
        admin = soup.find_all(
            'div', class_='col-xs-7 text-right m-b-sm break-word')[-1]
        admin = admin.text.strip()
        cols = ['group_id', 'admin']
        data = [group_id, admin]
        self._append_csv(data, cols, 'group_admins')

    def _parse_group_students(self, soup, group_id):
        main_div = soup.find('div', class_='js-stg-items')
        student_divs = main_div.find_all('div', class_='crm-hover-block')
        for student_div in student_divs:
            a = student_div.find_all('a')[-1]
            student_href = a['href']
            student_name = a.text
            date = student_div.find_all('small')[-1].text.strip()
            data = [group_id, student_href, student_name, date]
            cols = ['group_id', 'student_href', 'student_name', 'date']
            self._append_csv(data, cols, 'group2student')

    def _parse_reg_schedule(self, soup, group_id):
        div = soup.find('div', class_='js-regular-lesson-list')
        cards = div.find_all('div', class_='crm-hover-block')
        a_tags = div.find_all('a', 'crm-ajax-link')
        i_tags = soup.find_all('small', class_='text-lowercase')
        for a, it, card in zip(a_tags, i_tags, cards):
            row = card.find_all(
                'div', class_='col-xs-12 text-muted')[0].text.strip()
            day = a.find('big')
            time = a.find('small')
            day = day.text.strip(),
            time = ' '.join([i.strip() for i in time.text.strip().split()]),
            period = it.text.strip().replace('                   ', '')
            columns = ['group_id', 'day', 'time', 'period', 'subject']
            data = [group_id, day[0], time[0], period, row]
            self._append_csv(data, columns, 'reg_schedules')

    def _parse_schedule_items(self, soup, group_id):
        div = soup.find('div', class_='visit-stats')
        spans = div.find_all('span')
        for span in spans:
            # if 'data-date'
            date = span.get('data-date')
            schedule_item_id = span.get('data-id')
            ru_date = span.find('small').text
            if ru_date:
                cols = ['group_id', 'date', 'schedule_item_id', 'ru_date']
                data = [group_id, date, schedule_item_id, ru_date]
                self._append_csv(data, cols, 'schedule_items')

    def crawl(self) -> None:
        groups = pd.read_csv('cleaned/groups.csv')
        for _, row in groups.iterrows():
            group_id = row['group_id']
            city_id = row['city_id']
            url = f'https://algoritmikakz.s20.online/company/{city_id}/group/view?id={group_id}'
            self.driver.get(url)
            sleep(3)
            self.driver.find_element(by=By.XPATH, value="//small[@class='pull-right']").click()
            sleep(0.5)
            self.driver.find_element(by=By.XPATH, value="//li//a[contains(text(), '±360 дней')]").click()
            sleep(5)
            if self._check_exists_by_xpath("//a[contains(text(), 'Архивное рег. расписание')]"):
                self.driver.find_element('xpath', "//a[contains(text(), 'Архивное рег. расписание')]").click()
                sleep(1)
            if self._check_exists_by_xpath("//a[contains(text(), 'Архивные участники')]"):
                self.driver.find_element('xpath', "//a[contains(text(), 'Архивные участники')]").click()
                sleep(1)
            self.parse_page(group_id)


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
