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

    def parse_page(self, student_id):
        soup = BeautifulSoup(self.driver.page_source, features="lxml")
        self._parse_admin_indiv(soup, student_id)
        self._parse_indiv_schedule_items(soup, student_id)
        self._parse_reg_schedule(soup, student_id)
        self._parse_parent(soup, student_id)

    def _parse_admin_indiv(self, soup, student_id):
        admin = soup.find_all('a', class_='chosen-single')[-1]
        admin = admin.text.strip()
        cols = ['student_id', 'admin']
        data = [student_id, admin]
        self._append_csv(data, cols, 'student_admins')

    def _parse_indiv_schedule_items(self, soup, student_id):
        div = soup.find('div', class_='sponge-bob')
        divs = div.find_all('div')
        for div in divs:
            attendance = ''
            if 'done_paid' in div['class']:
                attendance = 'attendend'
            if 'absence_paid' in div['class']:
                attendance = 'absent'
            if 'done_free' in div['class']:
                attendance = 'repeat'
            if 'planned_unpaid' in div['class'] or 'planned_paid' in div['class']:
                attendance = 'planned'
            schedule_item_id = div.get('data-id')
            ru_date = div.get('data-date')
            comission = div.get('data-c')
            date = div.text
            if ru_date:
                cols = ['student_id', 'date', 'schedule_item_id',
                        'ru_date', 'comission', 'attendance']
                data = [student_id, date, schedule_item_id,
                        ru_date, comission, attendance]
                self._append_csv(data, cols, 'schedule_items_indiv')

    def _parse_reg_schedule(self, soup, student_id):
        div = soup.find('div', class_='js-regular-lesson-list')
        cards = div.find_all('div', class_='crm-hover-block')
        a_tags = div.find_all('a', class_='crm-ajax-link')
        a_tags = [a_tag for a_tag in a_tags if 'update' in a_tag['href']]
        i_tags = div.find_all('small', class_='text-lowercase')
        for a, it, card in zip(a_tags, i_tags, cards):
            subject = card.find_all('div', class_='col-xs-12 text-muted')[0].text.strip()
            teacher = card.find_all('div', class_='col-xs-12 text-muted')[1].text.strip()
            day = a.find('big')
            time = a.find('small')
            day = day.text.strip(),
            time = ' '.join([i.strip() for i in time.text.strip().split()]),
            period = it.text.strip().replace('                   ', '')
            columns = ['student_id', 'day', 'time', 'period', 'subject', 'teacher']
            data = [student_id, day[0], time[0], period, subject, teacher]
            self._append_csv(data, columns, 'reg_schedule_items_indiv')


    def _parse_parent(self, soup, student_id):
        span = soup.find('span', {'title': 'Тип клиента - Физ.лицо'})
        tel_link = soup.find('a', href=lambda href: href and 'tel:+' in href)
        if tel_link:
            tel_link = tel_link.text
        if span:
            span = span.parent.text.split('Физ.лицо')[-1].strip()
        columns = ['student_id', 'parent_name', 'parent_phone']
        data = [student_id, span, tel_link]
        self._append_csv(data, columns, 'student_parent')


    def crawl(self) -> None:
        clients = pd.read_csv('cleaned/clients.csv')
        clients_parsed = pd.read_csv('student_admins.csv')
        for _, row in clients.iterrows():
            city_id = row['city_id']
            student_id = row['student_id']
            if clients_parsed[clients_parsed['student_id'] == student_id].shape[0] > 0:
                continue
            url = f'https://algoritmikakz.s20.online/company/{city_id}/customer/view?id={student_id}'
            # url = 'https://algoritmikakz.s20.online/company/1/customer/view?id=9515'
            print(url)
            self.driver.get(url)
            self.driver.find_element(
                by=By.XPATH, value="//span[@class='text-muted sponge-bob-dd-limited']").click()
            sleep(0.5)
            self.driver.find_element(
                by=By.XPATH, value="//li//a[contains(text(), '±360 дней')]").click()
            sleep(5)
            if self._check_exists_by_xpath("//a[contains(text(), 'Архивное рег. расписание')]"):
                self.driver.find_element(
                    'xpath', "//a[contains(text(), 'Архивное рег. расписание')]").click()
                sleep(0.5)
            self.parse_page(row['student_id'])


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()
