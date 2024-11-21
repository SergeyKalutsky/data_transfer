import re
import os
import json
import pickle
import pandas as pd
from bs4 import BeautifulSoup


def read_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        html_doc = f.read()
    return BeautifulSoup(html_doc, features="lxml")


student_id = 2
soup = read_file('cat.html')
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
    print(data)
    
# div = soup.find('div', class_='js-regular-lesson-list')
# cards = div.find_all('div', class_='crm-hover-block')
# a_tags = div.find_all('a', 'crm-ajax-link')
# i_tags = soup.find_all('small', class_='text-lowercase')
# for a, it, card in zip(a_tags, i_tags, cards):
#     subject = card.find_all('div', class_='col-xs-12 text-muted')[0].text.strip()
#     teacher = card.find_all('div', class_='col-xs-12 text-muted')[1].text.strip()
#     day = a.find('big')
#     time = a.find('small')
#     day = day.text.strip(),
#     time = ' '.join([i.strip() for i in time.text.strip().split()]),
#     period = it.text.strip().replace('                   ', '')
#     columns = ['group_id', 'day', 'time', 'period', 'subject', 'teacher']
#     data = [group_id, day[0], time[0], period, subject, teacher]
    