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
main_div = soup.find('div', class_='js-stg-items')
student_divs = main_div.find_all('div', class_='crm-hover-block')
for student_div in student_divs:
    a = student_div.find_all('a')[-1]
    student_href = a['href']
    student_name = a.text
    date = student_div.find_all('small')[-1].text.strip()
    print([student_href, student_name, date])