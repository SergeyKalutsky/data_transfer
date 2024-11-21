import re
import pyodbc
import recs as r
from time import sleep
from tqdm import tqdm
from helpers import load_map, save_map, calculate_age
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_schedule_item_dates(date, duration, hour, minute):

    # Step 2: Set the hour and minute to 12:00
    date_with_time = date + np.timedelta64(hour, 'h') + np.timedelta64(minute, 'm')

    # Step 3: Convert to string format '2023-10-15 12:00:00'
    date_str = date_with_time.astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%d %H:%M:%S')
    # Step 4: Add duration of 90 minutes
    duration = timedelta(minutes=duration)
    new_date = date_with_time.astype('datetime64[s]').astype(datetime) + duration
    # Step 5: Convert the resulting datetime to string format '2023-10-15 13:30:00'
    new_date_str = new_date.strftime('%Y-%m-%d %H:%M:%S')
    return date_str, new_date_str


cnxn = pyodbc.connect(
    Trusted_Connection='Yes',
    Driver='{ODBC Driver 11 for SQL Server}',
    Server='localhost,1433',
    Database='itm'
)


cities = {
    'Алматы': 1,
    'Астана': 2,
    'Караганда': 3,
    'Павлодар': 4,
    'Семей': 9,
    'Усть-Каменогорск': 5,
    'Шымкент': 6,
    'Актау': 11,
    'Актобе': 8,
    'Петропавловск': 10,
    'Онлайн': 7,
    'Талдыкорган': 12
}


def populate_cities(kz_id):
    print('Заполняем города')
    cities_map = {}
    res = cnxn.execute(f'SELECT HierarchyId FROM Country WHERE Id = {kz_id}')
    hierarchy_id = res.fetchone()[0]
    for branch in cities:
        print(f'Добавляем город {branch}')
        cnxn.execute(f'''INSERT INTO LocationHierarchy (ParentId, LocationType)
                    VALUES ({hierarchy_id}, 2)''')
        new_hierarchy_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(f'''INSERT INTO City (Name, CountryId, HierarchyId) 
                    VALUES ('{branch}', {kz_id}, {new_hierarchy_id})''')
        city_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        cities_map[cities[branch]] = int(city_id)
    cnxn.commit()
    save_map(cities_map, 'cities')


def populate_venues(branches):
    cities_map = load_map('cities')
    venues_map = {}
    for branch in branches['items']:
        print(f'Заполняем локации для {branch["name"]}')
        city_id = cities_map[branch['id']]
        locations = r.get_locations(branch_id=branch['id'])
        for location in locations['items']:
            name = location['name']
            hierarchy_id = cnxn.execute(
                f'SELECT HierarchyId FROM City WHERE Id = {city_id}').fetchone()[0]
            cnxn.execute(f'''INSERT INTO LocationHierarchy (ParentId, LocationType)
                            VALUES ({hierarchy_id}, 3)''')
            new_hierarchy_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
            cnxn.execute(f'''INSERT INTO Venue (Name, ShortName, AddressString, CityId, HierarchyId)
                            VALUES ('{name}','{name}','{name}', {city_id}, {new_hierarchy_id})''')
            venue_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
            venues_map[location['id']] = int(venue_id)
        sleep(10)
    cnxn.commit()
    save_map(venues_map, 'venues')


def populate_teachers():
    cities_map = load_map('cities')
    teacher_map = {}
    teachers = pd.read_csv('cleaned/teachers.csv')
    teachers = teachers.drop_duplicates(subset=['name']).fillna('')
    for _, row in tqdm(teachers.iterrows()):
        common_name = row['name']
        name = common_name.split(' ')
        middle_name = ''
        if len(name) == 2:
            last_name, first_name = name
        if len(name) == 3:
            last_name, first_name, middle_name = name
        if row['gender'] == 'Мужчина':
            gender = 1
        elif row['gender'] == 'Женщина':
            gender = 2
        else:
            gender = 0
        email = row['email'] if row['email'] else '-'
        phone = row['phone'] if row['phone'] else 'null'
        if phone:
            phone = phone.replace(
                '+', '').replace('(', '').replace(')', '').replace('-', '')
        teacher_cities = row['cities'].split(', ')    
        create_date = datetime.now().date()
        cnxn.execute(f'''INSERT INTO Person 
                    (LastName, FirstName, MiddleName, eMail, Mobile, CreateDate, CommonName, CreateType, Gender)
                    VALUES ('{last_name}', '{first_name}', '{middle_name}', '{email}', {phone}, '{create_date}', '{common_name}', 5, {gender})
                    ''')
        person_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(
            f'INSERT INTO Employee (PersonId, EmployeeStatus) VALUES ({person_id}, 0)')
        employee_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(
            f"INSERT INTO EmployeeTeacher (EmployeeId, JobStatus, StartDate) VALUES ({employee_id}, 0, '{create_date}')")
        teacher_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        for teacher_city in teacher_cities:
            new_city_id = cities_map[cities[teacher_city]]
            cnxn.execute(
                f'INSERT INTO Employee2City (EmployeeId, CityId) VALUES ({employee_id}, {new_city_id})')
        teacher_map[row['id_']] = [int(teacher_id), int(employee_id)]
        cnxn.commit()
        save_map(teacher_map, 'teachers')



def populate_subjects():
    subjects = r.get_subjects(1, 1)
    subject_map = {}
    for subject in subjects['items']:
        cnxn.execute(f'''INSERT INTO Course (Title, StartDate, LessonCount, Status)
                    VALUES ('{subject["name"]}', '{datetime.now().date()}', 0, 1) ''')
        course_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        subject_map[subject['id']] = int(course_id)
    cnxn.commit()
    save_map(subject_map, 'subjects')


def populate_students():
    cities_map = load_map('cities')
    student_map = {}
    students = pd.read_csv('cleaned/clients.csv')
    students = students.drop_duplicates(subset='student_id').fillna('')
    parents = pd.read_csv('students_data/student_parent.csv')
    parents = parents.fillna('')
    for _, row in students.iterrows():
        common_name = row['name']
        parent_name = parents[parents['student_id'] == row['student_id']]['parent_name'].values[0]
        phone = parents[parents['student_id'] == row['student_id']]['parent_phone'].values[0]
        phone = phone if phone else 'null'
        dob = row['brirth']
        create_date = datetime.now().date()
        if phone != 'null':
            phone = ''.join(re.findall(r'\d+', phone))
        if row['gender'] == 'Мужчина':
            gender = 1
        elif row['gender'] == 'Женщина':
            gender = 2
        else:
            gender = 0
        sql = f'''INSERT INTO Person (CreateDate, CommonName, CreateType, Gender, BirthDate)
                    VALUES ('{create_date}', '{common_name}', 5, {gender}'''
        age = None
        if dob:
            d, m, y = dob.split('.')
            born = datetime.strptime(dob, '%d.%m.%Y')
            age = calculate_age(born)
            dob = f'{y}-{m}-{d}'
            sql += f", '{dob}') "
        else:
            sql += ', null)'
        cnxn.execute(sql)
        student_person_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(f'''INSERT INTO Person (CreateDate, CommonName, CreateType, Mobile)
                    VALUES ('{create_date}', '{parent_name}', 5, {phone})''')
        parent_person_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(
            f'''INSERT INTO Parent (PersonId) VALUES ({parent_person_id})''')
        parent_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        if age:
            cnxn.execute(f'''INSERT INTO Student (Age, AgeSetDate, PersonId) 
                        VALUES ({age}, '{datetime.now().date()}', {student_person_id})''')
            student_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
        else:
            cnxn.execute(
                f'''INSERT INTO Student (PersonId) VALUES ({student_person_id})''')
            student_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
        city_id = cities_map[row['city_id']]
        cnxn.execute(
            f'''INSERT INTO Student2City (StudentId, CityId) VALUES ({student_id}, {city_id})''')
        cnxn.execute(f'''INSERT INTO Student2Parent (StudentId, Parentid, CreateType) 
                        VALUES ({student_id}, {parent_id}, 5)''')
        student_map[row['student_id']] = int(student_id)
    cnxn.commit()
    save_map(student_map, 'students')


def populate_groups():
    group_map = {}
    cities_map = load_map('cities')
    groups = pd.read_csv('cleaned/groups.csv').fillna('')
    for _, row in groups.iterrows():
        teacher_name = row['teacher']
        admin_name = row['admin']
        teacher_id = cnxn.execute(f'''select et.id from EmployeeTeacher et 
                                join Employee e on e.id = et.EmployeeId 
                                join Person p on p.id = e.PersonId 
                                where p.CommonName = '{teacher_name}' ''').fetchone()
        if not teacher_id:
            continue
        course_id = cnxn.execute(f'''select id from Course c where Title = '{row["subject"]}' ''').fetchone()[0]
        teacher_id = teacher_id[0]
        group_name = row['name']
        bo_id = row['bo_link'].split('/')[-1].split('#')[0] if row['bo_link'] else 'null'
        d, m, y = row['date_start'].split('.')
        start_date = f'{y}-{m}-{d}'
        city_id = cities_map[row['city_id']]
        admin_id = cnxn.execute(f'''select et.id from EmployeeAdmin et 
                                join Employee e on e.id = et.EmployeeId 
                                join Person p on p.id = e.PersonId 
                                where p.CommonName = '{admin_name}' ''').fetchone()
        if admin_id:
            admin_id = admin_id[0]
        else: 
            middle_name = ''
            if len(admin_name.split()) == 2:
                last_name, first_name = admin_name.split()
            if len(admin_name.split()) == 3:
                last_name, first_name, middle_name = admin_name.split()
            create_date = datetime.now().date()
            cnxn.execute(f'''INSERT INTO Person 
                        (LastName, FirstName, MiddleName,  CreateDate, CommonName, CreateType, Gender)
                        VALUES ('{last_name}', '{first_name}', '{middle_name}', '{create_date}', '{admin_name}', 5, 2)
                        ''')
            person_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
            cnxn.execute(
                f'INSERT INTO Employee (PersonId, EmployeeStatus) VALUES ({person_id}, 0)')
            employee_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
            cnxn.execute(
                f"INSERT INTO EmployeeAdmin (EmployeeId, JobStatus, StartDate) VALUES ({employee_id}, 0, '{create_date}')")
            admin_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
        if 'meet.jit.si' not in row['bo_link']:
            cnxn.execute(f'''INSERT INTO [Group] (boId, Name, DisplayName, CreateDate, TeacherId, GroupStatus, CityId, StartDate, AdminId, CourseId, LessonDuration) 
                            VALUES ({bo_id}, '{group_name}', '{group_name}', '{datetime.now().date()}', {teacher_id}, 1, {city_id}, '{start_date}', {admin_id}, {course_id}, 90)''')
        else:
            cnxn.execute(f'''INSERT INTO [Group] (Name, DisplayName, CreateDate, TeacherId, GroupStatus, CityId, StartDate, AdminId, CourseId, LessonDuration) 
                                    VALUES ('{group_name}', '{group_name}', '{datetime.now().date()}', {teacher_id}, 1, {city_id}, '{start_date}', {admin_id}, {course_id}, 90)''')
        group_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        group_map[row['group_id']] = int(group_id)
    cnxn.commit()
    save_map(group_map, 'groups')

def students_to_group():
    student_map = load_map('students')
    group_map = load_map('groups')
    st2group = pd.read_csv('groups_data/group2student.csv').fillna('')
    for _, row in st2group.iterrows():
        if row['group_id'] not in group_map:
            continue
        back_group_id = group_map[row['group_id']]
        if row['student_id'] not in student_map:
            continue
        student_id = student_map[row['student_id']]
        join_date = row['date_add']
        d, m, y = join_date.split('.')
        join_date = f'{y}-{m}-{d}'
        leave_date = row['date_remove']
        if leave_date:
            d, m, y = leave_date.split('.')
            leave_date = f'{y}-{m}-{d}'
            cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate, LeaveDate, GroupLeaveReason)
                        VALUES ({back_group_id}, {student_id}, '{join_date}', '{leave_date}', 2)''')
        else:
            cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate)
                        VALUES ({back_group_id}, {student_id}, '{join_date}')''')
    cnxn.commit()

def populate_schedules():
    groups_map = load_map('groups')
    schedules = pd.read_csv('groups_data/reg_schedules.csv')
    for _, row in tqdm(schedules.iterrows()):
        if row['group_id'] not in groups_map:
            continue
        group_id = groups_map[row['group_id']]
        teacher_id = cnxn.execute(f'''Select TeacherId From [Group] Where Id = {group_id}''').fetchone()[0]
        admin_id = cnxn.execute(f'''Select AdminId From [Group] Where Id = {group_id}''').fetchone()[0]
        start_date = row['start_date']
        d, m, y = start_date.split('.')
        start_date = f'20{y}-{m}-{d}'
        end_date = row['end_date']
        d, m, y = end_date.split('.')
        end_date = f'20{y}-{m}-{d}'
        duration = row['duration']
        hour = row['start_hour']
        minute = row['start_minute']
        week_day = row['week_day']
        cnxn.execute(f'''INSERT INTO Schedule (GroupId, TeacherId, AdminId, Minute, Hour, WeekDay, Duration, StartDate, EndDate, Status)
                    VALUES ({group_id}, {teacher_id}, {admin_id}, {minute}, {hour}, {week_day}, {duration}, '{start_date}', '{end_date}', 0)''')
    cnxn.commit()


def populate_indiv_groups():
    cities_map = load_map('cities')
    students_map = load_map('students')
    schedules = pd.read_csv('students_data/reg_schedule_items_indiv.csv')
    schedules = schedules[schedules['group_type'] == '(Индивидуальный)']
    for _, row in schedules[['student_id', 'subject']].drop_duplicates().iterrows():
        student_id = row['student_id']
        subject = row['subject']
        temp = schedules[(schedules['student_id'] == student_id) & (schedules['subject'] == subject)]
        start_date = str(pd.to_datetime(temp['start_date'], format='%d.%m.%y').min()).split()[0]
        student_id = students_map[temp['student_id'].values[0]]
        city_id = cities_map[temp['city_id'].values[0]]
        teacher_name = temp['teacher'].values[0]
        admin_name = temp['admin'].values[0]
        teacher_id = cnxn.execute(f'''select et.id from EmployeeTeacher et 
                                    join Employee e on e.id = et.EmployeeId 
                                    join Person p on p.id = e.PersonId 
                                    where p.CommonName = '{teacher_name}' ''').fetchone()
        if not teacher_id:
            continue
        teacher_id = teacher_id[0]
        admin_id = cnxn.execute(f'''select et.id from EmployeeAdmin et 
                                    join Employee e on e.id = et.EmployeeId 
                                    join Person p on p.id = e.PersonId 
                                    where p.CommonName = '{admin_name}' ''').fetchone()
        course_id = cnxn.execute(f'''select id from Course c where Title = '{row["subject"]}' ''').fetchone()[0]
        if admin_id:
            admin_id = admin_id[0]
        else: 
            middle_name = ''
            if len(admin_name.split()) == 2:
                last_name, first_name = admin_name.split()
            if len(admin_name.split()) == 3:
                last_name, first_name, middle_name = admin_name.split()
            create_date = datetime.now().date()
            cnxn.execute(f'''INSERT INTO Person 
                        (LastName, FirstName, MiddleName,  CreateDate, CommonName, CreateType, Gender)
                        VALUES ('{last_name}', '{first_name}', '{middle_name}', '{create_date}', '{admin_name}', 5, 2)
                        ''')
            person_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
            cnxn.execute(
                f'INSERT INTO Employee (PersonId, EmployeeStatus) VALUES ({person_id}, 0)')
            employee_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
            cnxn.execute(
                f"INSERT INTO EmployeeAdmin (EmployeeId, JobStatus, StartDate) VALUES ({employee_id}, 0, '{create_date}')")
            admin_id = cnxn.execute(
                "SELECT SCOPE_IDENTITY()").fetchone()[0]
        temp = temp.copy()
        duration = temp['duration'].values[0]
        temp['start_date_time'] = pd.to_datetime(pd.to_datetime(temp['start_date'], format='%d.%m.%y'))
        res = temp[temp['start_date_time'] == temp['start_date_time'].max()][['day', 'start_hour']].values.tolist()
        schedule_name = ' '.join([' '.join([str(ii) for ii in i]) for i in res])
        group_name = f'Индив. {teacher_name} {schedule_name}'
        cnxn.execute(f'''INSERT INTO [Group] (Name, DisplayName, CreateDate, TeacherId, GroupStatus, CityId, StartDate, AdminId, CourseId, LessonDuration, EventType) 
                                VALUES ('{group_name}', '{group_name}', '{datetime.now().date()}', {teacher_id}, 1, {city_id}, '{start_date}', {admin_id}, {course_id}, {duration}, 1)''')
        group_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate)
                            VALUES ({group_id}, {student_id}, '{start_date}')''')
        for _, row in temp.iterrows():  
            start_date = row['start_date']
            d, m, y = start_date.split('.')
            start_date = f'20{y}-{m}-{d}'
            end_date = row['end_date']
            d, m, y = end_date.split('.')
            end_date = f'20{y}-{m}-{d}'
            duration = row['duration']
            hour = row['start_hour']
            minute = row['start_minute']
            week_day = row['week_day']
            cnxn.execute(f'''INSERT INTO Schedule (GroupId, TeacherId, AdminId, Minute, Hour, WeekDay, Duration, StartDate, EndDate, Status)
                        VALUES ({group_id}, {teacher_id}, {admin_id}, {minute}, {hour}, {week_day}, {duration}, '{start_date}', '{end_date}', 0)''')
    cnxn.commit()    
    
    
def load_group_schudule_items():
    schedule_items = pd.read_csv('groups_data/schedule_items.csv')
    groups_map = load_map('groups')
    rev_groups_map = {v: k for k, v in groups_map.items()}
    group_ids = cnxn.execute('select id from [Group] g where EventType = 0').fetchall()
    for group_id in tqdm(group_ids):
        group_id = group_id[0]
        schedules_group = cnxn.execute(f'select WeekDay, Duration, StartDate, EndDate, Hour, Minute, Id from Schedule g where GroupId = {group_id}').fetchall()
        alfa_group_id = rev_groups_map[group_id]
        gsia = schedule_items[schedule_items['group_id'] == alfa_group_id]
        gsia = gsia.copy()
        gsia['date'] = pd.to_datetime(gsia['date'], format='%Y-%m-%d')
        for schedule in schedules_group:
            temp = gsia[(gsia['day'] == schedule[0]) & (gsia['date'] >= schedule[2]) & (gsia['date'] <= schedule[3])]
            for _, row in temp.iterrows():
                schedule_id = schedule[-1]
                status = 0
                start, end = get_schedule_item_dates(np.datetime64(row['date']), schedule[1], schedule[4], schedule[5])
                cnxn.execute(f'''INSERT INTO ScheduleItem (ScheduleId, StartDateTime, EndDateTime, Status) VALUES ({schedule_id}, '{start}', '{end}', {status})''')
    cnxn.commit()    
    
    
def load_student_schedule_items():
    students = load_map('students')
    rev_student_map = {v: k for k, v in students.items()}
    student_ids = cnxn.execute('''select StudentId from Student2Group sg 
    WHERE GroupId  in (select id from [Group] g where EventType = 1)''').fetchall()
    student_ids = list(set([x[0] for x in student_ids]))
    for student_id in tqdm(student_ids):
        schedules_group = cnxn.execute(f'''select WeekDay, Duration, StartDate, EndDate, Hour, Minute, Id from Schedule g 
                                    where GroupId in (select GroupId from Student2Group sg where StudentId = {student_id})''').fetchall()
        alfa_student_id = rev_student_map[student_id]
        gsia = schedule_items[schedule_items['student_id'] == alfa_student_id]
        gsia = gsia.copy()
        gsia['date'] = pd.to_datetime(gsia['ru_date'], format='%Y-%m-%d')
        for schedule in schedules_group:
            temp = gsia[(gsia['day'] == schedule[0]) & (gsia['date'] >= schedule[2]) & (gsia['date'] <= schedule[3])]
            for _, row in temp.iterrows():
                schedule_id = schedule[-1]
                status = 0
                start, end = get_schedule_item_dates(np.datetime64(row['date']), schedule[1], schedule[4], schedule[5])
                cnxn.execute(f'''INSERT INTO ScheduleItem (ScheduleId, StartDateTime, EndDateTime, Status) VALUES ({schedule_id}, '{start}', '{end}', {status})''')
    cnxn.commit()    