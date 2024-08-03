import re
import pyodbc
import old.recs as r
from time import sleep
from tqdm import tqdm
from datetime import datetime
from old.helpers import load_map, save_map, calculate_age, refactor_date

cnxn = pyodbc.connect(
    Trusted_Connection='Yes',
    Driver='{ODBC Driver 11 for SQL Server}',
    Server='localhost,1433',
    Database='itm'
)


def populate_cities(branches):
    print('Заполняем города')
    cities_map = {}
    khazakhstan_id = 4
    res = cnxn.execute(
        f'SELECT HierarchyId FROM Country WHERE Id = {khazakhstan_id}')
    hierarchy_id = res.fetchone()[0]
    for branch in branches['items']:
        print(f'Добавляем город {branch["name"]}')
        cnxn.execute(f'''INSERT INTO LocationHierarchy (ParentId, LocationType)
                    VALUES ({hierarchy_id}, 2)''')
        new_hierarchy_id = cnxn.execute(
            "SELECT SCOPE_IDENTITY()").fetchone()[0]
        cnxn.execute(f'''INSERT INTO City (Name, CountryId, HierarchyId) 
                    VALUES ('{branch["name"]}', {khazakhstan_id}, {new_hierarchy_id})''')
        city_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        cities_map[branch['id']] = int(city_id)
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


def populate_teachers(branches):
    cities_map = load_map('cities')
    teacher_map = {}
    for branch in branches['items']:
        branch_id = branch['id']
        print(f'Загружаем учителей по городу {branch["name"]}')
        page = 0
        while True:
            teachers = r.get_teachers(branch_id=branch_id, page=page)
            if not teachers['items']:
                break
            for teacher in tqdm(teachers['items']):
                if teacher['id'] in teacher_map:
                    continue
                common_name = teacher['name']
                name = common_name.split(' ')
                middle_name = ''
                if len(name) == 2:
                    last_name, first_name = name
                if len(name) == 3:
                    last_name, first_name, middle_name = name
                if teacher['gender'] == 1:
                    gender = 1
                elif teacher['gender'] == 0:
                    gender = 2
                else:
                    gender = 0
                email = teacher['email'][0] if teacher['email'] else '-'
                phone = teacher['phone'][0] if teacher['phone'] else 'null'
                if phone:
                    phone = phone.replace(
                        '+', '').replace('(', '').replace(')', '').replace('-', '')
                city_ids = list(set(teacher['branch_ids']))
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
                for city_id in city_ids:
                    new_city_id = cities_map[city_id]
                    cnxn.execute(
                        f'INSERT INTO Employee2City (EmployeeId, CityId) VALUES ({employee_id}, {new_city_id})')
                teacher_map[teacher['id']] = [
                    int(teacher_id), int(employee_id)]
                page += 1
                sleep(10)
    cnxn.commit()
    save_map(teacher_map, 'teachers')


def populate_tariffs(branches):
    print('Заполняем таблицу Tariff')
    cities_map = load_map('cities')
    tariff_map = {}
    for branch in branches['items']:
        branch_id = branch['id']
        page = 0
        while True:
            tariffs = r.get_tariffs(branch_id, page)
            if not tariffs['items']:
                break
            for tariff in tariffs['items']:
                if tariff['id'] in tariff_map:
                    continue
                name = tariff['name']
                price = float(tariff['price'])
                lesson_count = tariff['lessons_count']
                lesson_duration = tariff['duration'] if tariff['duration'] else 'null'
                added = tariff['added'].split(' ')[0]
                price_per_lesson = price / lesson_count
                cnxn.execute(f'''INSERT INTO Tariff (Name, CurrencyCode, Price, PricePerLesson,	LessonCount, LessonDuration, StartDate )
                             VALUES ('{name}', 'KZT', {price}, {price_per_lesson}, {lesson_count}, {lesson_duration}, '{added}')''')
                tariff_id = cnxn.execute(
                    "SELECT SCOPE_IDENTITY()").fetchone()[0]
                for tariff_branch_id in tariff['branch_ids']:
                    city_id = cities_map[tariff_branch_id]
                    cnxn.execute(f'''INSERT INTO Tariff2CountryCity (CityId, TariffId)
                                VALUES ({city_id}, {tariff_id})''')
                tariff_map[tariff['id']] = int(tariff_id)
            page += 1
            sleep(10)
    cnxn.commit()
    save_map(tariff_map, 'tariffs')


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


def populate_students(branches):
    cities_map = load_map('cities')
    student_map = {}
    for branch in branches['items']:
        branch_id = branch['id']
        print(f'Добавляем данные по студентам по городу {branch["name"]}')
        page = 0
        while True:
            students = r.get_branch_customers(branch_id, page)
            if not students['items']:
                break
            for student in tqdm(students['items']):
                if student['id'] in student_map:
                    continue
                common_name = student['name']
                parent_name = student['legal_name']
                email = student['email'][0] if student['email'] else '-'
                phone = student['phone'][0] if student['phone'] else 'null'
                dob = student['dob']
                create_date = datetime.now().date()
                if phone != 'null':
                    phone = ''.join(re.findall(r'\d+', phone))
                sql = f'''INSERT INTO Person (CreateDate, CommonName, CreateType, BirthDate)
                            VALUES ('{create_date}', '{common_name}', 5'''
                age = None
                if dob:
                    d, m, y = dob.split('.')
                    bord = datetime.strptime(dob, '%d.%m.%Y')
                    age = calculate_age(bord)
                    dob = f'{y}-{m}-{d}'
                    sql += f", '{dob}') "
                else:
                    sql += ', null)'
                cnxn.execute(sql)
                student_person_id = cnxn.execute(
                    "SELECT SCOPE_IDENTITY()").fetchone()[0]
                cnxn.execute(f'''INSERT INTO Person (CreateDate, CommonName, CreateType, Mobile, eMail)
                            VALUES ('{create_date}', '{parent_name}', 5, {phone}, '{email}')''')
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
                for stunet_branch_id in student['branch_ids']:
                    city_id = cities_map[stunet_branch_id]
                    cnxn.execute(
                        f'''INSERT INTO Student2City (StudentId, CityId) VALUES ({student_id}, {city_id})''')
                cnxn.execute(f'''INSERT INTO Student2Parent (StudentId, Parentid, CreateType) 
                                VALUES ({student_id}, {parent_id}, 5)''')
                student_map[student['id']] = int(student_id)
            page += 1
            sleep(10)
    cnxn.commit()
    save_map(student_map, 'students')


def populate_group_init(branches):
    group_map = {}
    cities_map = load_map('cities')
    branches = r.get_branches()
    teachers_map = load_map('teachers')
    for branch in branches['items']:
        print(f'Загрузка данных по граппам по городу {branch["name"]}')
        page = 0
        while True:
            groups = r.get_group(branch['id'], page)
            if not groups['items']:
                break
            for group in tqdm(groups['items']):
                if not group['teachers']:
                    continue
                if group['teachers'][0]['id'] not in teachers_map:
                    continue
                teacher_id = teachers_map[group['teachers'][0]['id']][0]
                group_name = group['name']
                bo_id = group['custom_bogrouplink'].split(
                    '/')[-1].split('#')[0]
                d, m, y = group['b_date'].split('.')
                # create_date = f'{y}-{m}-{d}'
                city_id = cities_map[group['branch_ids'][0]]
                if bo_id and 'meet.jit.si' not in group['custom_bogrouplink']:
                    cnxn.execute(f'''INSERT INTO [Group] (boId, Name, DisplayName, CreateDate, TeacherId, GroupStatus, CityId) 
                                VALUES ({bo_id}, '{group_name}', '{group_name}', '{datetime.now().date()}', {teacher_id}, 1, {city_id})''')
                else:
                    cnxn.execute(f'''INSERT INTO [Group] (Name, DisplayName, CreateDate, TeacherId, GroupStatus, CityId) 
                                VALUES ('{group_name}', '{group_name}', '{datetime.now().date()}', {teacher_id}, 1, {city_id})''')
                group_id = cnxn.execute(
                    "SELECT SCOPE_IDENTITY()").fetchone()[0]
                group_map[group['id']] = int(group_id)
            page += 1
            sleep(10)
    cnxn.commit()
    save_map(group_map, 'groups')


def populate_schedules(branches):
    groups_map = load_map('groups')
    teacher_map = load_map('teachers')
    subjects_map = load_map('subjects')
    for branch in branches['items']:
        branch_id = branch['id']
        page = 0
        print(f'Загружаем расписание с города {branch["name"]}')
        while True:
            schedules = r.get_reg_lessons(branch_id, page)
            if not schedules['items']:
                break
            for schedule in tqdm(schedules['items']):
                if schedule['related_class'] != 'Group':
                    continue
                if schedule['related_id'] not in groups_map or not schedule['teacher_ids']:
                    continue
                group_id = groups_map[schedule['related_id']]
                teacher_id = teacher_map[schedule['teacher_ids'][0]]
                start_date = schedule['b_date_v']
                d, m, y = start_date.split('.')
                start_date = f'{y}-{m}-{d}'
                end_date = schedule['e_date_v']
                d, m, y = end_date.split('.')
                end_date = f'{y}-{m}-{d}'
                start_time = datetime.strptime(
                    schedule['time_from_v'], '%H:%M')
                end_time = datetime.strptime(schedule['time_to_v'], '%H:%M')
                duration = int((end_time - start_time).total_seconds() / 60)
                hour = start_time.hour
                minute = start_time.minute
                week_day = schedule['day'] - 1
                course_id = subjects_map[schedule['subject_id']]
                cnxn.execute(
                    f'''UPDATE [GROUP] set CourseId = {course_id} WHERE id={group_id}''')
                cnxn.execute(f'''INSERT INTO Schedule (GroupId, TeacherId, Minute, Hour, WeekDay, Duration, StartDate, EndDate, Status)
                            VALUES ({group_id}, {teacher_id[0]}, {minute}, {hour}, {week_day}, {duration}, '{start_date}', '{end_date}', 0)''')
                cnxn.commit()
            page += 1
            sleep(10)


def students_to_group(branches):
    student_map = load_map('students')
    group_map = load_map('groups')
    reversed_group_map = {v: k for k, v in group_map.items()}
    cities_map = load_map('cities')
    for branch in branches['items']:
        print(f's2g в городе {branch["name"]}')
        branch_id = branch['id']
        city_id = cities_map[branch['id']]
        group_ids = [i[0] for i in cnxn.execute(
            f'SELECT id FROM [Group] WHERE CityId = {city_id}').fetchall()]
        for group_id in group_ids:
            alfa_group_id = reversed_group_map[group_id]
            students = r.client_to_group(branch_id, alfa_group_id)
            for student in tqdm(students['items']):
                student_id = student_map[student['customer_id']]
                join_date = student['b_date']
                d, m, y = join_date.split('.')
                join_date = f'{y}-{m}-{d}'
                leave_date = student['e_date']
                if leave_date:
                    d, m, y = leave_date.split('.')
                    leave_date = f'{y}-{m}-{d}'
                    cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate, LeaveDate, GroupLeaveReason)
                                VALUES ({group_id}, {student_id}, '{join_date}', '{leave_date}', 2)''')
                else:
                    cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate)
                                VALUES ({group_id}, {student_id}, '{join_date}')''')
                sleep(10)
    cnxn.commit()


def populate_student_tariff(branches):
    tariffs_map = load_map('tariffs')
    students_map = load_map('students')
    reversed_students_map = {v: k for k,v in students_map.items()}
    cities_map = load_map('cities')
    for branch in branches['items']:
        branch_id = branch['id']
        city_id = cities_map[branch_id]
        student_ids = cnxn.execute(f'''SELECT StudentId FROM Student2City WHERE CityId = {city_id}''').\
            fetchall()
        student_ids = [i[0] for i in student_ids]
        print(f'Загрзужаем тарифы для студентов по городу {branch["name"]}')
        for student_id in tqdm(student_ids):
            if student_id not in reversed_students_map:
                continue
            customer_id = reversed_students_map[student_id]
            tariffs = r.get_customer_tariffs(branch_id, customer_id)
            for tariff in tariffs['items']:
                if tariff['tariff_id'] not in tariffs_map:
                    print(tariff)
                    continue
                db_tariff_id = tariffs_map[tariff['tariff_id']]
                db_tariff_price, db_tariff_lessons_left = cnxn.execute(f'''SELECT Price, LessonCount FROM tariff WHERE id = {db_tariff_id}''').fetchone()
                start_date = refactor_date(tariff['b_date'])
                # end_date = refactor_date(tariff['e_date'])
                cnxn.execute(f'''INSERT INTO Tariff2Student (TariffId, StudentId, LessonLeft, StartDate, LessonCount, Price, PriceLeft)
                            VALUES ({db_tariff_id}, {student_id}, {db_tariff_lessons_left}, '{start_date}', {db_tariff_lessons_left}, {db_tariff_price}, {db_tariff_price})''')
        sleep(10)
        cnxn.commit()

def update_group_start_date():
    sql = '''UPDATE [Group]
            SET [Group].StartDate = tt.StartDate,
                [Group].LessonDuration = tt.Duration
                    FROM [Group]
                    INNER JOIN (
                        SELECT DISTINCT t.GroupId, t.StartDate, s.Duration FROM (
                        SELECT GroupId, MIN(StartDate) AS StartDate
                        FROM Schedule s
                        GROUP BY GroupId) t
                        RIGHT JOIN Schedule s ON
                        s.StartDate = t.StartDate and 
                        s.GroupId = t.GroupId
                        WHERE t.GroupId is not null
                    ) AS tt ON [Group].id = tt.GroupId;'''
    cnxn.execute(sql)
    cnxn.commit()
    cnxn.execute('''DELETE FROM [Group] WHERE StartDate is null''')
    cnxn.commit()
    

def populate_indiv_groups(branches):
    indiv_group_map = {}
    cities_map = load_map('cities')
    students_map = load_map('students')
    teacher_map = load_map('teachers')
    subjects_map = load_map('subjects')
    for branch in branches['items']:
        branch_id = branch['id']
        city_id =cities_map[branch['id']]
        page = 0
        print(f'Загружаем индивидуальные группы с города {branch["name"]}')
        while True:
            schedules = r.get_reg_lessons(branch_id, page)
            if not schedules['items']:
                break
            for schedule in tqdm(schedules['items']):
                if schedule['related_class'] == 'Group':
                    continue
                if schedule['related_id'] not in students_map or not schedule['teacher_ids']:
                    continue
                student_id = students_map[schedule['related_id']]
                if schedule['teacher_ids'][0] not in teacher_map:
                    continue
                teacher_id = teacher_map[schedule['teacher_ids'][0]]
                start_date = schedule['b_date_v']
                d, m, y = start_date.split('.')
                start_date = f'{y}-{m}-{d}'
                end_date = schedule['e_date_v']
                d, m, y = end_date.split('.')
                end_date = f'{y}-{m}-{d}'
                start_time = datetime.strptime(
                    schedule['time_from_v'], '%H:%M')
                end_time = datetime.strptime(schedule['time_to_v'], '%H:%M')
                duration = int((end_time - start_time).total_seconds() / 60)
                hour = start_time.hour
                minute = start_time.minute
                week_day = schedule['day'] - 1
                course_id = subjects_map[schedule['subject_id']]
                teacher_name = cnxn.execute(f'''select LastName from Person p 
                                            where id in (
                                            select PersonId from Employee e 
                                            where id in (
                                            select EmployeeId from EmployeeTeacher et 
                                            where id = {teacher_id[0]}))''').fetchone()[0]
                group_name = branch["name"] + f' {teacher_name}'
                if schedule['related_id'] in indiv_group_map:
                    group_id = indiv_group_map[schedule['related_id']]
                    old_course_id = cnxn.execute(f'''SELECT CourseId FROM [Group] WHERE id = {group_id}''').fetchone()[0]
                    if old_course_id == course_id:
                        cnxn.execute(f'''INSERT INTO Schedule (GroupId, TeacherId,  Minute, Hour, WeekDay, Duration, StartDate, EndDate, Status)
                            VALUES ({group_id}, {teacher_id[0]}, {minute}, {hour}, {week_day}, {duration}, '{start_date}', '{end_date}', 0)''')
                    continue
                cnxn.execute(f'''INSERT INTO [GROUP] (Name, DisplayName, StartDate, 
                            CreateDate, CourseId, TeacherId, EventType, GroupStatus, LessonDuration, CityId) 
                    VALUES ('{group_name}', '{group_name}', '{start_date}', 
                            '{datetime.now().date()}', {course_id}, {teacher_id[0]}, 1, 0, {duration}, {city_id})''')
                group_id = cnxn.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
                cnxn.execute(f'''INSERT INTO Student2Group (GroupId, StudentId, JoinDate)
                            VALUES ({group_id}, {student_id}, '{start_date}')''')
                cnxn.execute(f'''INSERT INTO Schedule (GroupId, TeacherId,  Minute, Hour, WeekDay, Duration, StartDate, EndDate, Status)
                            VALUES ({group_id}, {teacher_id[0]}, {minute}, {hour}, {week_day}, {duration}, '{start_date}', '{end_date}', 0)''')
                cnxn.commit()
                indiv_group_map[schedule['related_id']] = int(group_id)
            page += 1
            sleep(10)
    save_map(indiv_group_map, 'indiv_group')