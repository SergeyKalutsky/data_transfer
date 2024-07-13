import requests
from credentials import email, api_key


hostname = 'algoritmikakz.s20.online'


def get_token() -> str:
    data = {"email": email, "api_key": api_key}
    url = f'https://{hostname}/v2api/auth/login'
    response = requests.post(url, json=data)
    return response.json()['token']


headers = {'X-ALFACRM-TOKEN': get_token()}


def get_branches(page: int = 0) -> dict:
    url = f'https://{hostname}/v2api/branch/index'
    data = {"is_active": 1, "page": page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_branch_customers(branch_id: int, page: int) -> dict:
    data = {"page": page}
    url = f'https://{hostname}/v2api/{branch_id}/customer/index'
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_subjects(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/subject/index'
    data = {"page": page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_teachers(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/teacher/index'
    data = {"page": page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_lessons(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/lesson/index'
    data = {"page": page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_group(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/group/index'
    data = {"page": page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def client_to_group(branch_id: int, group_id: int):
    url = f'https://{hostname}/v2api/{branch_id}/cgi/index?group_id={group_id}'
    data = {'page': 0}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_tariffs(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/tariff/index'
    data = {'page': page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_discounts(branch_id: int):
    url = f'https://{hostname}/v2api/{branch_id}/discount/index'
    data = {'page': 0}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_customer_tariffs(branch_id: int, customer_id: int):
    url = f'https://{hostname}/v2api/{branch_id}/customer-tariff/index?customer_id={customer_id}'
    data = {'page': 0}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def payments(branch_id: int):
    url = f'https://{hostname}/v2api/{branch_id}/pay/index'
    data = {'page': 0}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_locations(branch_id: int):
    url = f'https://{hostname}/v2api/{branch_id}/location/index'
    data = {'page': 0}
    response = requests.post(url, json=data, headers=headers)
    return response.json()


def get_reg_lessons(branch_id: int, page: int):
    url = f'https://{hostname}/v2api/{branch_id}/regular-lesson/index'
    data = {'page': page}
    response = requests.post(url, json=data, headers=headers)
    return response.json()
