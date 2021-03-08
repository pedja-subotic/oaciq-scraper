from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from pprint import pprint
from colorama import Fore
import pandas as pd
import requests
import random
import time
import sys
import csv
import os
import re


# - Site that's scraped: https://www.oaciq.com/en

# Note: the site is quite slow and takes some time to respond to requests

# - Needed to be done if the script won't run:
# Add new cookies and headers to get_source() function in order to be able to send a request.
# Add new cookies, headers and data('authenticity_token') to find_all_brokers() function in order to be able to send a request.


def find_all_brokers():
    """Finds all brokers"""
    cookies = {
        '_gcl_au': '1.1.1177621410.1608212695',
        '_ga': 'GA1.2.1590174178.1608212695',
        '_gid': 'GA1.2.1906815979.1608212695',
        '_hjTLDTest': '1',
        '_hjid': 'e002644e-956c-4478-ae2f-b0a365e6abae',
        '_fbp': 'fb.1.1608212695609.29392531',
        '__atuvc': '9%7C51',
        '__atuvs': '5fdbab5b8230c648000',
        '_oaciq_session_dksfhkdsfhdkdfhs': 'clQ1TE9JMklMbWNKQVBNUTFIdXBQaHk1L0NTTHdTTU90QmozVTVPVTZLK1hkZFprZ1V3NDVhTjZVdFZKYmp3TUdzZkNtY2JrM1FYdVBiY0kzM083c1VWVEFxVkJQRVY3U3B4ZkJWNk0wV3N0TXBoaGR1clZvVG11SXlqZDEzYlFhbEVlektDdzkvcGdaNERZd2YraktRcjlrLzQrbVlpcXRhVGhUT0hKakV6UkRtdGtZZTZwTnZwd3lCeWUzMWVUdGJiU3hPTGFLd1VwTDBxTTQrNTd0MElsOWNaOWhtVnRHQjhRanFWcWNPRW12aXZvUzlyREF5Y0FsUUowZ1BPWXV3dVpHRUVVQjJUZjBONTlremNXWWUzYUhhbzVQeTJ5Q1VyNnE0ME5tdHc9LS1yZ0VRNlQwWjlOQVpEWEFZakt4M3FBPT0%3D--da2abaf865567486c74e3067f90b038a712063ed',
    }

    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'sec-ch-ua-mobile': '?0',
        'Upgrade-Insecure-Requests': '1',
        'Origin': 'https://www.oaciq.com',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://www.oaciq.com/en',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    data = {
        'utf8': '\u2713',
        'authenticity_token': 'RakP3V/gIFIFFBm93FhG6vnrJ8Xak6Tw/FfpZ+92WF4=',
        'find_broker[name]': '',
        'find_broker[include_revoked_brokers]': '0',
        'find_broker[area_of_practice]': '',
        'find_broker[licence_number]': '',
        'find_broker[agency_name]': '',
        'find_broker[region]': '',
        'find_broker[city]': '',
        'commit': 'Search'
    }

    try:
        r = requests.post('https://www.oaciq.com/en/find-broker', headers=headers, cookies=cookies, data=data, timeout=120)
    except Exception as e:
        print(e)
        # The script is shut down because no brokers were extracted, in that case new headers and cookies need to be added
        print('The script is shut down because no brokers were extracted, in that case new headers and cookies need to be added')
        sys.exit()

    soup = BeautifulSoup(r.text, 'lxml')

    # - All broker links and cities
    all_brokers = []
    rows = soup.select('#find-brokers-result tbody tr')
    for row in rows:
        # Broker link
        broker_link = row.find_all('td')[0].find('a').get('data-redirect')

        # City
        try:
            city = row.find_all('td')[2].get_text(strip=True)
        except:
            city = ''

        all_brokers.append([broker_link, city])

    return all_brokers


def get_source(url):
    """Makes a GET request to the webpage.

    Args:
        url (str): url from a particular broker
    Returns:
        Returns requests Response if status_code = 200, otherwise returns None
    """

    cookies = {
        '_gcl_au': '1.1.1177621410.1608212695',
        '_ga': 'GA1.2.1590174178.1608212695',
        '_gid': 'GA1.2.1906815979.1608212695',
        '_hjTLDTest': '1',
        '_hjid': 'e002644e-956c-4478-ae2f-b0a365e6abae',
        '_fbp': 'fb.1.1608212695609.29392531',
        '_oaciq_session_dksfhkdsfhdkdfhs': 'MTlNUHdONll3bE41aEJLV2o3ZGxuRkFqVi9jQml5VzJCMUtBZkVha1lrenJDaXVqbkVMS2lNbVRkSmZGOFd3eDh5cXB3T0o4SU95Z0JpRE12dGZKZE9iY3NSL3YzK1E4bE5zcC9DTVZMUW5vOUJWdEFDRGR4N2dPcCs1YU5rV3ZCVmtOTytnbm1hSGhaUVRBWmF6bXg2bFppQzc0RW4wT3lFMWdqd3lTZmZNZGd1NHlTMjlWQUZreXNiTUJvNmRhdzdPR0YySHQ3Wjh3bzFaUk1RQzVEUlRVOU5OSjdRb2E4SlN2cXYvZ0xzdjB1aEV2ZXJOd3NacG1IMFpRUzZOVml0MzdmOGxxVDlNYmQ1RUZIaXdHaWd2MmtIaUxLcUFicEVoUC9McVQ0MkU9LS1ubkFTQXlIVkpwUERZTmcyRXZqLzZnPT0%3D--035b30e61fe2726c134d0571fbd6e8e9709e4ff3',
        '__atuvc': '11%7C51',
        '__atuvs': '5fdbab5b8230c648002',
        '_gat_UA-17730258-22': '1',
    }

    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'sec-ch-ua-mobile': '?0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://www.oaciq.com/en/find-broker',
        'Accept-Language': 'en-US,en;q=0.9',
        'If-None-Match': 'W/"e7e8115b89106ff4545b0854e51b2073"',
    }

    try:
        r = requests.get(url, headers=headers, cookies=cookies, timeout=40)
        status_code = r.status_code
        if status_code == 200:
            return r
        elif status_code == 404:
            print(Fore.YELLOW + 'Wrong link' + Fore.RESET)
            return None
        elif status_code == 443:
            print(Fore.RED + f'Status code: {status_code}' + Fore.RESET)

            # The script gets shut down if status code 443 occurs 10 times
            global count_433
            count_433 += 1
            if count_433 >= 10:
                sys.exit()
        else:
            print(Fore.LIGHTRED_EX + f'Status code: {status_code}' + Fore.RESET)
            return None
    except Exception as e:
        print(Fore.LIGHTRED_EX + str(e) + Fore.RESET)
        return None


def extract_data_test(row):
    """Extracts the needed data from a particular broker's page

    Args:
        row (list): list which contains broker's link and city
    """

    link = row[0]
    city = row[1]

    r = get_source(link)
    if not r:
        return

    soup = BeautifulSoup(r.text, 'lxml')

    # First name, Last Name
    first_name, last_name = '', ''
    try:
        full_name = soup.select_one('div.register-entity-name-header').get_text(strip=True)
        full_name_subtitle = soup.select_one('div.register-entity-name-header p.register-entity-subtitle')
        if full_name_subtitle:
            full_name = full_name.replace(full_name_subtitle.get_text(strip=True), '')
    except:
        print(Fore.LIGHTYELLOW_EX + 'Nema Name', Fore.RESET)
        return
    register_usual_name = soup.select_one('p.register-usual-name')
    if register_usual_name:
        full_name = full_name.replace(register_usual_name.get_text(strip=True), '')
    full_name_split = full_name.split(' ', 1)
    if len(full_name_split) >= 2:
        first_name = full_name_split[0]
        last_name = full_name_split[1]
    else:
        first_name = full_name_split[0]

    # Permit Number
    try:
        permit_number = soup.find('b', text=re.compile("Licence holder's number")).find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        permit_number = ''

    # Email
    try:
        email = soup.find('b', text=re.compile("E-mail")).find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        email = ''

    # Website
    try:
        website = soup.find('b', text=re.compile("Website")).find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        website = ''

    # Authorized field of practice
    try:
        authorized_field_of_practice = soup.find('b', text=re.compile("Authorized field of practice")).find_parent('div').find_next_sibling('div').get_text(strip=True)
        info_areas_of_practices = soup.select_one('div[data-box="info_areas_of_practices_full_exercice"]')
        if info_areas_of_practices:
            authorized_field_of_practice = authorized_field_of_practice.replace(info_areas_of_practices.get_text(strip=True), '')
        else:
            info_areas_of_practices = soup.select_one('div[data-box="info_areas_of_practices_residential"]')
            if info_areas_of_practices:
                authorized_field_of_practice = authorized_field_of_practice.replace(info_areas_of_practices.get_text(strip=True), '')
            if not info_areas_of_practices:
                info_areas_of_practices = soup.select_one('div[data-box="info_areas_of_practices_commercial"]')
                if info_areas_of_practices:
                    authorized_field_of_practice = authorized_field_of_practice.replace(info_areas_of_practices.get_text(strip=True), '')
    except:
        authorized_field_of_practice = ''

    # Company
    try:
        company = soup.find('b', text=re.compile("Practices within a business corporation")).find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        company = ''

    # Agency
    try:
        agency = soup.find('b', text="Agency").find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        agency = ''

    # Professional Address
    try:
        professional_address = soup.find('b', text=re.compile("Business address")).find_parent('div').find_next_sibling('div').get_text(strip=True)
    except:
        professional_address = ''

    # If the agency was visited instead of a broker
    agency_test = soup.select_one('h1.no_print')
    if agency_test:
        if 'agency' in agency_test.get_text(strip=True):
            agency = f'{first_name} {last_name}'
            first_name = ''
            last_name = ''

    # Save data
    all_data_list = [first_name, last_name, permit_number, email, website, authorized_field_of_practice, company, agency,
                     professional_address, city, link]
    csv_writer.writerow(all_data_list)

    global count_brokers
    count_brokers -= 1
    print(f'Brokers remaining: {count_brokers}. Current broker: {full_name}')


def save_data():
    """End function which executes other functions and saves the data to csv"""

    global csv_writer, count_brokers, count_433
    count_433 = 0

    file_name = 'oaciq_data.csv'
    with open(file_name, 'w', errors='ignore', newline='', encoding='utf-8') as f:
        col_names = ['First Name', 'Last Name', 'Permit Number', 'Email', 'Website', 'Authorized field of practice', 'Company',
                     'Agency', 'Professional Address', 'City', 'URL']

        csv_writer = csv.writer(f)
        csv_writer.writerow(col_names)

        # If an error occurs
        scraped_links = []
        try:
            df = pd.read_csv(file_name)
            print('Number of scraped rows: ', df.shape[0])
            scraped_links = [[row, row.rsplit('/', 1)[-1]] for row in df.URL.tolist()]
        except:
            print('pandas error')
            pass

        # All brokers
        all_brokers = find_all_brokers()
        count_brokers = len(all_brokers)
        print('Total amount of brokers: ', count_brokers)

        # Ignoring the scraped brokers
        if scraped_links:
            all_brokers = list(set(all_brokers) - set(scraped_links))
            print('Brokers remaining: ', len(all_brokers))

        # Threading
        ThreadPool(processes=4).map(extract_data_test, all_brokers)


#####################
# - The script gets run
save_data()
#####################


# Convert from csv to excel
df = pd.read_csv('oaciq_data.csv')
df.to_excel('oaciq_data.csv', index=False)




