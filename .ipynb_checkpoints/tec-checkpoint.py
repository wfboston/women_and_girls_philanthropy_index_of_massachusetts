import os
import sys
import re
from pathlib import Path
import csv
from zipcodes import greater_boston_zipcodes
import concurrent.futures

from bs4 import BeautifulSoup
from tqdm import tqdm
from xlsx2csv import Xlsx2csv
import requests

WGI_URL = 'https://wgi.communityplatform.us/'
WGI_API_BASE = 'https://wgi.communityplatform.us/platform-api/'
STATE = 'MA'
ORG_LIST_URL = f'{WGI_API_BASE}search/base-search?page=1&perPage=10000&orderBy=revenue&keywordType=all&resultType=all&states[]={STATE}'
# https://wgi.communityplatform.us/platform-api/search/base-search?page=1&perPage=10000&orderBy=revenue&keywordType=all&resultType=all&states[]=MA
IRS_SRC_URL = 'https://www.irs.gov/statistics/soi-tax-stats-annual-extract-of-tax-exempt-organization-financial-data'
DEV_EMAIL = 'elijahllopezz@gmail.com'
script_dir = Path(os.path.dirname(os.path.abspath(sys.argv[0])))
API = 'https://wgi.communityplatform.us/platform-api/search/base-search?page=1&perPage=40&orderBy=revenue&keywordType=all&resultType=all&states%5B%5D=MA&onlyFilers=true&searchView=map'

def download_file(link, filepath, force=False):
    if not os.path.exists(filepath) or force:
        response = requests.get(link, stream=True)
        with open(filepath, 'wb') as f:
            # 10MB chunk size
            for chunk in tqdm(response.iter_content(chunk_size=10_000_000),
                              desc=f'Downloading {filepath.name}'):
                if chunk:
                    f.write(chunk)


def xlsx_to_csv(src, dest=None, force=False):
    stem = src.stem
    if dest is None:
        dest = src.with_suffix('.csv')
    if not os.path.exists(dest) or force:
        print(f'Converting {stem}.xslx to {stem}.csv...')
        Xlsx2csv(str(src), outputencoding='utf-8').convert(str(dest))
    return dest


def get_download_links():
    """Parse download links for excel forms from IRS website

    Returns:
        dict(int:list(dict{filename, link})): list of excel download links for each year
    """
    r = requests.get(IRS_SRC_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    # get all <h2> that appear before tables
    h2s = filter(
        lambda tag: tag.text.startswith(
            'Exempt Organization Returns Filed in Calendar Year'),
        soup.find_all('h2'))
    download_links = {}
    for h2 in h2s:
        year = int(re.search(r'\d+', h2.text).group(0))
        table = h2.find_next('table')
        links = [{
            'name': f'{a_tag.text} ({year})',
            'link': a_tag['href'],
        } for a_tag in table.find_all('a')]
        download_links[year] = links
    return download_links


def download_raw_data(year: int, force=False):
    """Download excel files from the IRS website corresponding to the year and convert to CSV
    Example of exported file: `script_dir/2021/Form 990 Extract (2021).csv`

    Raises RuntimeError if something went wrong
    Raises ValueError if year is not available to download

    Args:
        year (int): year for which files to download
    """
    # https://pythonprogramming.net/introduction-scraping-parsing-beautiful-soup-tutorial/
    # https://www.crummy.com/software/BeautifulSoup/bs4/doc/
    try:
        download_links = get_download_links()
    except requests.RequestException:
        raise RuntimeError(f'Could not access IRS website ({IRS_SRC_URL})')
    if not download_links:
        raise RuntimeError('Could not parse IRS website to fetch links')
    if year not in download_links:
        raise ValueError(f'Year {year} is unavailable')

    # download files into directory

    download_folder = script_dir / f'{year}'
    os.makedirs(download_folder, exist_ok=True)
    total_contrib = 0
    for file in download_links[year]:
        name = download_folder / file['name']
        xlsx_file = name.with_suffix('.xlsx')
        download_file(file['link'], xlsx_file, force=force)
        csv_file = str(xlsx_to_csv(xlsx_file, force=force))
        if '990-EZ' in csv_file:
            with open(csv_file) as f:
                total_contrib += sum(int(r['totcntrbs']) for r in csv.DictReader(f))
        elif 'Form 990 Extract' in csv_file:
            with open(csv_file) as f:
                total_contrib += sum(int(r['totcntrbgfts']) for r in csv.DictReader(f))
        # print(f'{name.stem} headers: ', end='')
        # with open(csv_file) as f:
        #     csv_reader = csv.reader(f, delimiter = ',')
        #     for row in csv_reader:
        #         print(', '.join(row))
        #         break
    print('Completed required download and CSV conversions')
    print('Total Contribution:', total_contrib)
    return total_contrib


def get_latest_wgi(force=False):
    r = requests.get(WGI_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    a_tag = soup.find('a', string='Download The List')
    if not a_tag:
        raise RuntimeError('Could not download WGI list from ')
    dl_link = a_tag['href']
    wgi_dir = script_dir / 'WGI'
    os.makedirs(wgi_dir, exist_ok=True)
    xlsx_file = wgi_dir / Path(dl_link).name
    download_file(dl_link, xlsx_file, force=force)
    return xlsx_to_csv(xlsx_file)


def get_org(org_id):

    # https://wgi.communityplatform.us/platform-api/organization/1776515
    url = f'{WGI_API_BASE}organization/{org_id}'
    r = requests.get(url)
    return r.json()


def get_gba_orgs():
    """
    Get orgs from the Greater Boston Area
    """
    # greater_boston_zipcodes
    orgs = requests.get(ORG_LIST_URL).json()['data']
    org_in_state = {}
    wg_revenue = 0
    output_file = script_dir / 'output.csv'
    try:
        with open(output_file) as f:
            wg_revenue = sum(int(r['revenue']) for r in csv.DictReader(f))
    except (FileNotFoundError, ValueError):
        # https://wgi.communityplatform.us/platform-api/organization/1776515
        for org in orgs:
            org_zip = int(org['zip'])
            if org_zip in greater_boston_zipcodes:
                # clean data
                org.pop('distance')
                org.pop('icon')
                org.pop('programId')
                org.pop('programName')
                org.pop('redirectUrl')
                org.pop('relevance')
                org['ein'] = ''
                org_in_state[org['organizationId']] = org
                revenue = org['revenue']
                if isinstance(revenue, str):
                    revenue = revenue.strip()
                    if revenue[0] == '(' and revenue[-1] == ')':
                        revenue = f'-{revenue[1:-1]}'
                    if revenue == '-':
                        revenue = 0
                    revenue = int(revenue)
                org['revenue'] = revenue
                wg_revenue += revenue

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_org = {executor.submit(get_org, org_id): org_id for org_id in org_in_state}
            # Order is not guaranteed even if you use a list. Use the value part above as an index
            for future in tqdm(concurrent.futures.as_completed(future_to_org), total=len(future_to_org), desc='Downloading organization data'):
                try:
                    org_id = future_to_org[future]
                    res = future.result()
                    org_in_state[org_id]['ein'] = res['ein']
                except ValueError as e:
                    print(e)

        with open(output_file, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=list(next(iter(org_in_state.values())).keys()))
            writer.writeheader()
            writer.writerows(org_in_state.values())
    print('Organization data for Greater Boston Area found in:', output_file)
    print('Revenue W&G organizations:', wg_revenue)
    return wg_revenue


if __name__ == '__main__':
    try:
        wg_revenue = get_gba_orgs()
        wgi_latest = get_latest_wgi()
        total_revenue = download_raw_data(2021)
        xlsx_key_list = script_dir / 'keys' / 'V2 April 22_WSO_GSO_MA.xlsx'
        while not os.path.exists(xlsx_key_list):
            print(f'warning file not found: {xlsx_key_list}')
            xlsx_key_list = Path(input('enter keys source file: '))
        csv_key_list = xlsx_key_list.with_suffix('.csv')
        if not os.path.exists(csv_key_list):
            print('Converting', xlsx_key_list.name, 'to csv')
            Xlsx2csv(str(xlsx_key_list),
                     outputencoding='utf-8').convert(str(csv_key_list))
        print('Percent contribution:', round(wg_revenue / total_revenue * 100, 2), '%')
    except Exception as e:
        exc_type, exc_tb = sys.exc_info()[0], sys.exc_info()[2]
        print(e.__repr__())
        print(f'\nThe error above was encountered on line {exc_tb.tb_lineno}. Please contact Elijah Lopez <{DEV_EMAIL}>')

# UofM list of orgs in the greater boston area serving women & girls for 2018
# use zip codes provided in IRS data set to find all orgs in the greater boston area
# use this list to find contributions for greater boston orgs for years 2018-2020
