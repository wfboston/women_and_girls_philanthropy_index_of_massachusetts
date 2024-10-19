import os
import sys
import re
from pathlib import Path
import csv
from zipcodes import greater_boston_zipcodes
import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from xlsx2csv import Xlsx2csv
import requests
from datetime import datetime
#import logging


WGI_URL = 'https://wgi.communityplatform.us/'
WGI_API_BASE = 'https://wgi.communityplatform.us/platform-api/'
STATE = 'MA'
ORG_LIST_URL = f'{WGI_API_BASE}search/base-search?page=1&perPage=10000&orderBy=revenue&keywordType=all&resultType=all&states[]={STATE}'
# https://wgi.communityplatform.us/platform-api/search/base-search?page=1&perPage=10000&orderBy=revenue&keywordType=all&resultType=all&states[]=MA
IRS_SRC_URL = 'https://www.irs.gov/statistics/soi-tax-stats-annual-extract-of-tax-exempt-organization-financial-data'
DEV_EMAIL = 'dhee.panwar@dell.com'
#script_dir = Path(os.path.dirname(os.path.abspath(sys.argv[0])))
API = 'https://wgi.communityplatform.us/platform-api/search/base-search?page=1&perPage=40&orderBy=revenue&keywordType=all&resultType=all&states%5B%5D=MA&onlyFilers=true&searchView=map'

IRS_ORG_LIST = 'https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf'

current_directory = os.getcwd()
output_folder_path = Path(os.path.join(current_directory, 'output_files'))
input_folder_path = Path(os.path.join(current_directory, 'input_files'))
logs = Path(os.path.join(current_directory, 'logs'))
WGI_file = input_folder_path/'WGI'/'WGI_MA_Only_11_6_23.csv'


def download_file(link, filepath, force=False):
    if not os.path.exists(filepath) or force:
        response = requests.get(link, stream=True)
        print(response)
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
        print(f'This process can take upto 5-10 minutes. Please Wait!')
        Xlsx2csv(str(src), outputencoding='utf-8').convert(str(dest))
    #print("Dest\n")
    #print(dest)
    return dest


def get_download_links():
    """Parse download links for excel forms from IRS website

    Returns:
        dict(int:list(dict{filename, link})): list of excel download links for each year
    """
    r = requests.get(IRS_SRC_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    # get all <h2> that appear before tables
    #pint(soup)
    h2s = h2s = soup.find_all('h2', 
                              string=re.compile('^Exempt Organization '
                             'Returns Filed in Calendar Year', re.IGNORECASE))
    download_links = {}
    for h2 in h2s:
        year = int(re.search(r'\d+', h2.text).group(0))
        table = h2.find_next()
        ##int(table)
        links = [{
            'name': f'{a_tag.text} ({year})',
            'link': a_tag['href'],
        } for a_tag in table.find_all('a')]
        download_links[year] = links
    #print(download_links)
    return download_links


def download_raw_data(year: int, force=False):
    """Download excel files from the IRS website corresponding to the year and convert to CSV
    Example of exported file: `input_files/2021/Form 990 Extract (2021).csv`

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

    download_folder = input_folder_path / f'{year}'
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
        #comment below
       ## print(f'{name.stem} headers: ', end='')
       # with open(csv_file) as f:
       #      csv_reader = csv.reader(f, delimiter = ',')
       #      for row in csv_reader:
       #          print(', '.join(row))
       #          break
    #comment above 
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
    wgi_dir = input_folder_path / 'WGI'
    os.makedirs(wgi_dir, exist_ok=True)
    xlsx_file = wgi_dir / Path(dl_link).name
    download_file(dl_link, xlsx_file, force=force)
    #print("Get latest Wgi done!")
    return xlsx_to_csv(xlsx_file)


def get_org(org_id):

    # https://wgi.communityplatform.us/platform-api/organization/1776515
    url = f'{WGI_API_BASE}organization/{org_id}'
    r = requests.get(url)
    return r.json()


def get_gba_orgs():
    """
    using data from Indiana Women and Girls Index API
    Get orgs from the Greater Boston Area 
    If a file is found use that to determine revenue
    otherwise call api, clean data and save it as a csv

    negative revenue values are counted as 0
    return int wg_revenue

    """
    # greater_boston_zipcodes
    orgs = requests.get(ORG_LIST_URL).json()['data']
    org_in_state = {}
    wg_revenue = 0
    
    output_file = output_folder_path/'greater_boston_orgs.csv'
    
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
                #print("Printing Revenue \n")
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


def get_ma_orgs_list(force=False):
    #Download Massachusetts Orgnaization Data from IRS
    try:
        r = requests.get(IRS_ORG_LIST)
    except: 
        print("Unable to access IRS website check this URL {IRS_ORG_LIST}")
        print(r)
    soup = BeautifulSoup(r.text, 'html.parser')
    a_tag = soup.find('a', string='Massachusetts')      
    if not a_tag:
        raise RuntimeError('Could not download Massachusetts Org list from ')       
    dl_link = a_tag['href']
    wgi_dir = input_folder_path 
    os.makedirs(wgi_dir, exist_ok=True)
    csv_file = wgi_dir / Path(dl_link).name
    download_file(dl_link, csv_file, force=force)
    return csv_file


def update_ein_header(data_frame):
    updated_df = data_frame.rename(columns={'ein':'EIN'},inplace=True)
    return updated_df

###########This is for MA ####################
#Read eo_ma file, extract EIN, Name, City Zip (Zip needs to be seprated to) 
#Create a CSV file with this file 
#Create a list data structure with EIN ( Name, City, Zip)
#Compare the EIN and with IRS data base both 900 and 900EZ file and update the above list with, "totcntrbs" in 900 and ezfile "totcntrbgfts"
#Generate a CSV file with this info


def update_ma_orgs_file(file_name=None):
    print(file_name)
    columns_to_capture = ['EIN','NAME','STREET', 'CITY', 'STATE', 'ZIP']
    df = pd.read_csv(file_name, usecols=columns_to_capture)
    df[['ZIP_PART_1', 'ZIP_PART_2']] = df['ZIP'].str.split('-', expand=True)
    df = df.drop('ZIP', axis=1)
    return df


def process_irs_990_extract_file(file_name):
    #print("Processing IRS 990 Extract File")
    try:
        #print("trying one method!")
        columns_to_capture = ['ein','totcntrbgfts'] # add function to update this file
        df = pd.read_csv(file_name, usecols=columns_to_capture)
        df.rename(columns={'ein':'EIN'},inplace=True)
    except: 
        #print("trying 2nd method!")
        columns_to_capture = ['EIN','totcntrbgfts'] # add function to update this file
        df = pd.read_csv(file_name, usecols=columns_to_capture)
    #print("Processing IRS 990 Extract file done!")
    return df


def process_irs_990_ez_file(file_name):
    #print("Processing IRS 990 EX file")
    try:
        #print("trying one method!")
        columns_to_capture = ['ein','totcntrbs'] # add function to update this file
        df = pd.read_csv(file_name, usecols=columns_to_capture)
        df.rename(columns={'ein':'EIN'},inplace=True)
    except: 
        #print("trying 2nd method!")
        columns_to_capture = ['EIN','totcntrbs'] # add function to update this file
        df = pd.read_csv(file_name, usecols=columns_to_capture)
        #df.head()
        
    #print("Processing IRS 990 EX file done!")
    return df

    
def merge_df(first_df, second_df):
    merged_df = pd.merge(first_df, second_df, on='EIN', how='left')
    return merged_df    


###########This is for Greater Boston Area####################
# Create a list with EIN greater boston  
# Convert index file in csv if needed
# Read the above MA file compare it with index zip file if the zip is there in MA fill 
# Update the List with filtered info
# Create a CSV file  with the filtered info


def generate_gb_report(year, gb_dataframe, irs_990_extract_dataframe, irs_990_ez_dataframe):
    try:
        gb_dataframe.rename(columns={'ein':'EIN'},inplace=True)
    except:
        pass
    gb_dataframe = merge_df(gb_dataframe, irs_990_extract_dataframe)
    gb_dataframe = merge_df(gb_dataframe, irs_990_ez_dataframe)
    print("Genrating list of organizations in Greater Boston Area...\n")
    output_folder_path = Path(os.path.join(current_directory, f'output_files/{year}/'))
    if not output_folder_path.exists():
        try: 
            os.makedirs(output_folder_path, exist_ok=True)
        except:
            print("File name already exists!")
    output_file = os.path.join(output_folder_path, f'greater_boston_report{year}.csv')
    gb_dataframe.to_csv(output_file)
    print(f"Greater Boston file generated!! File location {output_file} ")
    return gb_dataframe

###########This is for Womens only in GB  ####################
# Create a list with EIN Womens only  GB
# Read the above MA file compare it with Given V2 April_WSO_GSO_MA.xlsx if the zip is there in MA fill 
# Update the List with filtered info
# Create a CSV file  with the filtered info

def generate_wgi_in_gb_report(year, gb_dataframe, wgi_file):
    columns_to_capture_in_gb=['organizationName','id', 'name', 'description', 'address', 'categories','revenue','EIN', 'totcntrbgfts', 'totcntrbs']
    columns_to_capture_in_wgi=['EIN','Name']
    wgi_df= pd.read_csv(wgi_file, usecols = columns_to_capture_in_wgi)
    output_folder_path = Path(os.path.join(current_directory, f'output_files/{year}'))
    wgi_in_gb_df = merge_df(gb_dataframe, wgi_df)
    wgi_in_gb_df['w&g_organization'] = 'No'
    wgi_in_gb_df.loc[wgi_in_gb_df['Name'].notnull(), 'w&g_organization'] = 'Yes'
    wgi_in_gb_df = wgi_in_gb_df.drop(['Name'], axis=1)
    output_folder_path = Path(os.path.join(current_directory, f'output_files/{year}/'))
    output_file = os.path.join(output_folder_path, f'wgi_greater_boston_report{year}.csv')
    wgi_in_gb_df.to_csv(output_file)
    print(f"W&G file generated!! File location {output_file} ")

def is_valid_year(year_str):
    current_year = datetime.now().year
    try:
        year = int(year_str)
        return 2018 <= year < current_year
    except ValueError:
        return False
    
def get_valid_year():
    while True:
        year_str = input("Enter the year you would like to download the file : ")
        if is_valid_year(year_str):
            return int(year_str)
        else:
            print("Invalid year. Please enter a valid year.")
            
def generate_report(year, irs_990_extract_file, irs_990_ez_file, ma_orgs_file, greater_boston_orgs_file):
    try: 
        ma_orgs_data = update_ma_orgs_file(ma_orgs_file)
        irs_990_extract_dataframe = process_irs_990_extract_file(irs_990_extract_file)
        irs_990_ez_dataframe = process_irs_990_ez_file(irs_990_ez_file)
        ma_orgs_dataframe = merge_df(ma_orgs_data, irs_990_extract_dataframe)
        ma_orgs_dataframe = merge_df(ma_orgs_dataframe, irs_990_ez_dataframe)
        output_folder_path = Path(os.path.join(current_directory, f'output_files/{year}/'))
        if not output_folder_path.exists():
            try: 
                os.makedirs(output_folder_path, exist_ok=True)
            except:
                print(f"File name already exists!")
        output_file = os.path.join(output_folder_path, f'MA_orgs_report{year}.csv')
        ma_orgs_dataframe.to_csv(output_file)
        print(f"MA organizations report generated Location {output_file}")
        #ma_orgs_dataframe.head()
        gb_dataframe = pd.read_csv(greater_boston_orgs_file)
        #print(gb_dataframe)
        gb_report_dataframe = generate_gb_report(year, gb_dataframe, irs_990_extract_dataframe, irs_990_ez_dataframe) 
        generate_wgi_in_gb_report(year, gb_report_dataframe, WGI_file)
    except: 
        print(f"Unable to generate report contact {DEV_EMAIL}")
        
                
    
if __name__ == '__main__':
    try:
        if not os.path.exists(output_folder_path):    
            os.makedirs('output_files', exist_ok=True)
        if not os.path.exists(input_folder_path):  
            os.makedirs('input_files', exist_ok=True)
        if not os.path.exists('logs'):  
            os.makedirs('logs', exist_ok=True)   
       # logging.basicConfig(filename='logs/script.log', encoding='utf-8', level=logging.DEBUG)
        xlsx_key_list = input_folder_path/'V2_April_22_WSO_GSO_MA.xlsx'
        #xlsx_key_list = script_dir / 'keys' / 'V2 April 22_WSO_GSO_MA.xlsx'
        while not os.path.exists(xlsx_key_list):
            print(f'Warning file not found: {xlsx_key_list}')
            xlsx_key_list = Path(input('Enter keys source file: '))
        csv_key_list = xlsx_key_list.with_suffix('.csv')
        if not os.path.exists(csv_key_list):
            print('Converting', xlsx_key_list.name, 'to csv')
            Xlsx2csv(str(xlsx_key_list),
                    outputencoding='utf-8').convert(str(csv_key_list))
        year=get_valid_year()
        wg_revenue = get_gba_orgs()
        print("Downloading latest revenue data")
        wgi_latest = get_latest_wgi()
        total_revenue = download_raw_data(year)
        print('Percent contribution:', round(wg_revenue / total_revenue * 100, 2), '%')
        print(f'Processing data to generate MA Orgs, Great Boston Orgs and W&G Orgs in Great Boston for year {year}')
       
        ma_orgs_file=get_ma_orgs_list()
        file_990_extract_name = f'Form 990 Extract ({year}).csv'
        file_990_ez_name = f'Form 990-EZ Extract ({year}).csv'
        irs_990_extract_file = input_folder_path / str(year) /file_990_extract_name
        irs_990_ez_file = input_folder_path / str(year) /file_990_ez_name
        greater_boston_orgs_file = Path(os.path.join(current_directory, 'output_files/greater_boston_orgs.csv')) 
        try:
            generate_report(year, irs_990_extract_file, irs_990_ez_file, ma_orgs_file, greater_boston_orgs_file)
        except:
            print("Unable to generate the report! Please contact Dhee Panwar <{DEV_EMAIL}>")
    
    except Exception as e:
        exc_type, exc_tb = sys.exc_info()[0], sys.exc_info()[2]
        print(e.__repr__())
        print(f'\nThe error above was encountered on line {exc_tb.tb_lineno}. Please contact Dhee Panwar <{DEV_EMAIL}>')

