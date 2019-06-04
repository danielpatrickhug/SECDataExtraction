# Daniel Patrick Hug
# 03/11/2019
# Script to collect form filings from SEC EDGAR 


import urllib.request
import gzip
import re
import pandas as pd
import requests
import lxml.html
import sqlite3
import csv
from bs4 import BeautifulSoup

from typing import Union

start_year = 2014
end_year = 2019

def create_connection(db_file):
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)

# Get company information by CIK.
def get_company_name(cik: Union[int, str]):
    # Setup company URL
    company_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}".format(cik)

    # Retrieve buffer
    remote_buffer = requests.get(company_url).content

    # Parse buffer to HTML
    html_doc = lxml.html.fromstring(remote_buffer)
    content_div = html_doc.get_element_by_id("contentDiv")
    company_info_div = list(content_div)[1]

    # Extract key fields
    company_data = {}
    try:
        company_data["name"] = list(list(company_info_div)[2])[0].text.strip()
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("Unable to parse name: {0}".format(e))
        company_data["name"] = None

    return company_data


def create_master_index():
	outlist = []
	count = 0
	header= '|'.join(['id' ,'cik', 'entity_name', 'file_type', 'filingdate', 'url']) + '\n'
	url_base = "https://www.sec.gov/Archives/"
	
	with open('../unzipped_indexes/Recent_10k/master_10K.csv', 'w') as outfile:
		outfile.write(header)
	for year in range(start_year, end_year+1):
		print(f'{year}') 
		try:
			with open(f'../unzipped_indexes/Recent_10k/report_{year}_10K', 'r', encoding='latin1') as infile:
				for line in infile:
					if(re.search(r'txt', line)):
						outlist.append(line)
		except Exception as e:
			print(f'Unable to parse index_{year}')				
	with open(f'../unzipped_indexes/Recent_10k/master_10K.csv', 'a') as outfile:
		for row in outlist:
			count+=1
			outfile.write(f'{count} | {row}')
	print(f"{count} 10Ks parsed")
			

def create_unique_cik_list():
	with open('../unzipped_indexes/master_index/master_index', 'r') as infile:
		cols = ['cik', 'entity_name', 'form_type', 'filing_date', 'file_url']
		dtypes = [float, str, str, str, str ]
		data = pd.read_csv(infile, sep='|')
		data = pd.DataFrame(data)
		cik_series = pd.Series(data[data.columns[0]])
		with open('../unzipped_indexes/test', 'w') as outfile:
			data.to_csv(outfile, sep='|')
		#cik_series = data[data.columns[0]].astype(str).astype(int)
		
		#cik_series = cik_series.unique()
		#cik_list = cik_series.tolist()
		#return cik_list



	
def combine():
	header= '|'.join(['form_type', 'cik', 'entity_name', 'filingdate', 'url']) + '\n'
	url_base = "https://www.sec.gov/Archives/"
	
	with open('../unzipped_indexes/form_index/master_form_index', 'w') as outfile:
		outfile.write(header)
	
	with open('../unzipped_indexes/form_index/master_form_index', 'a') as outfile:
		for year in range(start_year, end_year+1):
			for qtr in range(1,5):
				with open(f'../unzipped_indexes/form_index/index_{year}_{qtr}', 'r') as infile:
					outfile.writelines(infile.readlines()[1:])
		print("All years are combined")

##TODO: Allow any form type to be searched 
def extract_by_form_type():
	total_reports = 0
	for year in range(start_year, end_year):
		form_list = []
		for qtr in range(1,5):
			with open(f'../unzipped_indexes/recent_data/dataidx_{year}_Q{qtr}', 'r', encoding='latin1') as infile:
				for line in infile:
					if(re.search(r'10-K', line)):
						form_list.append(line)
			print(f'Writing {year}  10-K lines')
		
		with open(f'../unzipped_indexes/Recent_10k/report_{year}_10K', 'w', encoding='latin1') as outfile:
			outfile.writelines(form_list)
			total_reports += len(form_list)
	print(f'{total_reports} 10K reports extracted')


##Unzip index files into a text file
def unzip_indexes():
	try:
		for year in range (start_year, end_year):
			for qtr in range(1,5):
				path_to_file = f'../data/dataidx_{year}_Q{qtr}'	
				path_to_destination = f'../unzipped_indexes/recent_data/dataidx_{year}_Q{qtr}'	
				with gzip.open(path_to_file, 'rb') as infile:
					with open(path_to_destination, 'wb') as outfile:
						for line in infile:
							outfile.write(line)
	except Exception as e:
		print("could not unzip index")



##Collects index files from SEC.gov EDGAR 
def retrieve_indexes():
	try:
		for year in range(start_year, end_year):
			for qtr in range(1,5):
				print(f'Collecting Index file for {year}, Q{qtr}')
				urllib.request.urlretrieve(f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.gz', f'../data/dataidx_{year}_Q{qtr}')
	except Exception as e:
		print(f"Could not retrieve index for {year}, Q{qtr} ")


		
##Master index has , ',' in the names of a couple companies so this doesnt really work
def convert_to_csv():
	try:
		with open('../unzipped_indexes/master_index/master_index', 'r', encoding='latin1') as infile:
			with open('../unzipped_indexes/master_index/_index_csv', 'w') as outfile:
				data = infile.read()
				data = data.replace('|', ",")
				outfile.write(data)
	except Exception as e:
		print("could not cover to csv")

# Get company information by CIK.
def get_company(cik: Union[int, str]):
    # Setup company URL
    company_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}".format(cik)

    # Retrieve buffer
    remote_buffer = requests.get(company_url).content

    # Parse buffer to HTML
    html_doc = lxml.html.fromstring(remote_buffer)
    content_div = html_doc.get_element_by_id("contentDiv")
    company_info_div = list(content_div)[1]

    # Extract key fields
    company_data = {}

    try:
        raw_address = lxml.html.tostring(list(company_info_div)[0], method="text",
                                         encoding="utf-8").decode("utf-8")
        mailing_address = " ".join(raw_address.splitlines()[1:]).strip()
        company_data["mailing_address"] = mailing_address
    except Exception as e:  # pylint: disable=broad-except
        print(f'Unable to parse mailing_address: {e}')
        company_data["mailing_address"] = None

    try:
        raw_address = lxml.html.tostring(list(company_info_div)[1], method="text",
                                         encoding="utf-8").decode("utf-8")
        business_address = " ".join(raw_address.splitlines()[1:]).strip()
        company_data["business_address"] = business_address
    except Exception as e:  # pylint: disable=broad-except
        print(f'Unable to parse business_address: {e}')
        company_data["business_address"] = None

    try:
        company_data["name"] = list(list(company_info_div)[2])[0].text.strip()
    except Exception as e:  # pylint: disable=broad-except
        logger.warning("Unable to parse name: {0}".format(e))
        company_data["name"] = None

    try:
        ident_info_p = list(list(company_info_div)[2])[1]
        company_data["sic"] = list(ident_info_p)[1].text
    except Exception as e:  # pylint: disable=broad-except
        print(f'Unable to parse SIC: {e}')
        company_data["sic"] = None

    try:
        ident_info_p = list(list(company_info_div)[2])[1]
        company_data["state_location"] = list(ident_info_p)[3].text
    except Exception as e:  # pylint: disable=broad-except
        print(f'Unable to parse State location: {e}')
        company_data["state_location"] = None

    try:
        ident_info_p = list(list(company_info_div)[2])[1]
        company_data["state_incorporation"] = list(ident_info_p)[4].text
    except Exception as e:  # pylint: disable=broad-except
        print(f'Unable to parse State incorporation: {e}')
        company_data["state_incorporation"] = None

    return company_data

def parse_10k_data(company_url):
	# Retrieve buffer
	html_doc = requests.get(company_url).content
	# Parse buffer to HTML
	soup = BeautifulSoup(html_doc, 'html.parser')
	text = soup.get_text("\n")
	print(text)



if __name__ == '__main__':

	parse_10k_data('https://www.sec.gov/Archives/edgar/data/1318605/0001193125-14-069681.txt')



	






