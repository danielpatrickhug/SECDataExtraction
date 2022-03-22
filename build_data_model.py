import pymysql
import csv
#import re
#import pandas as pd
import requests
import lxml.html
import time
from multiprocessing import Pool

from typing import Union







# Get company information by CIK.
def get_company_name(url):
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



def create_cik_set():

	counter2=0
	with open('../unzipped_indexes/master_index/master_index', 'r') as infile:
		rdr = csv.reader(infile, delimiter ='|')
		CIK = set()

		for row in rdr:
			counter2+=1
			CIK.add(row[0])
			print(f'{counter2} rows parsed. Unique Ciks collected{len(CIK)}')
	return CIK



def parse_sec_by_cik(CIK):
    counter=0
    with open('../unzipped_indexes/name_cik_index', 'w') as outfile:
        wrt = csv.writer(outfile)
        wrt.writerow(['cik', 'entity_name'])
        for cik in CIK:
            counter+=1
            entity_name = get_company_name(cik)
            entities = (int(cik), entity_name['name'])
            wrt.writerow(entities)
            print(f'{counter} names parsed')


if __name__ == '__main__':
 
    conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='',
    password='',
    db='entityDB'
    )
    cur= conn.cursor()


    #cur.execute("CREATE TABLE `entityDB`.`cik_index` (`ID` INT NOT NULL AUTO_INCREMENT, `cik` INT NOT NULL, `entity_name` VARCHAR(45) NOT NULL, PRIMARY KEY (`ID`),UNIQUE INDEX `cik_UNIQUE` (`cik` ASC) VISIBLE);")

    #cur.execute('SHOW TABLES')
    entities =[]
    with open('../unzipped_indexes/index_cik', 'r') as index:
        data = csv.reader(index)
        for entity in data:
            entities.append((entity[0], entity[1]))

    print(entities[64][1])


    

    try:
        sql = 'SELECT * FROM entityDB.cik_index;'
        cur.execute(sql)
    except Exception as e:
        print(e)
    conn.commit()

    cur.close()
    conn.close()

'''
    start_time = time.time()    
    p = Pool()
    CIK = p.apply(create_cik_set)
    p.close()
    p1 = Pool()
    
    p1.map(parse_sec_by_cik, CIK)
    p1.close()
    p1.join()
    end_time = time.time() - start_time

    print(f'Processing {len(CIK)} ciks took {end_time} time using 8 cores')

''' 



  

