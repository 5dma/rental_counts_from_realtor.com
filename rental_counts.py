from datetime import datetime
from datetime import date
#from bs4 import BeautifulSoup
#from scrapy.spiders import SitemapSpider
#from scrapy.crawler import CrawlerProcess
import sys
import sqlite3
import requests
#import argparse
#import validators
#import urllib.parse
import os
from shutil import copyfile
import pathlib
from pathlib import Path

def format_date(date_string):
	local_date = date.fromisoformat(date_string)
	formatted_date = local_date.strftime('%m/%y')
	return formatted_date

def format_price(price_string):
	match price_string:
		case 0:
			return "Any"
		case 2200:
			return "Max 2200"
		case 1500:
			return "Max 1500"
#parser = argparse.ArgumentParser(
                    #prog='Documentation Retriever',
                    #description='Retrieves all HTML files in Brightspot\'s documentation sitemaps.',
                    #)
#parser.add_argument('-c','--crawl',action='store_const',const=False, default=False)
#args = parser.parse_args()
#print("Running with following options:")
#print("* Crawl sitemaps: {}".format(args.crawl))
#print("")


RESPONSE_CODE_BAD_MIN = 400
url_list = []
url_filename = 'urls.txt'
url_list = ['https://www.realtor.com/apartments/Montgomery-County_MD']
for myurl in url_list:
	response = requests.get(myurl)
	if response.status_code > RESPONSE_CODE_BAD_MIN:
		print("Failed to retrieve {0}".format(myurl))
		sys.exit()
	print(response.text)
sys.exit()
current_date = datetime.today().strftime('%Y-%m-%d')
sqlite_directory = '/home/abba/maryland-politics/clean_slate_moco/rental_listings_rent_control'
sqlite_file = 'rental_counts.sqlite'

sqlite_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_file)
if not Path(sqlite_path).exists():
	print("The database {0} does not exist. Exiting.".format(str(sqlite_path)))
	sys.exit

sqlite_backup_file = 'rental_counts.sqlite.{0}'.format(current_date)
sqlite_backup_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_backup_file)
copyfile(sqlite_path, sqlite_backup_path)

con = sqlite3.connect(str(sqlite_path))
cur = con.cursor()
select_statement = 'SELECT date as "Date", region as "Region", upper_limit AS "Price Range", rental_count as "Rentals" FROM rental_counts JOIN dates ON date_id = dates.ID JOIN regions ON region_id = regions.ID JOIN price_ranges ON price_range_id = price_ranges.ID'
res = cur.execute(select_statement)
rental_counts = res.fetchall()

output_file = 'rental_counts.csv'
output_file_path = pathlib.PurePath(sqlite_directory).joinpath(output_file)
outfile = open(str(output_file_path),'w')
outfile.write("Date\tRegion\tPrice Range\tRentals\n")

rental_dict = {}
for rental_count in rental_counts:
	date_string = format_date(rental_count[0])
	if date_string not in rental_dict:
		rental_dict[date_string] = {}

	region_string = rental_count[1]
	if region_string not in rental_dict[date_string]:
		rental_dict[date_string][region_string] = {}

	price_string = format_price(rental_count[2])
	rental_dict[date_string][region_string][price_string] = rental_count[3]
	#row_string="{0}\t{1}\t{2}\t{3}\n".format(date_string,rental_count[1], price_string, rental_count[3])
	#outfile.write(row_string)
outfile.write("Date\tRegion\tAny\tMax 2200\tMax 1500\n")

for key1, value1 in rental_dict.items():
	for key2, value2 in value1.items():
		row_string="{0}\t{1}\t{2}\t{3}\t{4}\n".format(key1,key2,value2['Any'],value2['Max 2200'],value2['Max 1500'])
		
		#for key3, value3 in value2.items():
		#	row_string="{0}\t{1}\t{2}\t{3}\n".format(key1,key2,key3,value2[key3])
		outfile.write(row_string)



outfile.close()
sys.exit



'''
if args.crawl == True:
	print("Crawling sitemaps...")
	c = CrawlerProcess({
	    'USER_AGENT': 'Mozilla/5.0',
		'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
		'format': 'csv',
	})
	c.crawl(MySpider)
	c.start()
	url_list.sort()

	# Save the URLS in a temporary file.
	url_file = open(url_filename,"w")
	for myurl in url_list:
		url_file.write("{0}\n".format(myurl))
	url_file.close()
else:
	print("Reading URLs from previously crawled sitemaps...")
	url_file = None
	try:
		url_file = open(url_filename,"r")
		data = url_file.read()
		url_list = data.split("\n")
		url_file.close()
	except FileNotFoundError as e:
		print("Exception occurred while reading the file {0}; {1}".format(url_filename,type(e).__name__))
		sys.exit()

	except Exception as e:
		print("Exception occurred: {0}".format(type(e).__name__))
		sys.exit()

ignored_urls = ['https://www.brightspot.com/documentation/', 'https://www.brightspot.com/documentation/4-2-x-x', 'https://www.brightspot.com/documentation/4-5-x-x', 'https://www.brightspot.com/documentation/4-7-releases']

headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}


dirpath = 'xml_extracts'
if os.path.exists(dirpath):
	shutil.rmtree(dirpath, ignore_errors=True)
os.mkdir(dirpath)
'''


