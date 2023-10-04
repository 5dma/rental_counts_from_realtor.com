from datetime import datetime
#from bs4 import BeautifulSoup
#from scrapy.spiders import SitemapSpider
#from scrapy.crawler import CrawlerProcess
import sys
#import requests
#import argparse
#import validators
#import urllib.parse
import os
#import shutil
import pathlib
from pathlib import Path


#parser = argparse.ArgumentParser(
                    #prog='Documentation Retriever',
                    #description='Retrieves all HTML files in Brightspot\'s documentation sitemaps.',
                    #)
#parser.add_argument('-c','--crawl',action='store_const',const=False, default=False)
#args = parser.parse_args()
#print("Running with following options:")
#print("* Crawl sitemaps: {}".format(args.crawl))
#print("")


current_date = datetime.today().strftime('%Y-%m-%d')
sqlite_directory = '/home/abba/maryland-politics/clean_slate_moco/rental_listings_rent_control'
sqlite_file = 'rental_counts.sqlite'
sqlite_backup_file = 'rental_counts.sqlite.backup'

sqlite_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_file)
if not Path(sqlite_path).exists():
	print("The database {0} does not exist. Exiting.".format(str(sqlite_path)))
else:
	print("found")
sys.exit


RESPONSE_CODE_BAD_MIN = 400
url_list = []
url_filename = 'urls.txt'

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


