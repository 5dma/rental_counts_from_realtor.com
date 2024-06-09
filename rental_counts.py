from datetime import datetime
from datetime import date
import time
from bs4 import BeautifulSoup
import sys
import sqlite3
import requests
import argparse
from shutil import copyfile
import pathlib
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import logging


# Source: https://matplotlib.org/stable/gallery/lines_bars_and_markers/barchart.html#sphx-glr-gallery-lines-bars-and-markers-barchart-py

def make_plot(periods, region, rental_counts):
	'''Generates a bar graph for the passed region. The x-axis contains dates, the y-axis auto-scales for the number of units.'''

	cmap = mpl.colors.ListedColormap(["#004586", "#FF420E", "#FFD320"])

	df = pd.DataFrame(rental_counts, periods)
	df.plot.bar(colormap=cmap, sharex = True, sharey = True, figsize=(8,4), ylabel="# listings", title = region, rot = 0)
	plt.legend(loc='lower center', ncols=3, bbox_to_anchor=(0.5,-.20,0,0))
	plt.savefig('/tmp/{0}.png'.format(region), bbox_inches='tight')

def format_date(date_string):
	'''Returns a passed date_string in YYYY-MM-DD format in MM/YY format.'''
	local_date = date.fromisoformat(date_string)
	formatted_date = local_date.strftime('%m/%y')
	return formatted_date


def return_primary_key(cur, table_name,field_name,value):
	'''Returns the primary key in a table corresponding to the passed value in a field name (column).'''
	select_statement = 'SELECT ID FROM {0} WHERE {1} = "{2}"'.format(table_name,field_name,value)
	res = cur.execute(select_statement)
	return res.fetchone()[0]

parser = argparse.ArgumentParser(
                    prog='Rental Counter',
                    description='Generates bar graphs for rental inventory at various price breaks, by region',
                    )
parser.add_argument('-r','--retrieve',action='store_const',const=True, default=False,help='Retrieves new data from realtor.com')
args = parser.parse_args()



sqlite_directory = '/home/abba/maryland-politics/clean_slate_moco/rental_listings_rent_control'
sqlite_file = 'rental_counts.sqlite'
log_file = 'rent_control.log'
sqlite_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_file)
logging_path = pathlib.PurePath(sqlite_directory).joinpath(log_file)
logging.basicConfig(filename=logging_path,level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Starting")

if (args.retrieve) == False:
	print("Not retrieving new data from network.")
	logging.info("Not retrieving new data from network.")
else:
	print("Retrieving new data from network...")
	logging.info("Retrieving new data from network...")

if not Path(sqlite_path).exists():
	path_message = "The database {0} does not exist. Exiting.".format(str(sqlite_path))
	print(path_message)
	logging.critical(path_message)
	sys.exit()

# current_date is used in the file name of the backed up sqlite database as well as to generate the date added to the dates table.
current_date = datetime.today().strftime('%Y-%m-%d')
sqlite_backup_file = 'rental_counts.sqlite.{0}'.format(current_date)
sqlite_backup_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_backup_file)
copyfile(sqlite_path, sqlite_backup_path)
logging.info("Created backup of sqlite database.")

con = sqlite3.connect(str(sqlite_path))
con.row_factory = sqlite3.Row
cur = con.cursor()


# String constants for regions and price points. Would probably be better to extract these from the database.
ALL_MOCO = 'All MoCo'
ROCKVILLE = 'Rockville'
WHEATON = 'Wheaton'
GAITHERSBURG = 'Gaithersburg'
GERMANTOWN = 'Germantown'
SILVER_SPRING = 'Silver Spring'
ANY = 'Any'
MAX_2200 = 'Max 2200'
MAX_1500 = 'Max 1500'

# If user passed the -r flag, screen scrape the web site to get inventory counts for each region-price point.
if args.retrieve == True:

	saved_html_path = pathlib.PurePath(sqlite_directory).joinpath(current_date)
	if not Path(saved_html_path).exists():
		Path(saved_html_path).mkdir()
		

	insert_statement = 'INSERT INTO dates (date) VALUES ("{0}")'.format(current_date)
	try:
		res = cur.execute(insert_statement)
		con.commit()
	except Exception as ex:
		date_message = "{0}: The date {1} already exists in the dates table, not adding again".format(ex.__class__.__name__,current_date)
		print(date_message)
		logging.warning(date_message)
	
	RESPONSE_CODE_BAD_MIN = 400
	# Create a dictionary of URLs for each region-price point
	url_dict = {
		ALL_MOCO : {ANY: 'https://www.realtor.com/apartments/Montgomery-County_MD?view-map', MAX_2200: 'https://www.realtor.com/apartments/Montgomery-County_MD/price-na-2200?view-map', MAX_1500: 'https://www.realtor.com/apartments/Montgomery-County_MD/price-na-1500?view-map'},
		ROCKVILLE : {ANY: 'https://www.realtor.com/apartments/Rockville_MD?view=map', MAX_2200: 'https://www.realtor.com/apartments/Rockville_MD/price-na-2200?view=map', MAX_1500: 'https://www.realtor.com/apartments/Rockville_MD/price-na-1500'},
		WHEATON : {ANY: 'https://www.realtor.com/apartments/Wheaton_MD?view=map', MAX_2200: 'https://www.realtor.com/apartments/Wheaton_MD/price-na-2200?view=map', MAX_1500: 'https://www.realtor.com/apartments/Wheaton_MD/price-na-1500'},
		GAITHERSBURG : {ANY: 'https://www.realtor.com/apartments/Gaithersburg_MD?view=map', MAX_2200: 'https://www.realtor.com/apartments/Gaithersburg_MD/price-na-2200?view=map', MAX_1500: 'https://www.realtor.com/apartments/Gaithersburg_MD/price-na-1500'},
		GERMANTOWN : {ANY: 'https://www.realtor.com/apartments/Germantown_MD?view=map', MAX_2200: 'https://www.realtor.com/apartments/Germantown_MD/price-na-2200?view=map', MAX_1500: 'https://www.realtor.com/apartments/Germantown_MD/price-na-1500'},
		SILVER_SPRING : {ANY: 'https://www.realtor.com/apartments/Silver-Spring_MD?view=map', MAX_2200: 'https://www.realtor.com/apartments/Silver-Spring_MD/price-na-2200?view=map', MAX_1500: 'https://www.realtor.com/apartments/Silver-Spring_MD/price-na-1500'}
	}

	my_headers= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0', 'Host': 'www.realtor.com'}
	#my_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8', 'Accept-Encoding': 'gzip, deflate, br', 'Connection': 'keep-alive', 'Cookie':'split=n; split_tcv=155; __vst=f2cd5be0-2637-4237-844c-9ad53fff377b; __ssn=438966f0-3df1-4855-9324-530f30d15846; __ssnstarttime=1697669984; __bot=false; AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C19649%7CMCMID%7C64173209909886957638697908199963201878%7CMCAID%7CNONE%7CMCOPTOUT-1697677189s%7CNONE%7CvVersion%7C5.2.0; __split=94; criteria=sprefix%3D%252Fnewhomecommunities%26area_type%3Dcounty%26pg%3D1%26state_code%3DMD%26state_id%3DMD%26loc%3DMontgomery%2520County%252C%2520MD%26locSlug%3DMontgomery-County_MD; _rdt_uuid=1697669987649.1003ba27-b7c9-465b-a66f-2a4b34107c08; _scid=4a96d1d3-f81d-4676-bdc7-1b2cad8fc2dc; _scid_r=4a96d1d3-f81d-4676-bdc7-1b2cad8fc2dc; ab.storage.userId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22visitor_f2cd5be0-2637-4237-844c-9ad53fff377b%22%2C%22c%22%3A1697669987696%2C%22l%22%3A1697669987697%7D; ab.storage.sessionId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%224017c338-09f4-54fc-ee0a-d9466246a8c5%22%2C%22e%22%3A1697671787702%2C%22c%22%3A1697669987696%2C%22l%22%3A1697669987702%7D; ab.storage.deviceId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22125a207f-4c89-7b05-7bec-e9b3b3741124%22%2C%22c%22%3A1697669987698%2C%22l%22%3A1697669987698%7D; permutive-id=a0255696-ab61-4883-8da0-a963ee7f5403; _gcl_au=1.1.1377085234.1697669988; _ga_MS5EHT6J6V=GS1.1.1697669988.1.0.1697669988.60.0.0; _ga=GA1.1.1305600340.1697669988; G_ENABLED_IDPS=google; _tac=false~self|not-available; _ta=us~1~c633ec0672eeb80ac2319af62d9003ff; _tas=4w0yv7imi2e; s_ecid=MCMID%7C64173209909886957638697908199963201878; pxcts=09359fd0-6e0a-11ee-b466-27f14756328c; _pxvid=09359340-6e0a-11ee-b466-1fe7b9389d05; AMCVS_8853394255142B6A0A4C98A4%40AdobeOrg=1; ajs_anonymous_id=190e4357-aa7e-4451-97b1-1f363d0de375; mdLogger=false; kampyle_userid=5446-f298-da01-efa9-10b9-79cb-0720-46d2; kampyleUserSession=1697669989258; kampyleSessionPageCounter=1; kampyleUserSessionsCount=1; kampyleUserPercentile=52.71595499200619; _iidt=HHrKPqXgN9SlLzr8C2qlHUrGHMikiBws3PJ8rFI3mpsSXjXwMWIllXjCZo8y/hjlxk8oNskVSYAsZw==; _vid_t=3FhjrZvdukKO9DbaPli0qPpCxRUZoEaBrlxo0oED7xWHkEc/+7Xb2aUUSYTJAJBYXmr39G4+3zvo6w==; __fp=grI8uGx9x8wefY7sIkkY; _px3=9c7fbe2cb42e6f3327248249b7115d0b381cdbfa37582c6294bfef3a23c046e0:6vO9G7QIFuOnA/XfG72dn9HpHFqs7i8e+LGp7jXJQF/YsftQq8fnM35ia8zypakoGFJeBzptyywZgc73DgOGOg==:1000:5k0FJ7ivr7n1VQ2WAN5j2bRdHFgipgF6J6wrsEW/8gecnUmF+jY/j7M5DYRZ2UuqEuhDAdp88yiwDgFqNGwMCVRnOWa+W3Bq42PRJYNHZgoOgRleWcmQVXO1k6QakGxF/ky4SgJnrFek7XmHzQMD4a4AQ078tlr6xe1AZmXAW4ztjVK2X+xYD/TqIMILsHNn9FXKhhpVe+dBheQsUVWyQze8Z/iMfkZeW1c40XXnPpg=; _lr_sampling_rate=0; _lr_retry_request=true; _lr_env_src_ats=false', 'DNT': '1'}
	# For each region and price point, go retrieve the web page.
	for region, value1 in url_dict.items():
		for price_range, url in value1.items():

			date_id = return_primary_key(cur,'dates','date',current_date)
			region_id = return_primary_key(cur,'regions','region',region)
			price_range_id = return_primary_key(cur,'price_ranges','upper_limit',price_range)

			# Check if an entry exists for the current date, region, and price point. If so, do not add a duplicate such entry.
			sel = "SELECT ID FROM rental_counts WHERE date_id={0} AND region_ID={1} AND price_range_id={2}".format(date_id, region_id, price_range_id) 
			res = cur.execute(sel)
			rental_counts = res.fetchone()
			if rental_counts is not None:
				select_message = "An entry already exists for date_id={0} AND region_ID={1} AND price_range_id={2}, skipping".format(date_id, region_id, price_range_id)
				print(select_message)
				logging.warning(select_message)
				continue

			print("Fetching {0}".format(url))
			logging.info("Fetching {0}".format(url))
			response = requests.get(url, headers=my_headers)
			if response.status_code > RESPONSE_CODE_BAD_MIN:
				print("Failed to retrieve {0}".format(url))
				print("Status code {0}".format(response.status_code))
				logging.critical("Failed to retrieve {0}".format(url))
				logging.critical("Status code {0}".format(response.status_code))
				sys.exit()
	
		
			# Create a file name with format region_price range.html, and save in the directory for today's date.
			html_filename = (region + "_" + price_range + ".html").replace(' ','_')
			html_path = pathlib.PurePath(saved_html_path).joinpath(html_filename)
			html_handle = open(html_path,'w')
			html_handle.write(response.text)
			html_handle.close()

			# Parse the retrieved HTML file for the number of units, and save it in the database.
			soup = BeautifulSoup(response.text,'html.parser')
			rental_number_text = soup.find('div',{'data-testid': 'total-results'}).text
			rental_number = int(rental_number_text.replace(',', ''))
			insert_statement = 'INSERT INTO rental_counts (date_id, region_id, price_range_id, rental_count) VALUES ({0},{1},{2},{3})'.format(date_id, region_id, price_range_id, rental_number)
			res = cur.execute(insert_statement)
			con.commit()
			time.sleep(60)


# Extract all the data from the database and save in a file rental_counts.csv. This CSV file is for backup purposes only.
select_statement = 'SELECT date, region, upper_limit, rental_count FROM rental_counts JOIN dates ON date_id = dates.ID JOIN regions ON region_id = regions.ID JOIN price_ranges ON price_range_id = price_ranges.ID'
res = cur.execute(select_statement)
rental_counts = res.fetchall()

rental_dict = {}
for rental_count in rental_counts:
	date_string = format_date(rental_count['date'])
	if date_string not in rental_dict:
		rental_dict[date_string] = {}

	region_string = rental_count['region']
	if region_string not in rental_dict[date_string]:
		rental_dict[date_string][region_string] = {}

	price_string = rental_count['upper_limit']
	rental_dict[date_string][region_string][price_string] = rental_count['rental_count']

output_file = 'rental_counts.csv'
output_file_path = pathlib.PurePath(sqlite_directory).joinpath(output_file)
outfile = open(str(output_file_path),'w')
outfile.write(f"Date\tRegion\t{ANY}\t{MAX_2200}\t{MAX_1500}\n")

for key1, value1 in rental_dict.items():
	for key2, value2 in value1.items():
		row_string="{0}\t{1}\t{2}\t{3}\t{4}\n".format(key1,key2,value2[ANY],value2[MAX_2200],value2[MAX_1500])
		outfile.write(row_string)
outfile.close()

# Select inventory data by month, by region, by price point. The rental inventory is averaged for each month to minimize variability.
select_statement = "SELECT substring(date,6,2) || '/' || substring(date,3,2) as mydate, region, upper_limit, AVG(rental_count) as average_rent FROM rental_counts JOIN dates ON date_id = dates.ID JOIN regions ON region_id = regions.ID JOIN price_ranges ON price_range_id = price_ranges.ID GROUP BY mydate, region,upper_limit ORDER BY mydate,region,price_ranges.ID"

data_frame = {}
periods = []
res = cur.execute(select_statement)
data_table = res.fetchall()

# Create a dictionary by date, region, price point, with value a list of rental quantities.
for row in data_table:
	if (row['mydate'] not in periods):
		periods.append(row['mydate'])
	if (row['region'] not in data_frame):
		data_frame[row['region']] = {}
	if row['upper_limit'] not in data_frame[row['region']]:
		data_frame[row['region']][row['upper_limit']] = []
	data_frame[row['region']][row['upper_limit']].append(row['average_rent'])
		
# Create a plot for each entry in the dictionary
for region,rental_counts in data_frame.items():
	make_plot(periods, region, rental_counts)

con.close()
print("All done!")
logging.info("All done!")


