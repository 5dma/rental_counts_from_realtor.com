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


if (args.retrieve) == False:
	print("Not retrieving new data from network.")
else:
	print("Retrieving new data from network...")

sqlite_directory = '/tmp/rental'
sqlite_file = 'rental_counts.sqlite'
sqlite_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_file)
if not Path(sqlite_path).exists():
	print("The database {0} does not exist. Exiting.".format(str(sqlite_path)))
	sys.exit

# current_date is used in the file name of the backed up sqlite database as well as to generate the date added to the dates table.
current_date = datetime.today().strftime('%Y-%m-%d')
sqlite_backup_file = 'rental_counts.sqlite.{0}'.format(current_date)
sqlite_backup_path = pathlib.PurePath(sqlite_directory).joinpath(sqlite_backup_file)
copyfile(sqlite_path, sqlite_backup_path)

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
		print("{0}: The date {1} already exists in the dates table, not adding again".format(ex.__class__.__name__,current_date))
	
	RESPONSE_CODE_BAD_MIN = 400
	# Create a dictionary of URLs for each region-price point
	url_dict = {
		ALL_MOCO : {ANY: 'https://www.realtor.com/apartments/Montgomery-County_MD', MAX_2200: 'https://www.realtor.com/apartments/Montgomery-County_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Montgomery-County_MD/price-na-1500'},
		ROCKVILLE : {ANY: 'https://www.realtor.com/apartments/Rockville_MD', MAX_2200: 'https://www.realtor.com/apartments/Rockville_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Rockville_MD/price-na-1500'},
		WHEATON : {ANY: 'https://www.realtor.com/apartments/Wheaton_MD', MAX_2200: 'https://www.realtor.com/apartments/Wheaton_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Wheaton_MD/price-na-1500'},
		GAITHERSBURG : {ANY: 'https://www.realtor.com/apartments/Gaithersburg_MD', MAX_2200: 'https://www.realtor.com/apartments/Gaithersburg_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Gaithersburg_MD/price-na-1500'},
		GERMANTOWN : {ANY: 'https://www.realtor.com/apartments/Germantown_MD', MAX_2200: 'https://www.realtor.com/apartments/Germantown_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Germantown_MD/price-na-1500'},
		SILVER_SPRING : {ANY: 'https://www.realtor.com/apartments/Silver-Spring_MD', MAX_2200: 'https://www.realtor.com/apartments/Silver-Spring_MD/price-na-2200', MAX_1500: 'https://www.realtor.com/apartments/Silver-Spring_MD/price-na-1500'}
	}

	my_headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
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
				continue

			print("Fetching {0}".format(url))
			response = requests.get(url, headers=my_headers)
			if response.status_code > RESPONSE_CODE_BAD_MIN:
				print("Failed to retrieve {0}".format(myurl))
				print("Status code {0}",response.status_code)
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


