* Scrapes available rental inventory at various price points from realtor.com, adds data to a database. 
* Extracts data from the database into a CSV file.

Requires the following:
* Python 3.10 (maybe you could get away with Python 3.9)
* Sqlite database property set up. Send me an email or message and I'll providate the `.schema` commands.

To run the script:

`python python rental_counts.py` -- Extracts data from database into CSV file without accumulating new data.
`python python rental_counts.py -r` -- Accumulates new data for the current date, and then extracts data from database into CSV file.

