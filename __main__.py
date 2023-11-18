"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import etl.cfb.extract.extract as ext

def main():
    ext.full_extract(year=2023, weeks=15)

if __name__ == '__main__':
    main()
