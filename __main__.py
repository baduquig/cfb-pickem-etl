"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import cfb_etl.cfb_extract.extract as x

def main():
    x.full_extract()

if __name__ == '__main__':
    main()
