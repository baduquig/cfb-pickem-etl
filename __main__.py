"""
CFB Pickem ETL
Author: Gabe Baduqui

Scrape, transform and load college football schedule data from various web pages
"""
import etl.extract as ex

def main():
    ex.full_extract()

if __name__ == '__main__':
    main()
