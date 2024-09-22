"""
Pickem ETL
Author: Gabe Baduqui

Scrape Team color codes from https://teamcolorcodes.com/
"""
import requests, json
from bs4 import BeautifulSoup

custom_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

team_colors = {}
color_codes_home = "https://teamcolorcodes.com/ncaa-color-codes/"
response = requests.get(color_codes_home)
soup = BeautifulSoup(response.text, 'html.parser')
entry_content_div = soup.find('div', class_='entry-content')

for team_link in entry_content_div.find_all('a'):
    team_url = team_link['href']
    team_response = requests.get(team_url, headers=custom_header)
    team_soup = BeautifulSoup(team_response.text, 'html.parser')

    color_blocks = team_soup.find_all('div', class_='colorblock')
    colors = []
    for block in color_blocks[:2]:
        block_text = block.text
        start_idx = block_text.find('#')
        end_idx = start_idx + 7
        hex_color = block_text[start_idx:end_idx].lstrip()
        colors.append(hex_color)

    team_colors[team_url[27:]] = colors


with open('./pickem_data/cfb_team_colors.json', 'w') as team_colors_file:
    json.dump(team_colors, team_colors_file, indent=4)



"""
def create_team_color_code_url(team_name: str, team_mascot: str, logfile: object):
    Function that creates the string for the given team's color code page url
       Accepts `team_name`: String, `team_mascot`: String, `logfile`: File Object
       Returns `team_color_codes_url`: String
    team_route = f'{team_name.lstrip().rstrip().replace(" ", "-")}-{team_mascot.lstrip().rstrip().replace(" ", "-")}'
    team_color_codes_url = f'https://teamcolorcodes.com/{team_route}-color-codes/'
    logfile.write(f'team_color_codes_url: {team_color_codes_url}')
    return team_color_codes_url

def extract_hex_code(color_block_div: str, logfile: object):
    Function that extracts the hex color code from the color block DIV
       Accepts `color_block_div`: HTML String, `logfile`: File Object
       Returns `hex_color_code`: String
    logfile.write(f'hex_color_code: ')
    try:
        start_idx = color_block_div.lower().find('hex color: ') + 11
        end_idx = start_idx + 8
        hex_color_code = color_block_div[start_idx:end_idx]
        logfile.write(f'{hex_color_code}\n')
    except Exception as e:
        hex_color_code = ''
        logfile.write(f'{e}\n')
    return hex_color_code    

def get_team_colors(team_name: str, team_mascot: str, logfile: object):
    #Function that scrapes and returns the hex color code for a given team
       Accepts `team_name`: String, `team_mascot`: String, `logfile`: File Object
       Returns `primary_color`: String, `secondary_color`: String, `accent_color`: String
    logfile.write(f'Retrieving color codes for {team_name}, {team_mascot}\n')
    try:
        team_color_codes_url = create_team_color_code_url(team_name, team_mascot, logfile)
        color_codes_resp = requests.get(team_color_codes_url, headers=ex.custom_header)
        color_codes_soup = BeautifulSoup(color_codes_resp.content, 'html.parser')

        primary_color_block = color_codes_soup.find('div', class_='entry-content').find_all('div', class_='colorblock')[0].text
        secondary_color_block = color_codes_soup.find('div', class_='entry-content').find_all('div', class_='colorblock')[1].text
        accent_color_block = color_codes_soup.find('div', class_='entry-content').find_all('div', class_='colorblock')[2].text

        primary_color = extract_hex_code(primary_color_block, logfile)
        secondary_color = extract_hex_code(secondary_color_block, logfile)
        accent_color = extract_hex_code(accent_color_block, logfile)

        logfile.write(f'Primary Color Code: {primary_color}\nSecondary Color Code: {secondary_color}\nAccent Color Code: Color Code: {accent_color}')
    except Exception as e:
        primary_color = ''
        secondary_color = ''
        accent_color = ''
        print(f'Error retrieving team {team_name} {team_mascot} color codes: {e}')
        logfile.write(f'Error retrieving team color codes: \n{e}\n\n')
    return primary_color, secondary_color, accent_color
"""