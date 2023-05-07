from datetime import datetime

class ExtractAll:
    """This class contains methods needed to scrape data from more than one source/webpage."""
    def __init__(self):
        self.logfile = open('./logs/scrape_all_' + datetime.now().strftime('%Y.%m.%d.%H.%M.%S') + '.log', 'a')
                
    def get_cell_text(self, td_html_str):
        """Method to extract the innerHTML text of the child tag of a given table cell."""
        try:
            cell_text = td_html_str.contents[0].text
        except:
            cell_text = ''
        return cell_text
    
    def get_school_id(self, school_span):
        """Method to extract SchoolID from the URL in the underlying href attribute."""
        try:
            href_str = school_span.find('a', href=True)['href']
            begin_index = href_str.index('/id/') + 4
            end_index = href_str.rfind('/')
            school_id = href_str[begin_index:end_index]
        except:
            school_id = '0'
        return school_id
    
    def get_game_id(self, td_tag_str):
        """Method to extract GameID from the URL in the underlying href attribute."""
        href_str = td_tag_str.find('a', href=True)['href']        
        game_id_index = href_str.index('gameId=') + 7
        game_id = href_str[game_id_index:]
        return game_id

    def get_logo_url(self, school_id):
        """Method to extract string value of HREF attribute of a given school (ID)"""
        try:
            logo_url = f'https://a.espncdn.com/combiner/i?img=/i/teamlogos/ncaa/500/{school_id}.png&h=2000&w=2000'
        except:
            logo_url = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/ncaa/500/4.png&h=2000&w=2000'
        return logo_url
    
    def cfb_etl_log(self, message):
        """Method to print logging message to log file and terminal"""
        self.logfile.writelines(message)
        print(message)

