# Imports
from operator import pos
from os import link
from urllib.request import urlopen
from bs4 import BeautifulSoup
import concurrent.futures
import mwparserfromhell
import wikitextparser
import traceback
import requests
import pymongo
import json
import uuid
import time
import re

# Read file
login_info = json.load(open("login.json"))

# Get login info from login file
ADDRESS = login_info["address"]
PORT = login_info["port"]
USERNAME = login_info["username"]
PASSWORD = login_info["password"]

# OTHER HYPERPARAMETERS
DOMAIN = "https://www.cpdl.org"
TR_KEYS = ["text", "linktext", "translation"]

T_AND_T = ['text', 'original text and translations', 'text and translations', 'texts and translations']

# Connect to MongoDB, whilst checking if the login file specified localhost
if ADDRESS.lower() == "localhost":
    client = pymongo.MongoClient(f"mongodb://localhost/")
else:
    client = pymongo.MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@{ADDRESS}:{PORT}/")

# Get all databases and the targeted collection
db = client['VIVY']
col = db['cpdlCOL']

def get_page_by_id(page_id):
    url = f'https://cpdl.org/wiki/api.php?action=query&prop=info&inprop=url&pageids={page_id}&format=json'
    with urlopen(url) as f:
        d = json.loads(f.read())['query']['pages'][f'{page_id}']
    assert 'missing' not in d
    return d

def redirect(title):
    url = f'https://cpdl.org/wiki/api.php?action=query&titles={title}&redirects=1&format=json'
    with urlopen(url) as f:
        page = list(json.loads(f.read())['query']['pages'].values())[0]
    assert 'missing' not in page
    page_id = page['pageid']
    return get_page_by_id(page_id)

# parse function for `text and translations`
def parse_text(wikitext_raw):
    out = {}
    # Parse JSON response for the text with wikitext parser
    text = mwparserfromhell.parse(wikitext_raw)
    
    # Get templates from parsed wikitext
    templates = text.filter_templates(recursive=False)

    # Iterate through the templates
    for index, item in enumerate(templates):
        # Check if the iterate item is one of the three allowed keys
        key = str(item.name.lower())
        if key in TR_KEYS:
            # Add translation to translation dictionary
            if key not in out: out[key] = []
            out[key].extend([str(i) for i in item.params])
    
    return out

# Scrape function
def scrape(link, text_only=False):
    '''
        params:
            link        

            text_only   
                `greneral_data` won't be computed and will returns `None` if `text_only` is `True`. 
        returns:
            Tuple (`greneral_data` | `None`, `text_and_translation`)
    '''
    # Try
    try:
        
        # Variable declaration
        gen_info = {}
        translations = {}
        downloads = {}

        # Fetch title of the page
        title = link.replace("/wiki/index.php/", "")
        
        urls = redirect(title)
        url, editurl = urls['fullurl'], urls['editurl']
        
        # Print link
        # print('-', link)

        # Get page info and bs4 instance based on given link
        link_html = urlopen(url)
        page_soup = BeautifulSoup(link_html, "lxml")
        
        # Make a UUID for the link
        id = uuid.uuid4().hex
        
        # Get page's source page
        wikitext_soup = BeautifulSoup(urlopen(editurl), "lxml")
        
        # Get the text of wikitext
        general_wikitext = wikitextparser.parse(wikitext_soup.find("textarea", attrs={"id": "wpTextbox1"}).text)
        
        # Generate a list of titles
        indexes = [str(item.title).lower() for item in general_wikitext.get_sections(level=2)]
        #
        #   TRANSLATION
        #
        # Get wikitext data from given page
        wikitext_raw = None
        for t_and_t in T_AND_T:
            if t_and_t in indexes:
                wikitext_raw = str(general_wikitext.get_sections(level=2)[indexes.index(t_and_t)])
        if wikitext_raw is None: raise f'No text or translations in the page: {link}'
        
        translations = parse_text(wikitext_raw)
        
        if 'linktext' in translations:
            del translations['linktext']
            # find the main `div`
            main_content = page_soup.find(id='mw-content-text').find(class_='mw-parser-output')

            # split it by `h2`
            content_dict = {}
            _current_key = ''
            for child in main_content.children:
                if len(child.text.strip()) == 0: continue
                if child.name == 'h2':
                    _current_key = child.text.strip().lower()
                else:
                    if _current_key not in content_dict:
                        content_dict[_current_key] = []
                    content_dict[_current_key].append(child)

            # get the soup object of text and translations
            translations_soups = None
            for t_and_t in T_AND_T:
                if t_and_t in content_dict: 
                    translations_soups = content_dict[t_and_t]
            assert translations_soups is not None

            # get the all `a`
            links = []
            for translations_soup in translations_soups:
                a_list = translations_soup.find_all('a')
                for a in a_list:
                    flag = True
                    if '(page does not exist)' in a['title']: 
                        flag = False
                    if flag: links.append(a['href'])
            
            for lk in links:
                # scrape text in the links
                _, translations_ = scrape(lk, text_only=True)
                # append them into `translations`
                for key in TR_KEYS:
                    if key not in translations: translations[key] = []
                    translations[key].extend(translations_.get(key, []))

            # delete useless keys
            keys = list(translations.keys())
            for key in keys:
                if len(translations[key]) == 0:
                    del translations[key]
        
        # Sleep for a bit
        time.sleep(1)

        if text_only:
            return None, translations
                
        #
        #   GENERAL INFORMATION
        #
        
        # Get wikitext data from given page
        wikitext_raw = str(general_wikitext.get_sections(level=2)[indexes.index("general information")])

        # Parse JSON response for the text with wikitext parser
        text = mwparserfromhell.parse(wikitext_raw)
        
        # Get templates from parsed wikitext
        templates = text.filter_templates(recursive=False)
        
        # Iterate through the templates
        for index, item in enumerate(templates):
            # Check if key is in the general information dictionary
            if str(item.name.lower()) in gen_info:
                # If so, add value to the list of that key
                gen_info[str(item.name.lower())].append(
                    ", ".join([information_parse(str(i)
                                                 .replace("''","")
                                                 .replace("[[","")
                                                 .replace("]]","")) for i in item.params]))
                
            # If not,
            else:
                # Create a key and create a list as a value with appended value
                gen_info[str(item.name.lower())] = [
                    ", ".join([information_parse(str(i)
                                                 .replace("''","")
                                                 .replace("[[","")
                                                 .replace("]]","")) for i in item.params])]

        #
        #   DOWNLOAD LINKS
        #
        
        # Get al 'li' tags
        possible_starters = page_soup.find_all('li')
        
        # Iterate through all tags
        for tag in possible_starters:
            # Check if the tag has "CPDL #" in it
            if tag.find(text=re.compile("CPDL #")):
                # Regex find the CPDL #
                cpdl_num = re.search("(?<=\#)(.*?)(?=\:)", tag.text).group(0)
                
                # Create index in download dictionary
                downloads[str(cpdl_num)] = []
                
                # Find a tags with href                
                for item in tag.find_all('a', href=True):
                    # Append link to download category
                    downloads[str(cpdl_num)].append(item.get('href') if 'http' in item.get('href') else DOMAIN + item.get('href'))        
        
        #
        #   DATA COMPILING
        #
        # Construct a dictionary for the information to be inserted into DB
        data = {
            "_id": id,
            "link": link,
            "title": title,
            "composer": gen_info['composer'][0],
            "general_information": gen_info,
            "download_links": downloads
        }
        return data, translations
    # Except
    except Exception as e:
        # Print status
        print(f"One link not scraped >>> {link}")
        
        traceback.print_exc()
        # Log error
        with open(f'logs/{uuid.uuid4().hex}.txt', 'w+') as file:
            traceback.print_exc(file=file)
        
def scrape_song(link):
    print(link)
    insert_data, translations = scrape(link)
    # Try
    try:
        # Construct a dictionary for the information to be inserted into DB
        insert_data['translations'] = translations
        # Insert to collection 
        col.insert_one(insert_data)
    # Except
    except Exception as e:
        # Print status
        print(f"One link not added >>> {link}")
        # Log error
        with open(f'logs/{uuid.uuid4().hex}.txt', 'w+') as file:
            traceback.print_exc(file=file)


# Recursive wikitext parser
def information_parse(value):
    # Variable declaration
    information = []
    
    # Create a parse instance from given value
    text = mwparserfromhell.parse(value)
    
    # Parse for templates
    templates = text.filter_templates()
    
    # Check if templates is empty
    if len(templates):
        # Iterate through all templates
        for item in templates:
            # Append parameters to information list
            information.append(" ".join([str(i) for i in item.params]))
            
        # Return information list
        return(" ".join([str(i) for i in information]))

    # Else, return the given value
    return value


# Main thread
if __name__ == '__main__':
    # var
    var = 0
    
    # Iterate 39 times
    for page in range(0, 4100, 100):       
        # Make a list of links for a given page
        list_of_links = []
        
        # If i is 0, get html data of the index pages
        if not page:
            response = urlopen(f"https://www.cpdl.org/wiki/index.php/ChoralWiki:Score_catalog")
        # If not, get the corresponding pages
        else:
            response = urlopen(f"https://www.cpdl.org/wiki/index.php/ChoralWiki:Score_catalog/{page}")
        
        # Make bs4 instance from html data
        soup = BeautifulSoup(response, "lxml")
        
        # Get the div that contains all composer-work items
        focus_div = soup.find("div", attrs={"class": "mw-parser-output"})
        
        # Get all divs that contain only works
        list_of_all_works = focus_div.find_all("div")
        
        # Iterate through each div tag to get a list of composer's work
        for composer in list_of_all_works:
            # Iterate through each "a" element and get href of it
            for index, item in enumerate(composer.find_all("a")):
                # Add each link to list of links
                list_of_links.append(item.get("href"))
        
        # for index, i in enumerate(list_of_links):        
        #     print(f"{index + 1} / {len(list_of_links)}")
        #     scrape(i)
                
        # Run scraping method
        with concurrent.futures.ProcessPoolExecutor() as executor:
            _ = [executor.submit(scrape_song, link) for link in set(list_of_links)]

        # Print next page
        print(f"-= NEXT PAGE {page}/4000 =-")
