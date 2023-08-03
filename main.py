# Imports
from urllib.request import urlopen
from bs4 import BeautifulSoup
import concurrent.futures
import mwparserfromhell
import wikitextparser
import traceback
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

# Connect to MongoDB, whilst checking if the login file specified localhost
if ADDRESS.lower() == "localhost":
    client = pymongo.MongoClient("mongodb://localhost/")
else:
    client = pymongo.MongoClient(
        f"mongodb://{USERNAME}:{PASSWORD}@{ADDRESS}:{PORT}/"
    )

# Get all databases and the targeted collection
db = client["VIVY"]
col = db["cpdlCOL"]


# Scrape function
def scrape(link):
    # Try
    try:
        # Variable declaration
        gen_info = {}
        translations = {}
        downloads = {}

        # Get page info and bs4 instance based on given link
        link_html = urlopen(DOMAIN + link)
        page_soup = BeautifulSoup(link_html, "lxml")

        # Fetch title of the page
        title = link.replace("/wiki/index.php/", "")

        # Print link
        print(link)

        # Get page's source page
        wikitext_soup = BeautifulSoup(
            urlopen(
                "https://www.cpdl.org/wiki/index.php?title="
                + f"{title}&action=edit"
            ),
            "lxml",
        )

        # Get the text of wikitext
        general_wikitext = wikitextparser.parse(
            wikitext_soup.find("textarea", attrs={"id": "wpTextbox1"}).text
        )

        # Generate a list of titles
        indexes = [
            str(item.title).lower() for item in general_wikitext.sections
        ]

        # try:
        #     a = indexes.index("general information")
        # except:
        #     input(indexes)

        #
        #   GENERAL INFORMATION
        #   region
        #

        # Get wikitext data from given page
        wikitext_raw = str(
            general_wikitext.sections[indexes.index("general information")]
        )

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
                    ", ".join(
                        [
                            information_parse(
                                str(i)
                                .replace("''", "")
                                .replace("[[", "")
                                .replace("]]", "")
                            )
                            for i in item.params
                        ]
                    )
                )

            # If not,
            else:
                # Create a key and create a list as a value with appended value
                gen_info[str(item.name.lower())] = [
                    ", ".join(
                        [
                            information_parse(
                                str(i)
                                .replace("''", "")
                                .replace("[[", "")
                                .replace("]]", "")
                            )
                            for i in item.params
                        ]
                    )
                ]

            # params = item.params
            # debug = ", ".join([information_parse(str(i)) for i in params])
            # print(item)
            # print(
            #     f"{index} {item.name} - {debug}"
            # )
            # print()

        #   endregion

        #
        #   TRANSLATION
        #   region
        #

        # Try:
        try:
            # Get wikitext data from given page
            wikitext_raw = str(
                general_wikitext.sections[
                    indexes.index("original text and translations")
                ]
            )

        # Except
        except Exception:
            # Get wikitext data from given page
            wikitext_raw = str(
                general_wikitext.sections[
                    indexes.index("text and translations")
                ]
            )

        # Parse JSON response for the text with wikitext parser
        text = mwparserfromhell.parse(wikitext_raw)

        # Get templates from parsed wikitext
        templates = text.filter_templates(recursive=False)

        # Iterate through the templates
        for index, item in enumerate(templates):
            # Check if the iterate item is one of the three allowed keys
            if item.name.lower() in TR_KEYS:
                # Add translation to translation dictionary
                translations[str(item.name.lower())] = [
                    str(i) for i in item.params
                ]

        #   endregion

        #
        #   DOWNLOAD LINKS
        #   region
        #

        # Get al 'li' tags
        possible_starters = page_soup.find_all("li")

        # Iterate through all tags
        for tag in possible_starters:
            # Check if the tag has "CPDL #" in it
            if tag.find(text=re.compile("CPDL #")):
                # Regex find the CPDL #
                cpdl_num = re.search(r"(?<=\#)(.*?)(?=\:)", tag.text).group(0)

                # Create index in download dictionary
                downloads[str(cpdl_num)] = []

                # Find a tags with href
                for item in tag.find_all("a", href=True):
                    # Append link to download category
                    downloads[str(cpdl_num)].append(DOMAIN + item.get("href"))

        #   endregion

        #
        #   DATA COMPILING
        #   region
        #

        # try:
        #     a = gen_info['composer'][0]
        # except:
        #     input(json.dumps(gen_info, indent=3))

        # Construct a dictionary for the information to be inserted into DB
        insert_data = {
            "_id": uuid.uuid4().hex,
            "link": link,
            "title": title,
            "composer": gen_info["composer"][0],
            "general_information": gen_info,
            "translations": translations,
            "download_links": downloads,
        }

        # # input("done")
        # input(json.dumps(gen_info, indent=3))

        # Insert to collection
        col.insert_one(insert_data)

        #   endregion

        # Sleep for a bit
        time.sleep(1)

    # Except
    except Exception:
        # Print status
        print(f"One link not added >>> {link}")

        # Log error
        with open(f"logs/{uuid.uuid4().hex}.txt", "w") as file:
            traceback.print_exc(file=file)

        # # Pause
        # input()


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
        return " ".join([str(i) for i in information])

    # Else, return the given value
    return value


# Main thread
if __name__ == "__main__":
    # var
    var = 0

    # Iterate 39 times
    for page in range(0, 4100, 100):
        # Make a list of links for a given page
        list_of_links = []

        # If i is 0, get html data of the index pages
        if not page:
            response = urlopen(
                "https://www.cpdl.org/wiki/index.php/ChoralWiki:Score_catalog"
            )
        # If not, get the corresponding pages
        else:
            response = urlopen(
                "https://www.cpdl.org/wiki/index.php/ChoralWiki"
                + f":Score_catalog/{page}"
            )

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
            _ = [executor.submit(scrape, link) for link in list_of_links]

        # Print next page
        print(f"-= NEXT PAGE {page}/4000 =-")
