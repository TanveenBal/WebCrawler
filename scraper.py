import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from tokenizer import tokenize, computeWordFrequencies
from scraper_data import ScraperData
from scraper_helper import is_calendar_url, low_information, get_absolute_url, wrong_ending, is_trap
from simhash import create_simhash

# Initialize global ScraperData instance
ScraperData = ScraperData()

def scraper(url, resp):
    try:
        return extract_next_links(url, resp)
    except Exception as e:
        # Log errors to a file
        with open("error.txt", 'a') as file:
            file.write(f"{e} -- {url}\n")
        return []


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    links = []

    # Ensures only unique links and non-calendar links are traversed and if the website is dead or there is an error.
    if url in ScraperData.get_unique_links() or is_calendar_url(url):
        return links
    
    parsed = urlparse(url)._replace(fragment='')

    # Check for valid and non-dead websites (status 200).
    if (resp.status == 200) and (resp.raw_response.content is not None) and (resp.error is None):
        # Parse the page content and tokenize the text
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        tokens = tokenize(soup.get_text())
  
        # Creates a hash for all tokens in the document.
        simhash = create_simhash(tokens)

        # Checks for similar website that have been crawled.
        if ScraperData.similar(simhash):
            return []

        # Update simhash and file count, write to file if necessary
        ScraperData.write_simhash(simhash)
        ScraperData.incr_file_count()
        if ScraperData.file_count == 100:
            ScraperData.write_all_tokens()
            ScraperData.write_subdomains()

        # Get the token and html count.
        token_count = computeWordFrequencies(tokens, ScraperData.all_tokens)
        html_count = len(soup.find_all())

        if low_information(token_count, html_count):
            # Update and write low-info data
            ScraperData.update_low_info(url, token_count)
            ScraperData.write_low_info(url)
        elif "sitemap" not in url and "xml" not in url:
            # Update and write unique links, and subdomains for ics.uci.edu URLs
            ScraperData.update_unique_links(url, token_count)
            ScraperData.write_unique_url(url)
            if parsed.netloc.endswith(".ics.uci.edu") and parsed.netloc != "www.ics.uci.edu" and parsed.netloc != "ics.uci.edu":
                ScraperData.update_subdomains(parsed.netloc)
        
        # Extract links from the page
        base_url = resp.url
        for link in soup.find_all('a', href=True):
            absolute_url = get_absolute_url(base_url, link)
            if is_valid(absolute_url):
                links.append(absolute_url)
                
    # Check for redirects (status 3xx).
    elif 300 <= resp.status < 400:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        base_url = resp.url
        for link in soup.find_all('a', href=True):
            absolute_url = get_absolute_url(base_url, link)
            if is_valid(absolute_url):
                links.append(absolute_url)

    # Invalid status (status 404).
    elif resp.status == 404:
        return []

    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)._replace(fragment='')

        # Conditions for invalid URLs.
        if is_trap(parsed) or wrong_ending(parsed):
            return False
        elif re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|img|mpg|ppsx)$", parsed.path.lower()):
            return False
            
        return True
    except TypeError:
        print("TypeError for ", parsed)
        raise


# Read text files for data recovery if the server crashes.
def read_unique_links():
    for line in open("unique_links.txt"):
        url, count = line.rstrip('\n').split(", ")
        ScraperData.update_unique_links(url, count)

def read_simhash():
    for line in open("hash_vals.txt"):
        hash = int(line.rstrip('\n'))
        ScraperData.update_simhash_values(hash)

def read_all_tokens():
    for line in open("all_tokens.txt"):
        token, freq = line.rstrip('\n').split(", ")
        ScraperData.all_tokens[token] = int(freq)

def read_subdomains():
    for line in open("subdomains.txt"):
        subdom, freq = line.rstrip('\n').split(", ")
        ScraperData.subdomains[subdom] = int(freq)
