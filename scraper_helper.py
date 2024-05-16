from urllib.parse import urldefrag, urljoin
import re

# Checks parsed url for traps.
def is_trap(parsed):
    # Limited queries for archive.ics.uci.edu
    max_rec_archives = {"format=": 0, "format=mat": 0, "format=nonmat": 0, "att=": 0}
    disallowed_queries = {"download", "login", "edit", "do=download", "do=login", "do=edit, action=download", "action=login", "action=edit", "do=backlink"}
    allowed_schemes = {"http", "https"}

    # Not allowed if these traps are found.
    if parsed.path.startswith("/~eppstein/pix") or parsed.path.startswith("/~eppstein/pubs/pubs.sh") or parsed[:6] == "mailto" or parsed.scheme not in allowed_schemes:
        return True
    
    # Split queries based on the '&' symbol and check for disallowed queries.
    query_params = parsed.query.split('&')
    for param in query_params:
        for query in disallowed_queries:
            if query in param:
                return True
        if parsed.netloc.endswith("archive.ics.uci.edu") and param in max_rec_archives:
            max_rec_archives[param] += 1
            # Limit for archive.ics.uci.edu queries
            if max_rec_archives[param] > 1000:
                return True
    
    return False

# Checks if url is formatted as an event or calendar.
def is_calendar_url(url):
    calendar_pattern = r'.*/(events|calendar)/.*'
    match = re.match(calendar_pattern, url)
    return match

# Joins base and relative urls.
def get_absolute_url(base_url, link):
    relative_url = link.get('href')
    absolute_url = urljoin(base_url, relative_url)
    absolute_url, _ = urldefrag(absolute_url)
    return absolute_url

# Checks if parsed url does not belong in the 4 allowed domains.
def wrong_ending(parsed):
    allowed_domains = [".informatics.uci.edu", ".stat.uci.edu", ".ics.uci.edu", ".cs.uci.edu"]
    for domain in allowed_domains:
        if parsed.netloc.endswith(domain):
            return False
    return True

# Checks if the website has a low amount of text and tokens.
def low_information(tokens, html_tags):
    return html_tags == 0 or tokens/html_tags <= .22 or tokens < 50
