from collections import defaultdict
from simhash import hamming_distance

class ScraperData():
    def __init__(self):
        # key: tokens, value: count of each token
        self.all_tokens = defaultdict(int)
        # key: unique urls, value: total number of tokens on each url
        self.unique_links = dict()
        # key: ics.uci.edu subdomains, value: number of unique pages detected in each subdomain
        self.subdomains = defaultdict(int)
        # all simhash vlaues
        self.simhash_values = set()
        # key: low info url, value: total number of tokens on each url
        self.low_info = dict()
        self.file_count = 0

    def incr_file_count(self):
        self.file_count += 1

    # Getter and setter methods.
    def get_unique_links_keys(self):
        return self.unique_links.keys()
    def get_unique_links(self):
        return self.unique_links.items()
    def get_unique_links_value(self, url):
        return self.unique_links[url]
    def update_unique_links(self, url, count):
        self.unique_links[url] = count

    def update_subdomains(self, subdomain):
        self.subdomains[subdomain] += 1

    def get_simhash_values(self):
        return self.simhash_values
    def update_simhash_values(self, value):
        self.simhash_values.add(value)

    def update_low_info(self, url, count):
        self.low_info[url] = count
    def get_low_info_value(self, url):
        return self.low_info[url]

    # Write data contents to file for crash recovery.
    def write_unique_url(self, url):
        with open("unique_links.txt", 'a') as file:
            file.write(f"{url}, {self.get_unique_links_value(url)}\n")
    
    def write_simhash(self, hash):
        with open("hash_vals.txt", 'a') as file:
            file.write(f"{hash}\n")
    
    def write_all_tokens(self):
        with open("all_tokens.txt", 'w') as file:
            for token, freq in self.all_tokens.items():
                file.write(f"{token}, {freq}\n")
        self.file_count = 0
    
    def write_subdomains(self):
        with open("subdomains.txt", 'w') as file:
            for subdom, freq in self.subdomains.items():
                file.write(f"{subdom}, {freq}\n")
    
    def write_low_info(self, url):
        with open("low_info.txt", 'a') as file:
            file.write(f"{url}, {self.get_low_info_value(url)}\n")

    # Simhash similarity checker.
    def similar(self, simhash):
        # Check similarity of a new simhash with existing simhash values.
        for i in self.get_simhash_values():
            similarity = hamming_distance(simhash, i)
            if similarity < 0.05: # Threshold for similarity.
                return True
        # If not similar, add the new simhash value to the set.
        self.update_simhash_values(simhash)
        return False
    
    # Get information about the longest page
    def get_longest_info(self):
        if len(self.unique_links.keys()) == 0:
            return ("", 0)
        max_key = max(self.unique_links, key=self.unique_links.get)
        max_value = self.unique_links[max_key]
        return (max_key, max_value)
    
