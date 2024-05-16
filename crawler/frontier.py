import os
import shelve

from threading import Thread, RLock, Condition
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

from urllib.parse import urlparse


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.shelf_lock = RLock()
        self.lock = RLock()
        self.inf_lock = RLock()
        self.stat_lock = RLock()
        self.ics_lock = RLock()
        self.cs_lock = RLock()
        self.is_multithreaded = False if self.config.threads_count == 1 else True
        
        if self.is_multithreaded:
            self.domains = {
                ".informatics.uci.edu": [],
                ".stat.uci.edu": [],
                ".ics.uci.edu": [],
                ".cs.uci.edu": [],
            }

            # Condition variables for each domain.
            self.conditions = {  
                ".informatics.uci.edu": Condition(self.lock),
                ".stat.uci.edu": Condition(self.lock),
                ".ics.uci.edu": Condition(self.lock),
                ".cs.uci.edu": Condition(self.lock),
            }
        else:
            self.to_be_downloaded = list()

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def get_domain(self, url):
        domain = '#' + urlparse(url)._replace(fragment='').netloc
        if domain.endswith(".informatics.uci.edu") or domain.endswith("#informatics.uci.edu"):
            return ".informatics.uci.edu"
        elif domain.endswith(".stat.uci.edu") or domain.endswith("#stat.uci.edu"):
            return ".stat.uci.edu"
        elif domain.endswith(".ics.uci.edu") or domain.endswith("#ics.uci.edu"):
            return ".ics.uci.edu"
        elif domain.endswith(".cs.uci.edu") or domain.endswith("#cs.uci.edu"):
            return ".cs.uci.edu"
        return
    
    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                if self.is_multithreaded:
                    domain = self.get_domain(url)
                    with self.conditions[domain]:
                        self.domains[domain].append(url)
                        self.conditions[domain].notify()
                else:
                    self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def has_urls(self, domain):
        return len(self.domains[domain]) > 0

    def get_tbd_url(self, domain):
        try:
            if self.is_multithreaded:
                if self.domains[domain]:
                        with self.conditions[domain]:
                            return self.domains[domain].pop(0)
                return None
            else:
                return self.to_be_downloaded.pop()
        except IndexError:
            return None
                
    
    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            if self.is_multithreaded:
                domain = self.get_domain(url)
                if domain is not None:
                    with self.conditions[domain]:
                        self.save[urlhash] = (url, False)
                        self.save.sync()
                        self.domains[domain].append(url)
                        self.conditions[domain].notify()
            else:
                self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
