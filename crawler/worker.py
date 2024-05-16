from threading import Thread, Timer
from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
from time import sleep
import re
from urllib.parse import urlparse
import urllib.robotparser
import requests
from bs4 import BeautifulSoup

class Worker(Thread):
    def __init__(self, worker_id, config, frontier, domain):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.domain = domain
        self.rp = None
        self.download_thread = None
        self.download_timer = None
        self.is_multithreaded = False if self.config.threads_count == 1 else True
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    # Start a download thread if not already running.
    def start_download_thread(self):
        if self.download_thread is None or not self.download_thread.is_alive():
            self.download_thread = Thread(target=self.backup_process)
            self.download_thread.start()

    # Wait for the domain's frontier to have URLs if in multithreaded mode.
    def domain_wait(self):
        if self.is_multithreaded:
            if len(self.frontier.domains[self.domain]) <= 0:
                with self.frontier.conditions[self.domain]:
                    self.frontier.conditions[self.domain].wait()
        else:
            return

    # Check if all frontiers for domains are empty if in multithreaded mode.
    def all_empty(self):
        if self.is_multithreaded:
            for _, links in self.frontier.domains.items():
                if len(links) > 0:
                    return False
        else:
            return True

    def run(self):
        while True:
            self.domain_wait()
            tbd_url = self.frontier.get_tbd_url(self.domain)
            
            if not tbd_url and self.all_empty():
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            
            # Check if URL is allowed by robots.txt before downloading.
            if self.is_allowed_robots(tbd_url):
                if self.is_multithreaded:
                    # Set timer to start backup thread if download exceeds time.
                    self.download_timer = Timer(0.2, self.start_download_thread)
                    self.download_timer.start() 
                    resp = download(tbd_url, self.config, self.logger)
                    self.download_timer.cancel()
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{resp.status}>, "
                        f"using cache {self.config.cache_server}.")
                else:
                    # Download the URL and log the status.
                    resp = download(tbd_url, self.config, self.logger)
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{resp.status}>, "
                        f"using cache {self.config.cache_server}.")

                # Extract URLs from the downloaded content and add them to the frontier.
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scrape_url in scraped_urls:
                    self.frontier.shelf_lock.acquire()
                    self.frontier.add_url(scrape_url)
                    self.frontier.shelf_lock.release()

                # Mark the current URL as complete in the frontier.
                self.frontier.shelf_lock.acquire()
                self.frontier.mark_url_complete(tbd_url)
                self.frontier.shelf_lock.release()
                
            else:
                self.logger.info(f"{tbd_url} is not allowed by robots.txt")
            
            sleep(self.config.time_delay)

            # Add URLs from the sitemap to the frontier.
            sitemap_urls = self.get_sitemap_urls(tbd_url)
            for url in sitemap_urls:
                self.frontier.shelf_lock.acquire()
                self.frontier.add_url(url)
                self.frontier.shelf_lock.release()

    # Retrieve URLs from the sitemap of a given URL.
    def get_sitemap_urls(self, url):
        parsed = urlparse(url)._replace(fragment='')
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
        try:
            response = requests.get(sitemap_url)
            if response.status_code == 200:
                sitemap_content = response.text
                sitemap_urls = self.parse_sitemap(sitemap_content)
                return sitemap_urls
            else:
                pass
        except Exception as e:
            self.logger.error(f"Error retrieving sitemap.xml from {url}: {e}")
        return []
    
    # Parse the content of a sitemap and extract URLs.
    def parse_sitemap(self, sitemap_content):
        urls = []
        try:
            soup = BeautifulSoup(sitemap_content, 'lxml')
            for loc in soup.find_all('loc'):
                urls.append(loc.text.strip())
        except Exception as e:
            self.logger.error(f"Error parsing sitemap: {e}")
        return urls

    # Parse robots.txt for a given URL.
    def robots(self, url):
        try:
            parsed = urlparse(url)._replace(fragment='')
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            self.rp = urllib.robotparser.RobotFileParser()
            self.rp.set_url(robots_url)
            self.rp.read()
        except Exception as e:
            self.logger.error(f"Error with robots.txt occurred: {e}")
            self.rp = None

    # Check if a URL is allowed to be fetched according to robots.txt.
    def is_allowed_robots(self, url):
        try:
            self.robots(url)
            if not self.rp.can_fetch('*', url):
                return False
            return True 
        except Exception as e:
            return True
    
    # Backup process to handle download and scraping in a separate thread.
    def backup_process(self):
        if len(self.frontier.domains[self.domain]) <= 0:
            return
        
        tbd_url = self.frontier.get_tbd_url(self.domain)
        resp = download(tbd_url, self.config, self.logger)
        self.logger.info(
            f"Downloaded {tbd_url}, status <{resp.status}>, "
            f"using cache {self.config.cache_server}.")
        
        scraped_urls = scraper.scraper(tbd_url, resp)
        for scraped_url in scraped_urls:
            self.frontier.shelf_lock.acquire()
            self.frontier.add_url(scraped_url)
            self.frontier.shelf_lock.release()
        self.frontier.shelf_lock.acquire()
        self.frontier.mark_url_complete(tbd_url)
        self.frontier.shelf_lock.release()