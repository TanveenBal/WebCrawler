from configparser import ConfigParser
from argparse import ArgumentParser
from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from report_generator import report_generator
import time
import scraper

def main(config_file, restart):
    finished = False
    connectionerror = False

    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart, [".informatics.uci.edu", ".stat.uci.edu", ".ics.uci.edu", ".cs.uci.edu",])
    
    while not finished:
        try:
            if connectionerror:
                scraper.read_unique_links()
                scraper.read_simhash()
                scraper.read_all_tokens()
                scraper.read_subdomains()

            crawler.start()
            finished = report_generator()
        except ConnectionError:
            print("Connection Error")
            time.sleep(60)

if __name__ == "__main__":
    start = True
    while start:
        try:
            parser = ArgumentParser()
            parser.add_argument("--restart", action="store_true", default=False)
            parser.add_argument("--config_file", type=str, default="config.ini")
            args = parser.parse_args()
            main(args.config_file, args.restart)
            start = False
        except:
            time.sleep(30)