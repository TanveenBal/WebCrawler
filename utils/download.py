import requests
import cbor
import time

from utils.response import Response
from scraper import is_valid


def _is_large_file(headers):
    if 'Content-Length' in headers:
        file_size = int(headers['Content-Length'])
        print(file_size)
        if file_size > (250 * 1024): #250 kb
            return True
    else:
        return False

def download(url, config, logger=None):
    if not is_valid(url):
        return Response({
            "error": f"Invalid URL: {url}.",
            "status": 0,  # idk wha the code would be though
            "url": url})

    host, port = config.cache_server
    headers = requests.head(f"http://{host}:{port}/", params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
    if _is_large_file(headers):
        return Response({
            "error": f"File too large: {url}.",
            "status": headers.status_code,  #idk wha the code would be though
            "url": url})

    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
