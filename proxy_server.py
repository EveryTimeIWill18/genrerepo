import os
import sys
import threading
import requests
from itertools import chain
from pprint import pprint
from requests.auth import HTTPBasicAuth


url = "http://alm.genre.com/qcbin/start_a.jsp"

def load_data(url, chunk_size):

    raw_data = []
    proxies = {
        'http': 'http://WMurphy:Tr2oy2222222!@usdrlxmcp01.genre.com:80',
        'https': 'http://WMurphy:Tr2oy2222222!@usdrlxmcp01.genre.com:80'
    }
    # web request
    response = requests.get(url=url, stream=True, verify=False,
                            auth=HTTPBasicAuth('WMurphy', 'Tr2oy222222!'),
                            proxies=proxies)
    for chunk in response.iter_content(chunk_size=chunk_size):
        if chunk:
            raw_data.append(chunk)

        # join the different chunks into one list
    return ''.join(d for d in raw_data)


alm_data = load_data(url=url, chunk_size=1000)
print alm_data
