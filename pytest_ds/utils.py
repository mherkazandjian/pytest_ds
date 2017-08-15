from __future__ import print_function
import re
import StringIO
import time
import hashlib
from collections import OrderedDict

from lxml import etree


try:
    import selenium
    SELENIUM_IS_AVAILABLE = True
except ImportError:
    print("can not import selenium, headless source fetcher will be disabled")
    SELENIUM_IS_AVAILABLE = False

if SELENIUM_IS_AVAILABLE:
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from pyvirtualdisplay import Display

import requests
from requests.packages.urllib3.exceptions import (
    InsecureRequestWarning,
    InsecurePlatformWarning,
    SNIMissingWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
requests.packages.urllib3.disable_warnings(SNIMissingWarning)



class Content(object):
    """
    Storage for items/content.
    """
    def __init__(self, content_type, name, mtime):
        """
        constructor

        :param content_type: type of content (dir, file, symlink...) 
        :param name: the name of the content (e.g the file name)
        :param mtime: the last modification time
        """
        self.type = content_type
        """the type of the cotent dir/file """

        self.name = name
        """the name of the content (e.g. filename)"""

        self.mtime = mtime
        """the last modification time """

        self.subdir = []
        """the content of the object if it is a subdir"""

        self.msize = None    # not used
        """the size of the in bytes"""

        self.md5 = None      # not used
        """the md5sum of the file"""



class HtlmSourceFetcherHeadless(object):
    """
    Handle to the html source of a url using a headless browser. 
    """

    SETTINGS = {
        'linux': {
            'chrome_binary_location': '/usr/bin/google-chrome',
            'chrome_driver_location': '/progs/chrome/chromedriver',
            'phantomjs_binary_location': '/usr/bin/phantomjs',
            'firefox_binary_location': '/progs/'
        }
    }

    def __init__(self):
        """
        constructor
        """
        self.driver = None
        if SELENIUM_IS_AVAILABLE:
            self._init()

    def _init(self):
        """
        constructor
        """
        display = Display(visible=0, size=(1000, 10000))
        display.start()

        # driver = self.setup_chrome()
        driver = self.setup_firefox()
        # driver = self.setup_phantomjs()
        driver.set_window_size(1080, 5000)

        self.driver = driver

    def setup_chrome(self):
        """return a webdriver set up to use chrome

        :return: webdriver 
        """
        paths = self.SETTINGS['linux']

        chrome_opts = webdriver.ChromeOptions()
        chrome_opts._binary_location = paths['chrome_binary_location']
        # chrome_opts._arguments = ["--enable-internal-flash", "--headless"]
        chrome_opts._arguments = []
        driver = webdriver.Chrome(paths['chrome_driver_location'],
                                  port=4445,
                                  chrome_options=chrome_opts)
        return driver

    def setup_firefox(self):
        """return a webdriver set up to use firefox

        :return: webdriver 
        """
        return webdriver.Firefox()

    def setup_phantomjs(self):
        """return a webdriver set up to use phantomjs

        :return: webdriver 
        """
        return webdriver.PhantomJS()


class HtmlSourceFetcherRequests(object):
    pass


class ElementsFinder(object):
    """
    
    """
    def __init__(self):
        self.fetcher = HtlmSourceFetcherHeadless()
        # self.fetcher = HtmlSourceFetcherRequests()
        self.enabled = None

        if self.fetcher.driver is not None:
            self.enabled = True

    def get(self, url):
        self.fetcher.driver.get(url)

    def find_table_elements(self):
        """
        
        :return: 
        """
        timeout = 10

        driver = self.fetcher.driver

        # if the page has "summary hidden" in the footer then it is an
        # empty folder, return []
        try:
            WebDriverWait(
                driver, timeout
            ).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//tr[@data-id]')
                )
            )
        except TimeoutException:
            print('either an empty dir or page not ready withing timeout')

        if driver.find_elements_by_xpath('//tr[@class="summary"]'):

            n_files_str = driver.find_element_by_xpath(
                    '//span[contains(@class, "fileinfo")]'
                ).text.encode().replace('file', '').replace('s', '').strip()
            n_files = 0 if n_files_str == '' else int(n_files_str)

            n_dirs_str = driver.find_element_by_xpath(
                    '//span[contains(@class, "dirinfo")]'
                ).text.encode().replace('folder', '').replace('s', '').strip()
            n_dirs = 0 if n_dirs_str == '' else int(n_dirs_str)

            try:
                WebDriverWait(
                    driver, timeout
                ).until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//tr[@data-id]')
                    )
                )
            except TimeoutException:
                msg = "page does not seem ready - <table .. /table> not foud"
                print(msg)

            elements = self.fetcher.driver.find_elements_by_xpath(
                '//tr[@data-id]')

            assert len(elements) == n_files + n_dirs
            return elements

        elif driver.find_elements_by_xpath('//tr[@class="summary hidden"]'):
            return []


def download_file_to_buffer(url, auth=None):
    """
    taken from http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    """
    print('downloading {}'.format(url))
    try:
        request = requests.get(url, stream=True, auth=auth, verify=False)
        _buffer = StringIO.StringIO()
        for chunk in request.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                _buffer.write(chunk)
        _buffer.seek(0)
    except Exception as e:
        print('download failed...')
        print(e)
        return None
    else:
        return _buffer.read()

def download_file(url, local_url, auth=None):
    """
    taken from http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    """
    print('downloading {} to {}'.format(url, local_url))
    try:
        request = requests.get(url, stream=True, auth=auth, verify=False)
        with open(local_url, 'wb') as fobj:
            for chunk in request.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    fobj.write(chunk)
    except Exception as e:
        print('download failed...')
        print(e)
        return False
    else:
        return True


def sort_dict_by_key(in_dict):
    """return the input dictionary as an ordered dictionary sorted by key
    
    :param value: a dictionary
    :return: ordered dictionary 
    """
    retval = OrderedDict()
    for key in sorted(in_dict):
        retval[key] = in_dict[key]
    return retval


def calculate_md5sum(path, expected_md5sum=None):
    """
    Compute the md5sum of a file located a 'path'

    :param str path: the path to the file
    :param str expected_md5sum: the expected md5sum of the file
    :return: str: the md5sum
    """
    _hash = hashlib.md5()
    with open(path, "rb") as fobj:
        for chunk in iter(lambda: fobj.read(4096), b""):
            _hash.update(chunk)
    local_md5sum = _hash.hexdigest()

    if expected_md5sum is not None:
        if local_md5sum == expected_md5sum:
            return local_md5sum
        else:
            msg = (
                'the md5sum of {} does not match the expected md5sum\n'
                'The corresponding md5sum are\n{}\n{}\n').format(
                path,
                local_md5sum,
                expected_md5sum
            )
            raise AssertionError(msg)
    else:
        return local_md5sum
