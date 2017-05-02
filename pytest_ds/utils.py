from __future__ import print_function
import re
import StringIO
import time
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

import euclid_useful_stuff
# from euclid_useful_stuff.tree import Content


class DataSourceBase(object):
    def __init__(self):
        print('DatasourceBase init')
        pass


class WebdavDataSource(DataSourceBase):

    def __init__(self, *args, **kwargs):
        super(WebdavDataSource, self).__init__(*args, **kwargs)
        self._xml_payload = """<?xml version="1.0"?><d:propfind  xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getlastmodified/><d:getetag/><d:getcontenttype/><d:resourcetype/><oc:fileid/><oc:permissions/><oc:size/><d:getcontentlength/></d:prop></d:propfind>"""

        self._headers = {
            'Authorization': 'Basic bDBPbXFobHZOem9KaHpwOm51bGw=',
            'Origin': 'https://ia2-owncloud.oats.inaf.it',
            # 'Depth': 'infinity'
            'Depth': '3'
        }

    def ls(self, recursive=False):
        """
        
        :param recursive: 
        :return: 
        """

        t0 = time.time()
        request = requests.request(
            'PROPFIND',
            'https://ia2-owncloud.oats.inaf.it/public.php/webdav/',
            data=self._xml_payload,
            headers=self._headers,
        )
        dt = time.time() - t0

        with open('webdav_tree.xml', 'w') as fobj:
            fobj.write(request.content)

        return request.content

    @staticmethod
    def strip_namespaces(xml_str):
        """find namespaces in the xml str and remove them from xml_str and
        return the string without all namespaces and the namespace prefixes.

          <d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">

        - search for line that contains xmlns
        - namespaces are the stuff between "xmlns:" and "="
        - remove them from all the file 
        - for each namespace "X" replace all matches "</X:" with "</:" and
          "<oc:" by "<:"

        :param xml_str: the xml string
        :return: the xml string without the namespaces
        """
        namespaces = map(lambda x: x.replace('xmlns:', ''),
                         filter(lambda x: 'xmlns:' in x,
                                re.split('(xmlns:\w+)', xml_str)))

        # remove namespaces from top the xml string
        xml_str = xml_str.replace(re.split('(xmlns.*)', xml_str)[1], '>')

        with open('foo.xml', 'w') as fobj:
            fobj.write(xml_str)

        # remove the namespace prefix from all elements
        for namespace in namespaces:
            xml_str = xml_str.replace('<{}:'.format(namespace), '<')
            xml_str = xml_str.replace('</{}:'.format(namespace), '</')

        return xml_str

    def parse_xml_content(self, webdave_response_content):
        """generate a list of files only from the xml response of webdav
        
        :return: list of file content [[href, xml_owncloud_props_element]]
        """
        root = etree.fromstring(
            self.strip_namespaces(webdave_response_content)
        )

        # keep file_elements that have an OK status (that refer to files)
        xml_content_elements = []
        for element in root.xpath('//response'):
            matches = element.xpath(".//*[contains(text(), '200 OK')]")
            if len(matches) > 0:
                match = matches[0]
                props = match.getparent()
                href = props.getparent().xpath('.//href/text()')[0]
                xml_content_elements.append((href, match))

        # generate the flat list of Content object, keep files, exclude dirs
        return filter(lambda x: not x[0].endswith('/'), xml_content_elements)

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
