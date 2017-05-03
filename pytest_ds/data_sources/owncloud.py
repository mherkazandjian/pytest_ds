from __future__ import print_function
import os
import re
import requests
from lxml import etree

from requests.packages.urllib3.exceptions import (
    InsecureRequestWarning,
    InsecurePlatformWarning,
    SNIMissingWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
requests.packages.urllib3.disable_warnings(SNIMissingWarning)

from .interface import DataSourceBase
from ..utils import Content, sort_dict_by_key


class WebdavDataSource(DataSourceBase):
    """
    Query an owncloud folder through its webdav API.
    """
    def __init__(self, url=None, token=None, dirname=None, *args, **kwargs):
        """
        constructor

        :param url: the root url of the owncloud server 
        :param token: the access token of the publicly shared folder
        :param args: None
        :param kwargs: None
        """
        super(WebdavDataSource, self).__init__(url=url, *args, **kwargs)

        self.token = token
        """The access token to the directory 'dirname'"""

        self.dirname = dirname
        """The dirname corresponding to self.token"""

        self._xml_payload = (
            """<?xml version="1.0"?><d:propfind  xmlns:d="DAV:" """
            """xmlns:oc="http://owncloud.org/ns"><d:prop>"""
            """<d:getlastmodified/><d:getetag/><d:getcontenttype/>"""
            """<d:resourcetype/><oc:fileid/><oc:permissions/><oc:size/>"""
            """"<d:getcontentlength/></d:prop></d:propfind>""")

        self._headers = {
            'Authorization': 'Basic {}'.format(token),
            'Origin': url,
            # 'Depth': 'infinity'
            'Depth': '3'
        }

    def ls(self, recursive=False):
        """return a flat list of Content object of the files in the specified
        url

        :param recursive: if True the all files at infinite depth are returned 
        :return: a flat list of Content objects
        """
        if recursive:
            old_depth = self._headers['Depth']
            self._headers['Depth'] = 'infinity'

        request = requests.request(
            'PROPFIND',
            '{}/public.php/webdav/'.format(self._headers['Origin']),
            data=self._xml_payload,
            headers=self._headers,
        )

        flat_contents = self.generate_flat_content(
            self.parse_xml_content(request.content))

        if recursive:
            self._headers['Depth'] = old_depth

        return flat_contents

    def ls_url(self, **kwargs):
        """
        return a dictionary of the paths on the server as keys and the values
        are pairs of the content object itself and the the download urls.

        :param kwargs: The recursive keyword is passed to self.ls() 
        :return: dict
        """
        flat_contnet = self.ls(**kwargs)
        download_urls = self.get_download_urls(flat_contnet)

        hashed = {
            content.name: (content, url) for
            content, url in zip(flat_contnet, download_urls)
        }

        return sort_dict_by_key(hashed)

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
                prop = match.getparent().xpath('.//prop')[0]
                href = match.getparent().getparent().xpath('.//href/text()')[0]
                xml_content_elements.append((href, prop))

        # generate the flat list of Content object, keep files, exclude dirs
        # everything ending with a / is assumed to be a dir
        return filter(lambda x: not x[0].endswith('/'), xml_content_elements)

    @staticmethod
    def generate_flat_content(parsed_xml_content):
        """given a list of pairs of [href, xml_owncloud_pros] return a flat
        list of Content objects

        :param parsed_xml_content: array returned by parse_xml_content 
        :return: list of Content objects
        """
        return [
            Content(
                content_type='file',
                name=href.replace('/public.php/webdav/', ''),
                mtime=str(prop.xpath('.//getlastmodified/text()')[0])
            )
            for href, prop in parsed_xml_content
        ]

    def get_download_urls(self, flat_contnet):
        """
        generate the owncloud download urls for a list of Content objects
        """
        download_urls = []

        for content in flat_contnet:
            dir_path = os.path.dirname(content.name)
            basename = os.path.basename(content.name)

            download_url = 'download?path={dir_path}&files={fname}'.format(
                dir_path=dir_path.replace('/', '%2F'),
                fname=basename
            )
            download_urls.append(download_url)

        url = '{}/index.php/s/{}/'.format(self._url, self.dirname)

        return map(lambda x: url + '/' + x, download_urls)
