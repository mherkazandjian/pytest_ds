"""
supported:
   - if no data is synced with remote, remote content is copied to local.
   - if a file is modified on remote it is synced.
   - if a file does not exist on local it is synced from remote.
   - if cache file is missing but the data dir has the same content as the
      remtoe everything needs to be synced again.
     .. todo:: maybe used md5 sums in this case to avoid re-dwon
   - empty dirctories on remote are not created on local

# not supported
   - .. todo:: if a file is deleted on remote, it should be deleted also locally
   - .. todo:: upon a sync.
   - .. todo:: sync empty dirs
"""
import os
from os.path import expanduser
import pickle
import ConfigParser
import threading

from .utils import (
    ElementsFinder, download_file, download_file_to_buffer,
    WebdavDataSource
)


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


class Query(object):
    """
    Content handler. Provide functionality to syncronize content obtained
    from an owncloud folder, or via an ascii file that maps a directory struct
    to http urls. The owncloud folder must be public one. No authentication
    methods are supported.
    """
    def __init__(self,
                 root_url=None,
                 crawler_enabled=True,
                 index_url_enabled=False,
                 setup_cache=True,
                 config=None):
        """
        constructor

        :param root_url: The root url of the owncloud folder
        :param crawler_enabled: If True, crawling urls in enabled. If false
         the crawling is disabled even if crawling config variable is enabled
         in the configuration variable.
        :param index_url_enabled: if True, getting the content info from the
         index url is enabled. if False getting infor from the index url is 
         disabled even if it is enabled in the configuration variable.
        :param setup_cache: if True read the cache from disk
        """
        self._url = root_url
        """.. todo:: """

        self._local_dir = None
        """.. todo:: """

        self.finder = None
        """.. todo:: """

        self.depth = 0
        """.. todo:: """

        self.contents = []
        """.. todo:: """

        self.cache = None
        """.. todo:: """

        self.fs_paths = None
        """.. todo:: """

        self.config_path = config
        """the path to the configuration file used"""

        self.config = None
        """the configuratoin dictionary"""

        self._condition = threading.Condition()
        """condition used for parallel downloads in updating cache"""

        if self.config_path is not None:
            self.config = self.setup_configuration(self.config_path)
            self._check_set_attributes_from_config()

        x = WebdavDataSource()
        v = x.ls()
        x.parse_xml_content(v)

        # get the content info from the index url only
        if index_url_enabled:
            if self.config.get('RemoteDataSource', 'has_index_file') == 'yes':
                if self.config.get('RemoteDataSource', 'crawl') == 'no':
                    self.contents = self._setup_content_from_index_url()

        # get the cotnent info by crawling the rool url
        if crawler_enabled:
            if self.config.get('RemoteDataSource', 'crawl') == 'yes':
                self.contents = self._setup_content_by_crawling_roo_url()

        # set up the content from cache file, no crawling or index reading
        # the cache and the content will be identical
        if (index_url_enabled or crawler_enabled) and self.contents == []:
            if self.config.get('RemoteDataSource', 'from_cache') == 'yes':
                print('reading content from cache file')
                _query = self.from_cache(self.config_path)
                self.contents = _query.contents

        if self.contents is not None:
            self.hash()

        if setup_cache:
            self.setup_cache()

    def _check_set_attributes_from_config(self):
        """
        set necessary attribute values from the configuration and check for
        conflicts in specified options
        """
        if self._url is None:
            self._url = self.config.get('RemoteDataSource', 'root_url')

    @staticmethod
    def setup_configuration(config_path):
        """
        parse the configuration file and set the attribute self.cofnig
        """
        config = ConfigParser.ConfigParser()
        config.read(config_path)
        return config

    def _setup_content_by_crawling_roo_url(self):
        """
        fetch the content info by crawling the root url recursively. If the
        finder (crawler) is not supported,
        """
        self.finder = ElementsFinder()
        if self.finder.enabled:
            contents = self.owncloud_crawl(self._url)
        else:
            print('can not crawl root_url, consider setting up content from\n'
                  'a cache file, or reading the content tree from a tree\n'
                  'dump file')
            contents = self._setup_content_from_index_url()

        return contents

    def _setup_content_from_index_url(self):
        """try to obtain the file index from the remote folder through http
        and parse it and return the content tree, the content that would be
        returned by crawling the root_url
        """
        index_file_url = expanduser(
            self.config.get('RemoteDataSource', 'index_file'))

        fsinfo = download_file_to_buffer(
            self.get_owncloud_download_url_from_fs_path(
                self._url,
                index_file_url))

        assert fsinfo is not None

        flat_content = []
        for line in filter(lambda x: x != '', fsinfo.split('\n')):
            ftype, size, mtime, fs_path = filter(lambda x: not x == '',
                                                 line.split('##'))
            ftype = ftype.replace('directory', 'dir')
            ftype = ftype.replace('regular file', 'file')
            flat_content.append(Content(ftype, fs_path, mtime))
            # print(ftype, mtime, fs_path)

        print('found {} items in the index:\n\t'.format(
            len(flat_content), index_file_url))

        return self.crawl_fs_info(flat_content)

    def crawl_fs_info(self, flat_content):
        """Generate the contents tree by parsing the content of a file that
        is dumped by the command:
        
              ~> find test_data -exec stat --format="##%F##%s##%Y##%n##" '{}' \;
        """
        contents = []

        # add contents that is at the current depth
        contents.extend(
            filter(
                lambda x: os.path.dirname(x.name) == '',
                flat_content))

        # handle the remaining flat contents
        flat_content_remaining = filter(
            lambda x: os.path.dirname(x.name) != '',
            flat_content)

        if flat_content_remaining:
            for content in contents:
                if content.type == 'dir':
                    dirname = content.name

                    this_dir_content_flat = []

                    # remove dirname from the prefix on the content of dirname
                    for _content in flat_content_remaining:
                        if _content.name.startswith(dirname + '/'):
                            _content.name = _content.name.split(
                                dirname + '/')[-1]
                            this_dir_content_flat.append(_content)

                    content.subdir = self.crawl_fs_info(this_dir_content_flat)
        else:
            # no subdir content to be processed
            pass

        return contents

    def owncloud_crawl(self, url):
        """
        Crawl the url and find all the data. The url is crawled recursively.

        :return: A tree of Content objects
        """
        self.depth += 1

        self.finder.get(url)
        elements = self.finder.find_table_elements()

        contents = []
        for element in elements:
            contents.append(Content(
                element.get_attribute('data-type').encode(),
                element.get_attribute('data-file').encode(),
                element.get_attribute('data-mtime').encode()
            ))

        for content in contents:
            print('{indentation}{:5} {:10} {}'.format(
                content.type,
                content.mtime,
                content.name,
                indentation='\t' * self.depth)
            )

            if content.type == 'dir':
                if self.depth == 1:
                    path_html_key = '?path='
                else:
                    path_html_key = ''
                sub_url = '{url}{path_html_key}%2F{sub_dir}'.format(
                    url=url, path_html_key=path_html_key, sub_dir=content.name)
                content.subdir = self.owncloud_crawl(sub_url)

        self.depth -= 1

        return contents

    def ls(self):
        """
        list all the paths
        """
        print('{} paths {}'.format('-'*50, '-'*50))
        for fs_path in self.fs_paths:
            print(fs_path)
        print('{} end paths {}'.format('-'*50, '-'*50))

    def ls_url(self):
        """
        list all the urls
        """
        print('{} urls {}'.format('-'*50, '-'*50))
        for download_url in self.get_download_urls():
            print(download_url)
        print('{} end urls {}'.format('-'*50, '-'*50))

    def show(self):
        """
        print the content tree (node names) to stdout
        """
        self.depth = 0
        self._show(self.contents)

    def _show(self, contents):
        """
        recursively traverse the content tree and print the names one on
        each line. Each dir level increases the indentation level by one.
        """
        print('-'*100)
        for content in contents:
            print('{}{}'.format('\t'*self.depth, content.name))
            if content.subdir:
                self.depth += 1
                self._show(content.subdir)
        self.depth -= 1
        print('-'*100)

    def get_file_system_paths(self):
        """
        generate the files system paths from the full content of the
        object
        """
        self.depth = 0
        return self._get_file_system_paths(self.contents)

    def _get_file_system_paths(self, contents):
        """
        recursively traverse the content tree and generate the path of the
        content.
        """
        conents_paths = []
        for content in contents:
            if content.subdir:
                self.depth += 1

                def foo(x):
                    _content, path = x
                    return _content, content.name + '/' + path

                conents_paths.extend(
                    map(foo,
                        self._get_file_system_paths(content.subdir))
                )
            else:
                if not content.type == 'dir':
                    conents_paths.append((content, content.name))

        self.depth -= 1
        return conents_paths

    def get_download_urls(self):
        return self.get_download_urls_owncloud()

    @staticmethod
    def get_owncloud_download_url_from_fs_path(root_url, fs_path):
        """return the download url of a file specified relative to the top
        level url e.g folder1/sub_folder2/foo.out of an owncloud folder
        
        :param root_url: the url of the top level owncloud folder dir
        :param fs_path: the fs path relative to the root_url
        :return: owncloud downloadable url 
        """
        dir_path = os.path.dirname(fs_path)
        basename = os.path.basename(fs_path)

        retval = '{root_url}/download?path={dir_path}&files={fname}'.format(
            root_url=root_url,
            dir_path=dir_path.replace('/', '%2F'),
            fname=basename
        )
        return retval

    def get_download_urls_owncloud(self):
        """
        generate the owncloud download urls of all the content.
        """
        fs_paths = self.get_file_system_paths()
        download_urls = []

        for content, fs_path in fs_paths:
            dir_path = os.path.dirname(fs_path)
            basename = os.path.basename(fs_path)

            download_url = 'download?path={dir_path}&files={fname}'.format(
                dir_path=dir_path.replace('/', '%2F'),
                fname=basename
            )
            download_urls.append(download_url)

        return map(lambda x: self._url + '/' + x, download_urls)

    def write_cache(self):
        """
        write the content to a cache pickle file pointed to by "cache" in the
        configuration file
        """
        cache_path = self.config.get('LocalStorage', 'cache')
        with open(os.path.expanduser(cache_path), 'wb') as fobj:
            pickle.dump(self.contents, fobj)
        print('wrote cache file:\n\t{}'.format(fobj.name))

    @staticmethod
    def from_cache(config):
        """
        read the content to a cache pickle file pointed to by "cache" variable
        in the configuration file

        :return: Query object
        """
        retval = Query(config=config,
                       crawler_enabled=False,
                       setup_cache=False,
                       index_url_enabled=False)
        cache_path = os.path.expanduser(
            retval.config.get('LocalStorage', 'cache'))

        if os.path.isfile(cache_path):
            print('loading cache from {}'.format(cache_path))
            contents = pickle.load(open(cache_path))
        else:
            print('cache file not found:\n\t{}'.format(cache_path))
            contents = []

        retval.contents = contents
        retval.hash()
        return retval

    def setup_cache(self):
        """
        Set the cache attribute to the content of the cache pickle file and
        set up the cache object by loading the cache info from disk.
        """
        print('setting up the cache object')
        self.cache = Query.from_cache(self.config_path)

    def hash(self):
        """
        set the fs_path dictionary by hashing all the fs_path and mapping them
        to the content and the url.
        """
        self.fs_paths = {
            path: (content, url) for
            (content, path), url in zip(self.get_file_system_paths(),
                                        self.get_download_urls())
        }

    def _update_local_cache_file(self, fs_path, download_url, content):
        """syncronize the download_url
        
        :param fs_path: the path relative to the data dir
        :param download_url: the download url
        :param content: the content object
        """
        local_data_dir = expanduser(self.config.get('LocalStorage', 'datadir'))
        local_abs_path = os.path.join(local_data_dir, fs_path)
        local_abs_dir = os.path.dirname(local_abs_path)

        def safe_makedirs(dir_path):
            self._condition.acquire()
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
            self._condition.release()

        # if content is a directory, create it
        if content.type == 'dir':
            safe_makedirs(local_abs_path)
        else:
            safe_makedirs(local_abs_dir)

            # download and update the cache if the download is successfull
            if download_file(download_url, local_abs_path):
                self._condition.acquire()
                self.fs_paths[fs_path] = (content, download_url)
                self._condition.release()

    def cache_file_is_in_localdir(self, fs_path):
        """
        if the file fs_path exists is local_dir True is returned else false is
        
        :return: bool
        """
        local_data_dir = expanduser(self.config.get('LocalStorage', 'datadir'))
        local_abs_path = os.path.join(local_data_dir, fs_path)

        retval = True if os.path.exists(local_abs_path) else False
        return retval

    def _sync_file(self, fs_path, download_url, content):
        """
        syncronzie the file at "download_url" to local path "fs_path" using
        metadata from the object "content"
        """
        if fs_path not in self.cache.fs_paths:
            # new file no found in cache
            # download the download_url to fs_path
            print('file not in local cache: {}'.format(fs_path))
            self._update_local_cache_file(fs_path, download_url, content)
        elif fs_path in self.cache.fs_paths:
            # file exists in cache db but has a more recent mtime on remote
            cache_content, _ = self.cache.fs_paths[fs_path]

            if not self.cache_file_is_in_localdir(fs_path):
                print('file not in local data dir: {}'.format(fs_path))
                self._update_local_cache_file(
                    fs_path, download_url, content)
                return

            if cache_content.mtime != content.mtime:
                # download the download_url to fs_path
                print('file modified: {}'.format(fs_path))
                self._update_local_cache_file(
                    fs_path, download_url, content)
            else:
                # nothing to do
                pass
        else:
            # i wonder why it should reach here, raise exception anyway
            msg = 'idk why it should get here, might be interesting'
            raise RuntimeError(msg)

    def sync(self, n_threads=10):
        """
        syncronize the local cache and data files with the remote url by
        checking the last modification time. Items on remote that have a more
        recent modification time are downloaded and the cache metadata is
        updated.
        """
        def worker(_work_producer):
            while True:
                self._condition.acquire()
                try:
                    args = _work_producer.next()
                except StopIteration:
                    args = None
                self._condition.release()

                if args is not None:
                    self._sync_file(*args)
                else:
                    break

        def work_producer():
            for fs_path, (content, download_url) in self.fs_paths.items():
                yield fs_path, download_url, content

        producer = work_producer()
        threads = [threading.Thread(target=worker, args=(producer,))
                   for tid in range(n_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.write_cache()
