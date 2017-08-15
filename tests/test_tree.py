import os
from pytest_ds.tree import Query
from pytest_ds.utils import calculate_md5sum


def test_that_owncloud_datasource_can_retrieve_files_lists():

    config_file = 'owncloud_1.ini'

    tree = Query(config=config_file, index_webdav_enabled=True)

    contents = tree.get_file_system_paths()
    paths = [content[1] for content in contents]

    expected_paths = [
        'data.dat',
        'foo1.txt',
        'test_dir1/test_subdir1/mini.txt'
    ]

    assert paths == expected_paths


def test_that_owncloud_datasource_can_compose_urls():

    config_file = 'owncloud_1.ini'

    tree = Query(config=config_file, index_webdav_enabled=True)

    urls = tree.get_download_urls()

    expected_download_urls = [
        'https://owncloud.strw.leidenuniv.nl/download?path=&files=data.dat',
        'https://owncloud.strw.leidenuniv.nl/download?path=&files=foo1.txt',
        'https://owncloud.strw.leidenuniv.nl/download?path=test_dir1%2Ftest_subdir1&files=mini.txt'
    ]

    assert urls == expected_download_urls


def test_that_owncloud_datasource_can_sync_files():

    config_file = 'owncloud_1.ini'

    tree = Query(config=config_file, index_webdav_enabled=True)

    tree.sync(n_threads=1, dry=False)

    expected_md5sums_of_downloaded_files = zip(
        tree.absolute_fs_paths(),
        [
            'e7a203bebecc14c30117226c365171fd',
            'f643ffa057e884814bf39e6b2141342c',
            'faff0001d525a613ebc80b7359d3a924'
        ]
    )

    md5sums = [
        (abs_fpath, calculate_md5sum(abs_fpath))
        for abs_fpath in tree.absolute_fs_paths()
    ]

    assert md5sums == list(expected_md5sums_of_downloaded_files)

