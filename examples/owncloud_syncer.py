from __future__ import print_function
import os

from pytest_ds.tree import Query

config_file = os.path.expanduser('~/tmp/owncloud_pytest_ds_webdav.ini')
# config_file = os.path.expanduser('~/tmp/surfdrive.ini')

tree = Query(config=config_file, index_webdav_enabled=True)

# tree.show()
# tree.ls()
# tree.ls_url()
# tree.sync()

print('done')
