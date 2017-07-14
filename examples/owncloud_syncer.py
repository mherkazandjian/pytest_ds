from __future__ import print_function

from pytest_ds.tree import Query

config_file = '../sample/configs/webdav_test.ini'

tree = Query(config=config_file, index_webdav_enabled=True)

tree.show()
# tree.ls()
tree.ls_url()
tree.sync(n_threads=10, dry=False)

print('done')
