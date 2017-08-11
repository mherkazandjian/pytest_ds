"""
<keywords>
test, python, pytest_ds, plugin, http
</keywords>
<description>
test using the pytest_ds pytest plugin to retreive a test file before running
a test
</description>
<seealso>
</seealso>
"""
import functools
import py.test


def fetch_file(url):

    # do something

    urls = {
        'http://www.foo.edu/aaa.out': '/local/path/to/aaa.out',
        'http://www.foo.edu/bbb.out': '/local/path/to/bbb.out',
    }

    local_path = urls[url]

    def wrapped(func):

        # do something before calling the function

        retval = functools.partial(func, local_path)

        # do something after calling the function

        return retval

    return wrapped

@py.test.datasource('http://ipv4.download.thinkbroadband.com/5MB.zip')
def test_that_data_can_be_retrieved_through_http(fpath):
    with open(fpath) as fobj:
        assert fobj.read() == '0x000000000000000000000000'



@fetch_file('http://www.foo.edu/aaa.out')
def my_foo(paths, a, b):
    return a + b


my_foo(1, 2)

print('done')
