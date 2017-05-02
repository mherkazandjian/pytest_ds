class DataSourceBase(object):
    def __init__(self, url, *args, **kwargs):
        self._url = url
    def ls(self):
        raise NotImplementedError('should be implemented by subclass')
    def ls_url(self):
        raise NotImplementedError('should be implemented by subclass')

