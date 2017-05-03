class DataSourceBase(object):
    """
    
    """
    def __init__(self, url, *args, **kwargs):
        """
        
        :param url: 
        :param args: 
        :param kwargs: 
        """
        self._url = url

    def ls(self):
        """
        
        :return: 
        """
        raise NotImplementedError('should be implemented by subclass')

    def ls_url(self):
        """should return a dictionary. The keys of the dictionary are the
        paths on the file as they would be accessed on a local file system
        .e.g path/to/file1 (not there should be no / at the beginning). The
        value correcsponding to each key is a [Content object, download url]
        
        :return: 
        """
        raise NotImplementedError('should be implemented by subclass')
