import abc


class Engine(abc.ABC):
    @abc.abstractmethod
    def put_doc(self, location, doc, generation=None):
        """
        :param location Location: Location obj
        :param doc dict: document as a dict
        :param generation int: document generation.
        :rtype: bool

        Given a location, upload a doc to that location.
        If a generation was provided, check that the generation is newer
        than the storage generation before uploading
        Returns boolean indicating success
        """

    @abc.abstractmethod
    def get_doc(self, location):
        """
        :param location Location: Location obj
        :rtype: dict

        Given a location, download a doc
        If the doc doesn't exist, return None
        """

    @abc.abstractmethod
    def doc_exists(self, location):
        """
        :param location Location: Location obj
        :rtype: bool

        Given a location, return True if the object exists and False otherwise
        """

    @abc.abstractmethod
    def list_ids(self, collection_location, limit=None):
        """
        :param collection_location Location: Location obj|
        :param limit int:
        :rtype: list[str]

        Given a Location without an _id, return a list of _ids.
        """

    @abc.abstractmethod
    def delete_doc(self, location):
        """
        :param location Location: Location obj
        :rtype: bool

        Given a location, delete the document. Return True if found and deleted.
        """

    @abc.abstractmethod
    def delete_dir(self, location):
        """
        :param location Location: Location obj
        :rtype: bool

        Given a location, delete the directory. Return True if found and deleted.
        """

    @abc.abstractmethod
    def create_path(self, location):
        """
        :param location Location: Location obj
        :rtype: bool

        Prepare a path. Only used by local_engine
        """

    @abc.abstractmethod
    def put_metadata(self, location, doc):
        """
        :param location Location: Location obj
        :param doc dict:
        :rtype: bool

        Upload a metadata doc. Metadata is different because of how indexes
        are stored.
        """

    @abc.abstractmethod
    def get_metadata(self, location):
        """
        :param location Location: Location obj
        :rtype: dict

        Download a metadata doc. Metadata is different because of how indexes
        are stored.
        """

    @abc.abstractmethod
    def close(self):
        """
        Delete all local cache to free memory
        """

    def find_one_id(self, prefix):
        """
        :param prefix Location: Location obj
        :param limit int:
        :rtype Location|None

        Convenience method for finding a single object given the prefix.
        Returns a Location if an object exists, otherwise, None
        """

        objs = self.list_ids(prefix, 1)
        if objs:
            return objs[0]
