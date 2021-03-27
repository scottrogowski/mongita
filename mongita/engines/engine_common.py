import abc



class Engine(abc.ABC):
    """
    Base class of every storage engine. This implements nothing.
    TODO lots of shit can be thrown from GCP / AWS and even from the filesystem.
    If we can't read or write, what do we do?
    Also, what can we do to ensure consistency? It's not a huge deal if some
    documents fail to upload as long as we throw an error. If those documents don't
    get into the index though, that is a bigger deal. Maybe we can make our first
    write after acquiring a lock something like "scheduled_update_index". Then, if we
    fail partway through, we can rebuild the index.

    After we delete the lockfile, we need to be doing something to make it clear
    that we are still working for long tasks. Maybe we can do something like updating
    the timestamp of a file?

    I could put this online with just the memory / memory+local options which
    won't require all of this nonsense because we control the process but that
    certainly doesn't solve this thing I started it all for.



    Options:
    - def acquire_lock. Perhaps try three times to acquire the lock before timeout.
        disadvantage is speed

    We acquire the lock by deleting the file.
    Then we upload a file that signifies the lock start
    Other clients get a 404 while we do our thing.
    We die
    Before the final timeout the client checks the lock start file
    Lock start file is old.

    We use the lock start file as our new lock and delete it.

    (other processes might upload a new lock file during this and the command
     might complete before we are done. Shit.)




    """

    @abc.abstractmethod
    def upload_doc(self, location, doc, generation=None):
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
    def download_doc(self, location):
        """
        :param location Location: Location obj
        :rtype: dict

        Given a location, download a doc
        If the doc doesn't exist, raise a MongitaError
        """

    @abc.abstractmethod
    def doc_exists(self, location):
        """
        :param location Location: Location obj
        :rtype: bool

        Given a location, return True if the object exists and False otherwise
        """

    @abc.abstractmethod
    def list_ids(self, prefix, limit, metadata):
        """
        :param prefix Location: Location obj|
        :param limit int:
        :param bool metadata:
        :rtype: list[str]

        Given a Location without an _id, return a list of _ids.
        If metadata is true, include metadata.
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
