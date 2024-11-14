#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php
#
# This code was inspired by:
#  * http://code.activestate.com/recipes/576638-draft-for-an-sqlite3-based-dbm/
#  * http://code.activestate.com/recipes/526618/

"""
A lightweight wrapper around Python's sqlite3 database, with a dict-like interface
and multi-thread access support::

>>> mydict = SqliteDict('some.db', autocommit=True) # the mapping will be persisted to file `some.db`
>>> mydict['some_key'] = {"A" : 1, "B" : {"a" : 1}} # any pickle struct
>>> print mydict['some_key']
>>> print len(mydict) # etc... all dict functions work

Pickle is used internally to serialize the values. Keys are strings.

If you don't use autocommit (default is no autocommit for performance), then
don't forget to call `mydict.commit()` when done with a transaction.

"""
import json
import logging
import os
import sqlite3
import sys
import tempfile
import traceback
from collections import UserDict as DictClass
from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from queue import Queue
from sqlite3 import Connection, Cursor
from threading import Thread

__version__ = "1.7.0"

from types import TracebackType

from typing import Tuple, Any, List, Optional, Type, Union, Generator, Set

from enum import Enum

def re_raise(tp: Any, value: Any, tb: Any = None) -> None:
    if value is None:
        value = tp()
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


logger = logging.getLogger(__name__)


def open(*args, **kwargs) -> "SqliteDict":
    """See documentation of the SqliteDict class."""
    return SqliteDict(*args, **kwargs)


def encode(obj: Any) -> Any:
    """Serialize an object using pickle to a binary format accepted by SQLite."""
    return sqlite3.Binary(dumps(obj, protocol=PICKLE_PROTOCOL))


def decode(obj: Any) -> Any:
    """Deserialize objects retrieved from SQLite."""
    return loads(bytes(obj))

class ResourcesOfQueue(Enum):
    NO_MORE: str = "--no more--"
    CLOSE: str = "--close--"
    COMMIT: str = "--commit--"

class SqliteDict(DictClass):
    VALID_FLAGS: Set[str] = {"c", "r", "w", "n"}

    def __init__(
        self,
        filename: str = "",
        tablename: str = "unnamed",
        flag: str = "c",
        autocommit: bool = False,
        journal_mode: str = "OFF",
        encode=encode,
        decode=decode,
        **kwargs,
    ) -> None:
        """
        Initialize a thread-safe sqlite-backed dictionary. The dictionary will
        be a table `tablename` in database file `filename`. A single file (=database)
        may contain multiple tables.

        If no `filename` is given, a random file in temp will be used (and deleted
        from temp once the dict is closed/deleted).

        If you enable `autocommit`, changes will be committed after each operation
        (more inefficient but safer). Otherwise, changes are committed on `self.commit()`,
        `self.clear()` and `self.close()`.

        Set `journal_mode` to 'OFF' if you're experiencing sqlite I/O problems
        or if you need performance and don't care about crash-consistency.

        The `flag` parameter. Exactly one of:
          'c': default mode, open for read/write, creating the db/table if necessary.
          'w': open for r/w, but drop `tablename` contents first (start with empty table)
          'r': open as read-only
          'n': create a new database (erasing any existing tables, not just `tablename`!).

        The `encode` and `decode` parameters are used to customize how the values
        are serialized and deserialized.
        The `encode` parameter must be a function that takes a single Python
        object and returns a serialized representation.
        The `decode` function must be a function that takes the serialized
        representation produced by `encode` and returns a deserialized Python
        object.
        The default is to use pickle.

        """
        super().__init__(**kwargs)
        if not filename:
            fd, filename = tempfile.mkstemp(prefix="sql_dict_")
            os.close(fd)

        if flag not in self.VALID_FLAGS:
            raise RuntimeError(f"Unrecognized flag: {flag}")
        self.flag = flag

        if flag == "n" and os.path.exists(filename):
            os.remove(filename)

        dirname = os.path.dirname(filename)
        if dirname and not os.path.exists(dirname):
            raise RuntimeError(f"Error! The directory {dirname} -> does not exist")

        self.filename = filename

        # Use standard SQL escaping of double quote characters in identifiers, by doubling them.
        # See https://github.com/RaRe-Technologies/sqlitedict/pull/113
        self.tablename = tablename.replace('"', '""')

        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.encode = encode
        self.decode = decode

        logger.info(f"opening Sqlite table {tablename} in {filename}")
        MAKE_TABLE = f'CREATE TABLE IF NOT EXISTS "{self.tablename}" (key TEXT PRIMARY KEY, value BLOB)'
        self.conn = self._new_conn()
        self.conn.execute(MAKE_TABLE)
        self.conn.commit()
        if flag == "w":
            self.clear()

    @staticmethod
    def __serialize_key(_key: Any) -> Any:
        if isinstance(_key, tuple):
            return json.dumps(list(_key))
        if isinstance(_key, frozenset):
            return json.dumps(list(_key))
        if isinstance(_key, set):
            return json.dumps(list(_key))
        if isinstance(_key, dict):
            return json.dumps(_key)
        return _key

    def _new_conn(self):
        return SqliteMultiThread(self.filename, autocommit=self.autocommit, journal_mode=self.journal_mode)

    def __enter__(self) -> object:
        if not hasattr(self, "conn") or self.conn is None:
            self.conn = self._new_conn()
        return self

    def __exit__(self, *exc_info: Tuple[Any, ...]) -> None:
        self.close()

    def __str__(self) -> str:
        return f"SqliteDict({self.filename})"

    def __repr__(self) -> str:
        return str(self)  # no need of something complex

    def __len__(self) -> Union[Any, int]:
        # `select count (*)` is super slow in sqlite (does a linear scan!!)
        # As a result, len() is very slow too once the table size grows beyond trivial.
        # We could keep the total count of rows ourselves, by means of triggers,
        # but that seems too complicated and would slow down normal operation
        # (insert/delete etc).
        GET_LEN = f'SELECT COUNT(*) FROM "{self.tablename}"'
        rows: Optional[Any] = self.conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __bool__(self) -> bool:
        # No elements is False, otherwise True
        GET_MAX = f'SELECT MAX(ROWID) FROM "{self.tablename}"'
        m = self.conn.select_one(GET_MAX)[0]
        # Explicit better than implicit and bla bla
        return True if m is not None else False

    def iterkeys(self) -> Generator[Any, Any, None]:
        GET_KEYS = f'SELECT key FROM "{self.tablename}" ORDER BY rowid'
        for key in self.conn.select(GET_KEYS):
            yield key[0]

    def itervalues(self):
        GET_VALUES = f'SELECT value FROM "{self.tablename}" ORDER BY rowid'
        for value in self.conn.select(GET_VALUES):
            yield self.decode(value[0])

    def iteritems(self):
        GET_ITEMS = f'SELECT key, value FROM "{self.tablename}" ORDER BY rowid'
        for key, value in self.conn.select(GET_ITEMS):
            yield key, self.decode(value)

    def keys(self):
        return self.iterkeys()

    def values(self):
        return self.itervalues()

    def items(self):
        return self.iteritems()

    def __contains__(self, key: Any) -> bool:
        key = self.__serialize_key(key)
        HAS_ITEM = f'SELECT 1 FROM "{self.tablename}" WHERE key = ?'
        return self.conn.select_one(HAS_ITEM, (key,)) is not None

    def __getitem__(self, key: Any) -> Any:
        key = self.__serialize_key(key)
        GET_ITEM = f'SELECT value FROM "{self.tablename}" WHERE key = ?'
        item = self.conn.select_one(GET_ITEM, (key,))
        if item is None:
            raise KeyError(key)
        return self.decode(item[0])

    def __setitem__(self, key: Any, value: Any) -> None:
        key = self.__serialize_key(key)
        if self.flag == "r":
            raise RuntimeError("Refusing to write to read-only SqliteDict")

        ADD_ITEM = f'REPLACE INTO "{self.tablename}" (key, value) VALUES (?,?)'
        self.conn.execute(ADD_ITEM, (key, self.encode(value)))
        if self.autocommit:
            self.commit()

    def __delitem__(self, key: Any) -> None:
        key = self.__serialize_key(key)
        if self.flag == "r":
            raise RuntimeError("Refusing to delete from read-only SqliteDict")

        if key not in self:
            raise KeyError(key)
        DEL_ITEM = f'DELETE FROM "{self.tablename}" WHERE key = ?'
        self.conn.execute(DEL_ITEM, (key,))
        if self.autocommit:
            self.commit()

    def update(self, items=(), **kwds):
        if self.flag == "r":
            raise RuntimeError("Refusing to update read-only SqliteDict")

        try:
            items = items.items()
        except AttributeError:
            pass

        items = [(k, self.encode(v)) for k, v in items]

        UPDATE_ITEMS = f'REPLACE INTO "{self.tablename}" (key, value) VALUES (?, ?)'
        self.conn.executemany(UPDATE_ITEMS, items)
        if kwds:
            self.update(kwds)
        if self.autocommit:
            self.commit()

    def __iter__(self):
        return self.iterkeys()

    def clear(self) -> None:
        if self.flag == "r":
            raise RuntimeError("Refusing to clear read-only SqliteDict")

        # avoid VACUUM, as it gives "OperationalError: database schema has changed"
        CLEAR_ALL = f'DELETE FROM "{self.tablename}";'
        self.conn.commit()
        self.conn.execute(CLEAR_ALL)
        self.conn.commit()

    @staticmethod
    def get_tablenames(filename: str) -> List[Any]:
        """get the names of the tables in an sqlite db as a list"""
        if not os.path.isfile(filename):
            raise IOError(f"file {filename} does not exist")
        GET_TABLENAMES = 'SELECT name FROM sqlite_master WHERE type="table"'
        with sqlite3.connect(filename) as conn:
            cursor = conn.execute(GET_TABLENAMES)
            res = cursor.fetchall()

        return [name[0] for name in res]

    def commit(self, blocking: bool = True) -> None:
        """
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        """
        if self.conn is not None:
            self.conn.commit(blocking)

    sync = commit

    def close(self, do_log: bool = True, force: bool = False) -> None:
        if do_log:
            logger.debug(f"closing {self}")
        if hasattr(self, "conn") and self.conn is not None:
            if self.conn.autocommit and not force:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit(blocking=True)
            self.conn.close(force=force)
            self.conn = None

    def terminate(self) -> None:
        """Delete the underlying database file. Use with care."""
        if self.flag == "r":
            raise RuntimeError("Refusing to terminate read-only SqliteDict")

        self.close()

        if self.filename == ":memory:":
            return

        logger.info(f"deleting {self.filename}")
        try:
            if os.path.isfile(self.filename):
                os.remove(self.filename)
        except (OSError, IOError):
            logger.exception(f"failed to delete {self.filename}")

    def __del__(self) -> None:
        # like close(), but assume globals are gone by now (do not log!)
        try:
            self.close(do_log=False, force=True)
        except (IOError, OSError):
            # prevent error log flood in case of multiple SqliteDicts
            # closed after connection lost (exceptions are always ignored
            # in __del__ method.
            pass


class SqliteMultiThread(Thread):
    """
    Wrap sqlite connection in a way that allows concurrent requests from multiple threads.

    This is done by internally queueing the requests and processing them sequentially
    in a separate thread (in the same order they arrived).

    """

    exception: Optional[Union[Tuple[Type[BaseException], BaseException, TracebackType], Tuple[None, None, None]]]

    def __init__(self, filename: str, autocommit: bool, journal_mode: str) -> None:
        super(SqliteMultiThread, self).__init__()
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        # use request queue of unlimited size
        self.reqs: Queue[Any] = Queue()
        self.setDaemon(True)  # python2.5-compatible
        self.exception = None
        self.log = logging.getLogger("sqlitedict.SqliteMultithread")
        self.start()

    def __db_connect(self) -> Connection:
        try:
            if self.autocommit:
                __conn = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
            else:
                __conn = sqlite3.connect(self.filename, check_same_thread=False)
        except Exception as err:
            self.log.exception(f"Failed to initialize connection for filename: {self.filename} -> {err}")
            self.exception = sys.exc_info()
            raise
        return __conn

    def __set_pragmas(self, conn: Connection) -> Tuple[Connection, Cursor]:
        try:
            conn.text_factory = str
            __cursor = conn.cursor()
            # set pragmas
            __cursor.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            __cursor.execute("PRAGMA synchronous = OFF")
            __cursor.execute("PRAGMA temp_store = MEMORY")
            __cursor.execute("PRAGMA mmap_size = 30000000000")
            __cursor.execute('PRAGMA encoding = "UTF-8"')
            #
            conn.commit()

        except Exception as err:
            self.log.exception(f"Failed to execute PRAGMA statements -> {err}")
            self.exception = sys.exc_info()
            raise
        return conn, __cursor

    def run(self) -> None:

        _conn = self.__db_connect()
        conn, cursor = self.__set_pragmas(conn=_conn)

        while True:
            req, arg, res, outer_stack = self.reqs.get()
            if req == ResourcesOfQueue.CLOSE:
                assert res, (f"{ResourcesOfQueue.CLOSE} without return queue", res)
                break
            elif req == ResourcesOfQueue.COMMIT:
                conn.commit()
                if res:
                    res.put(ResourcesOfQueue.NO_MORE)
            else:
                try:
                    cursor.execute(req, arg)
                except sqlite3.Error as _:
                    self.exception = (e_type, e_value, e_tb) = sys.exc_info()
                    inner_stack = traceback.extract_stack()

                    # An exception occurred in our thread, but we may not
                    # immediately able to throw it in our calling thread, if it has
                    # no return `res` queue: log as level ERROR both the inner and
                    # outer exception immediately.
                    #
                    # Any iteration of res.get() or any next call will detect the
                    # inner exception and re-raise it in the calling Thread; though
                    # it may be confusing to see an exception for an unrelated
                    # statement, an ERROR log statement from the 'sqlitedict.*'
                    # namespace contains the original outer stack location.
                    self.log.error("Inner exception:")
                    for item in traceback.format_list(inner_stack):
                        self.log.error(item)
                    self.log.error("")  # separate traceback & exception w/blank line
                    for item in traceback.format_exception_only(e_type, e_value):
                        self.log.error(item)

                    self.log.error("")  # exception & outer stack w/blank line
                    self.log.error("Outer stack:")
                    for item in traceback.format_list(outer_stack):
                        self.log.error(item)
                    self.log.error("Exception will be re-raised at next call.")

                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put(ResourcesOfQueue.NO_MORE)

                if self.autocommit:
                    conn.commit()

        self.log.debug(f"received: {req}, send: {ResourcesOfQueue.NO_MORE}")
        conn.close()
        res.put(ResourcesOfQueue.NO_MORE)

    def check_raise_error(self) -> None:
        """
        Check for and raise exception for any previous sqlite query.

        For the `execute*` family of method calls, such calls are non-blocking and any
        exception raised in the thread cannot be handled by the calling Thread (usually
        MainThread).  This method is called on `close`, and prior to any subsequent
        calls to the `execute*` methods to check for and raise an exception in a
        previous call to the MainThread.
        """
        if self.exception:
            e_type, e_value, e_tb = self.exception

            # clear self.exception, if the caller decides to handle such
            # exception, we should not repeatedly re-raise it.
            self.exception = None

            self.log.error(
                "An exception occurred from a previous statement, view "
                'the logging namespace "sqlitedict" for outer stack.'
            )

            # The third argument to raise is the traceback object, and it is
            # substituted instead of the current location as the place where
            # the exception occurred, this is so that when using debuggers such
            # as `pdb', or simply evaluating the naturally raised traceback, we
            # retain the original (inner) location of where the exception
            # occurred.
            re_raise(e_type, e_value, e_tb)

    def execute(self, req: Any, arg: Any = None, res: Any = None) -> None:
        """
        `execute` calls are non-blocking: just queue up the request and return immediately.
        """
        self.check_raise_error()

        # NOTE: This might be a lot of information to pump into an input
        # queue, affecting performance.  I've also seen earlier versions of
        # jython take a severe performance impact for throwing exceptions
        # so often.
        stack = traceback.extract_stack()[:-1]
        self.reqs.put((req, arg or tuple(), res, stack))

    def executemany(self, req: Any, items: List[Any]) -> None:
        for item in items:
            self.execute(req, item)
        self.check_raise_error()

    def select(self, req: Any, arg: Any = None) -> Any:
        """
        Unlike sqlite's native select, this select doesn't handle iteration efficiently.

        The result of `select` starts filling up with values as soon as the
        request is dequeued, and although you can iterate over the result normally
        (`for res in self.select(): ...`), the entire result will be in memory.
        """
        res: Queue[Any] = Queue()  # results of the select will appear as items in this queue
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            self.check_raise_error()
            if rec == ResourcesOfQueue.NO_MORE:
                break
            yield rec

    def select_one(self, req: Any, arg: Any = None) -> Optional[Any]:
        """Return only the first row of the SELECT, or None if there are no matching rows."""
        try:
            return next(iter(self.select(req, arg)))
        except StopIteration:
            return None

    def commit(self, blocking: bool = True) -> None:
        if blocking:
            # by default, we await completion of commit() unless
            # blocking=False.  This ensures any available exceptions for any
            # previous statement are thrown before returning, and that the
            # data has actually persisted to disk!
            self.select_one(ResourcesOfQueue.COMMIT)
        else:
            # otherwise, we fire and forget as usual.
            self.execute(ResourcesOfQueue.COMMIT)

    def close(self, force: bool = False) -> None:
        if force:
            # If a SqliteDict is being killed or garbage-collected, then select_one()
            # could hang forever because run() might already have exited and therefore
            # can't process the request. Instead, push the close command to the requests
            # queue directly. If run() is still alive, it will exit gracefully. If not,
            # then there's nothing we can do anyway.
            self.reqs.put((ResourcesOfQueue.CLOSE, None, Queue(), None))
        else:
            # we abuse 'select' to "iter" over a "--close--" statement so that we
            # can confirm the completion of close before joining the thread and
            # returning (by semaphore '--no more--'
            self.select_one(ResourcesOfQueue.CLOSE)
            self.join()


# endclass SqliteMultithread