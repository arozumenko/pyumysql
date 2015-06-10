#  Copyright (c) 2014 Artem Rozumenko (artyom.rozumenko@gmail.com)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import umysql
import logging
from umysql import SQLError

logger = logging.getLogger()

class PyUltraMySQL(object):
    ##################################################
    #
    # Wrapper around Ultra SQL to implement pymysql apis
    #
    ##################################################
    def __init__(self, db_database, db_host=None, db_port=None,
                 db_user=None, db_password=None, charset='utf8',
                 cursorclass='base', autocommit=False):
        self.db_user = db_user if db_user else "root"
        self.db_password = db_password if isinstance(db_password, basestring) \
            else ""
        self.db_host = db_host if db_host else "localhost"
        self.db_port = db_port if db_port else 3306
        self.__connect__ = umysql.Connection()
        self.__autocommit__ = autocommit
        self.db_database = db_database
        self.charset = charset
        self.__cursor__ = cursorclass # dict, base, etc.
        if self.db_database:
            self.__connect__.connect(self.db_host, self.db_port,
                                     self.db_user, self.db_password,
                                     self.db_database, self.__autocommit__,
                                     self.charset)
        self.res = None


    @property
    def DictCursor(self):
        self.__cursor__ = "dict"
        return self

    @property
    def Cursor(self):
        self.__cursor__ = "list"
        return self

    @property
    def BaseCursor(self):
        self.__cursor__ = "base"
        return self

    def autocommit(self, bool_val):
        self.__cursor__ = bool_val

    def cursor(self, cursor=None):
        """ Basically just a mock for cursor. """
        if cursor:
            self.__cursor__ = cursor
        return self

    def select_db(self, db):
        """Set db"""
        self.execute("use %s" % db)

    def close(self):
        # try:
        #     self.__connect__.close()
        # except:
        #     pass
        pass

    def commit(self):
        if not self.__autocommit__:
            self.execute("COMMIT")

    @staticmethod
    def _transform_to_json(result):
        res_json = list()
        for each in result.rows:
            tmp_res = dict()
            for index, fld in enumerate(result.fields):
                tmp_res[fld[0]] = each[index]
            res_json.append(tmp_res)
        return res_json

    def execute(self, query, args=None):
        if args is not None and not isinstance(args, (tuple, list)):
            args = (args,)

        try:
            if args:
                #logging.debug(query % args)
                self.res = self.__connect__.query(query, args)
            else:
                #logging.debug(query)
                self.res = self.__connect__.query(query)
        except (ValueError, SQLError):
            logging.error("This was an exception: %s \n Args: "
                          "%s" % (query, args))
            raise

        if not isinstance(self.res, tuple):
            if self.__cursor__ == 'dict':
                self.res = self._transform_to_json(self.res)
            elif self.__cursor__ in ('base', 'list'):
                self.res = self.res.rows

        else:
            logging.debug("Tuple res: %s" % str(self.res))
            logging.debug("Query was: %s" % str(query))

    def fetch_all(self):
        return self.res

    def fetch_one(self):
        return self.res.pop() if self.res else []

    @property
    def rowcount(self):
        return len(self.res)

    def fetch_row(self):
        return self.fetch_one()

    def fetchall(self):
        return self.fetch_all()

    def fetchone(self):
        return self.fetch_one()

    def fetchrow(self):
        return self.fetch_one()


class cursors(object):
    DictCursor = "dict"
    Cursor = "list"
    BaseCursor = "base"


def connect(db, host="localhost", port=3306, user="root", passwd="root",
            charset="utf8", cursorclass='base', autocommit=False):
    return PyUltraMySQL(db, host, port, user, passwd, charset, cursorclass,
                        autocommit)


Connection = Connect = connect

paramstyle = "format"

class MySQLError(StandardError):

    """Exception related to operation with MySQL."""

class Error(MySQLError):

    """Exception that is the base class of all other error exceptions
    (not Warning)."""

class DatabaseError(Error):

    """Exception raised for errors that are related to the
    database."""

class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's
        operation and not necessarily under the control of the programmer,
        e.g. an unexpected disconnect occurs, the data source name is not
        found, a transaction could not be processed, a memory allocation
        error occurred during processing, etc.
    """
    def __init__(self, *args, **kwargs): # real signature unknown
        pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database
        is affected, e.g. a foreign key check fails, duplicate key,
        etc.
    """
    def __init__(self, *args, **kwargs): # real signature unknown
        pass