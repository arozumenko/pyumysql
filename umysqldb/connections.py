#  Copyright (c) 2015 Artem Rozumenko (artyom.rozumenko@gmail.com)
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

__author__ = 'Artem Rozumenko - artyom.rozumenko@gmail.com'

import umysql
from umysqldb.cursors import Cursor
from umysqldb._compat import PY2, text_type, str_type, \
    JYTHON, IRONPYTHON
from umysqldb.converters import escape_item, escape_string
from umysqldb import err

class Connection(object):
    """
    Representation of a socket with a mysql server.
    The proper way to get an instance of this class is to call
    connect().
    """

    socket = None
    def __init__(self, host="localhost", user="root", password="",
                 database=None, port=3306, charset='utf8', cursorclass=Cursor,
                 db=None, passwd="", autocommit=False):
        self.user = user
        self.password = password if password else passwd
        self.db = database if database else db
        self.host = host
        self.port = port
        self.charset = charset
        self.autocommit = autocommit
        self.cursorclass = cursorclass
        self._connection = None
        self._connect()
        self._result = None

    def _connect(self):
        self._connection = umysql.Connection()
        self._connection.connect(self.host, self.port, self.user,
                                 self.password, self.db, self.autocommit,
                                 self.charset)

    def close(self):
        self._connection.close()

    def autocommit(self, value):
        if value != self.autocommit:
            self.autocommit = value
            self._send_autocommit_mode()

    def _send_autocommit_mode(self):
        """Set whether or not to commit after every execute()"""
        self._connection.query("SET AUTOCOMMIT = %s"
                               "" % self.escape(self.autocommit))

    def escape(self, obj, mapping=None):
        """Escape whatever value you pass to it"""
        if isinstance(obj, str_type):
            if "'" in obj:
                return "'" + self.escape_string(obj) + "'"
        return escape_item(obj, self.charset, mapping=mapping)

    def is_succsess(self, res):
        if isinstance(res, tuple):
            if res[0] == 0 and res[1] == 0:
                return True
        raise err.OperationalError

    def begin(self):
        self.is_succsess(self._connection.query('BEGIN'))

    def commit(self):
        """Commit changes to stable storage"""
        self.is_succsess(self._connection.query('COMMIT'))

    def rollback(self):
        """Roll back the current transaction"""
        self.is_succsess(self._connection.query('ROLLBACK'))

    def select_db(self, db):
        '''Set current db'''
        self.is_succsess(self._connection.query('use %s' % str(db)))

    def show_warnings(self):
        """SHOW WARNINGS"""
        return self._connection.query('SHOW WARNINGS').rows

    def cursor(self, cursor=None):
        """Create a new cursor to execute queries with"""
        if cursor:
            return cursor(self)
        return self.cursorclass(self)

    def escape_string(self, s):
        return escape_string(s)


    def query(self, sql):
        if isinstance(sql, text_type) and not (JYTHON or IRONPYTHON):
            encoding = self.charset if not 'utf8' in self.charset else 'utf8'
            if PY2:
                sql = sql.encode(encoding)
            else:
                sql = sql.encode(encoding, 'surrogateescape')
        self._result = self._connection.query(sql)


