import re
import logging

from umysqldb import err
from umysqldb._compat import range_type, text_type, PY2


RE_INSERT_VALUES = re.compile(r"""(INSERT\s.+\sVALUES\s+)(\(\s*%s\s*(?:,\s*%s\s*)*\))(\s*(?:ON DUPLICATE.*)?)\Z""",
                              re.IGNORECASE | re.DOTALL)

LOGGER = logging.getLogger()

class Cursor(object):
    '''This is the object you use to interact with the database.'''

    #: Max stetement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet-packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

    def __init__(self, connection):
        '''
        Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        '''
        self.connection = connection
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None

    def close(self):
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        conn = self.connection
        if conn is None:
            return
        self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()

    def _get_db(self):
        if not self.connection:
            raise err.ProgrammingError("Cursor closed")
        return self.connection

    def _check_executed(self):
        if not self._executed:
            raise err.ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def _nextset(self, unbuffered=False):
        """Get the next query set"""
        current_result = self._result
        if not current_result:
            return None
        self._do_get_result()
        return True

    def nextset(self):
        return self._nextset(False)

    def _escape_args(self, args, conn):
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return dict((key, conn.escape(val)) for (key, val) in args.items())
        else:
            #If it's not a dictionary let's try escaping it anyways.
            #Worst case it will throw a Value error
            return conn.escape(args)

    def mogrify(self, query, args=None):
        """
        Returns the exact string that is sent to the database by calling the
        execute() method.
        This method follows the extension to the DB API 2.0 followed by Psycopg.
        """
        conn = self._get_db()

        if PY2:  # Use bytes on Python 2 always
            encoding = conn.charset
            if 'utf8' in encoding:
                encoding = 'utf8'

            def ensure_bytes(x):
                if isinstance(x, unicode):
                    x = x.encode(encoding)
                return x

            query = ensure_bytes(query)

            if args is not None:
                if isinstance(args, (tuple, list)):
                    args = tuple(map(ensure_bytes, args))
                elif isinstance(args, dict):
                    args = dict((ensure_bytes(key), ensure_bytes(val)) for (key, val) in args.items())
                else:
                    args = ensure_bytes(args)

        if args is not None:
            query = query % self._escape_args(args, conn)

        return query

    def execute(self, query, args=None):
        '''Execute a query'''
        query = self.mogrify(query, args)
        result = self._query(query)
        self._executed = query
        return result

    def executemany(self, query, args):
        """Run several data against one query
        PyMySQL can execute bulkinsert for query like 'INSERT ... VALUES (%s)'.
        In other form of queries, just run :meth:`execute` many times.
        """
        if not args:
            return

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1)
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            return self._do_execute_many(q_prefix, q_values, q_postfix, args,
                                         self.max_stmt_length,
                                         self._get_db().encoding)

        self.rowcount = sum(self.execute(query, arg) for arg in args)
        return self.rowcount

    def _do_execute_many(self, prefix, values, postfix, args, max_stmt_length, encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, text_type):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, text_type):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, text_type):
            if PY2:
                v = v.encode(encoding)
            else:
                v = v.encode(encoding, 'surrogateescape')
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, text_type):
                if PY2:
                    v = v.encode(encoding)
                else:
                    v = v.encode(encoding, 'surrogateescape')
            if len(sql) + len(v) + len(postfix) + 1 > max_stmt_length:
                rows += self.execute(sql + postfix)
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        rows += self.execute(sql + postfix)
        self.rowcount = rows
        return rows

    def callproc(self, procname, args=()):
        """Execute stored procedure procname with args
        procname -- string, name of procedure to execute on server
        args -- Sequence of parameters to use with procedure
        Returns the original args.
        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.
        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected."""
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            self._query(q)

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range_type(len(args))]))
        self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        ''' Fetch the next row '''
        self._check_executed()
        if self._rows is None or self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        ''' Fetch several rows '''
        self._check_executed()
        if self._rows is None:
            return ()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._rows is None:
            return ()
        if self.rownumber:
            result = self._rows[self.rownumber:]
        else:
            result = self._rows
        self.rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        self._check_executed()
        if mode == 'relative':
            r = self.rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise err.ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self.rownumber = r

    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        try:
            conn.query(q)
        except:
            LOGGER.info("Query %s " % str(q))
            raise
        self._do_get_result()
        return self.rowcount

    def _do_get_result(self):
        conn = self._get_db()
        self.rownumber = 0
        self._result = result = conn._result
        if isinstance(self._result, tuple):
            self.rowcount = 1
            self._rows = [result]
        else:
            self.rowcount = len(result.rows)
            self._rows = result.rows

    def __iter__(self):
        return iter(self.fetchone, None)

    Warning = err.Warning
    Error = err.Error
    InterfaceError = err.InterfaceError
    DatabaseError = err.DatabaseError
    DataError = err.DataError
    OperationalError = err.OperationalError
    IntegrityError = err.IntegrityError
    InternalError = err.InternalError
    ProgrammingError = err.ProgrammingError
    NotSupportedError = err.NotSupportedError


class DictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        self._rows = list()
        if isinstance(self._result, tuple):
            return self._result
        for each in self._result.rows:
            tmp_res = dict()
            for index, fld in enumerate(self._result.fields):
                tmp_res[fld[0]] = each[index]
            self._rows.append(tmp_res)

class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""

class BaseCursor(Cursor):
    """Cursor just with a different name"""