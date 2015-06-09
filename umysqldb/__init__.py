from umysqldb import connections
from umysqldb import cursors


def connect(db, host="localhost", port=3306, user="root", passwd="root",
            charset="utf8", cursorclass=cursors.Cursor, autocommit=False):
    return connections.Connection(database=db, host=host, port=port, user=user,
                                  passwd=passwd, charset=charset,
                                  cursorclass=cursorclass,
                                  autocommit=autocommit)

Connection = Connect = connect