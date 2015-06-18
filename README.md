# pyumysql
python wrapper for ultramysql for testing of migration from MySQLdb to ultramysql

## Installation
Installation of ultamysql is optional, but I was doing it because version in PyPi was without utf8mb4 support
```
git clone https://github.com/arozumenko/ultramysql.git
cd ultramysql/
python setup.py build install
```

After that you just need to do ```python setup.py install```

##Usage
```
>>> import umysqldb
>>> conn = umysqldb.connect(db="bar", host="127.0.0.1", port=3306, user="root", passwd="")
>>> cur = conn.cursor(cursor=umysqldb.cursors.DictCursor)
>>> cur.execute("select * from foo;")
11897
>>> cur.fetchone()
{'uuid': u'1c6c526a-2031-4e5f-886d-0f264e8984b7', 'created': 1433181924, 'another_rnd': 72196, 'rnd': 216, 'ID': 0, 'zero_one': 1}
>>> cur.close()
>>> conn.close()
 ```
