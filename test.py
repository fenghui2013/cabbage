import time

from cabbage import *

db = MySQLDatabase(
    'peewee_test',
    host = '192.168.99.110',
    port = 3306,
    user = 'root',
    passwd = 'Fenglovehuihui!@#123',
    charset = 'utf8',
    autocommit = False
)

class Ttt(Model):
    count = IntegerField()

    class Meta:
        database = db

db.connect()
#time.sleep(30)
Ttt.select("update ttt set count=%s where count<%s", (5, 10000,))
ttt = Ttt()
print(type(ttt.count), ttt.count)
ttt.count = 10
print(type(ttt.count), ttt.count)
ttt.count = 100
print(type(ttt.count), ttt.count)
db.close()
