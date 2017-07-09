import time

from cabbage import *

db = MySQLDatabase(
    'peewee_test',
    host = '192.168.99.110',
    port = 3306,
    user = 'root',
    passwd = 'Fenglovehuihui!@#123',
    charset = 'utf8',
    autocommit = True
)

class Ttt(Model):
    count = IntegerField()
    #num = IntegerField()

    class Meta:
        database = db

db.connect()

# ----insert----
#ttt = Ttt(count=8)
#ttt.save()

# ----select----
#print(Ttt.select().execute().get())
for temp in Ttt.select().execute().get():
    print(temp.count)
db.close()
