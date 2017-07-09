import time

from cabbage import *

db = MySQLDatabase(
    'peewee_test',
    host = '192.168.99.110',
    port = 3306,
    user = 'root',
    passwd = 'xxx',
    charset = 'utf8',
    autocommit = True
)

class Ttt(Model):
    count = IntegerField()
    num = IntegerField()

    class Meta:
        database = db

db.connect()

#----insert----
ttt = Ttt(count=8, num=0)
ttt.save()
ttt = Ttt(count=7, num=0)
ttt.save()

#----select----
#print(Ttt.select().execute().get())
for temp in Ttt.select("count", "num").where(count=8).execute().get():
    print(temp.count, temp.num)

#----update----
Ttt.update(count=0, num=9).where(count=8, num=0).execute()

for temp in Ttt.select().execute().get():
    print(temp.count)

#----delete----
Ttt.delete().where(count=7).execute()
db.close()
