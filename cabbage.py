try:
    import MySQLdb as mysql
except ImportError:
    try:
        import pymysql as mysql
    except ImportError:
        mysql = None

_METACLASS = "metaclass_helper"
def with_metaclass(meta, base=object):
    return meta(_METACLASS, (base,), {})

class CabbageException(Exception): pass
class EnvironmentError(CabbageException): pass

class FieldDescriptor(object):
    def __init__(self, field):
        self.field = field
        self.att_name = self.field.name

    def __get__(self, instance, instance_type):
        return instance._data[self.att_name]

    def __set__(self, instance, value):
        instance._data[self.att_name] = value
        instance._dirty.add(self.att_name)

class Field(object):
    def __init__(self, null=True, index=False, unique=False, default=0, primary_key=False):
        self._null = null
        self._index = index
        self._unique = unique
        self._default = default
        self._primary_key = primary_key

    def add_to_class(self, model_class, name):
        self.name = name
        self.model_class = model_class
        setattr(model_class, name, FieldDescriptor(self))


class IntegerField(Field):
    def __init__(self, null=True, index=False, unique=False, default=0, primary_key=False):
        super(IntegerField, self).__init__(null, index, unique, default, primary_key)

class Operation(object):
    def __init__(self, table, op_data):
        self._table = table
        self._op_data = op_data
        self._values = []
        self._sql = None
        self.sql()

    def execute(self):
        self._table.database.execute_sql(self._sql, self._values)
        return self

    def where(self, **where_data):
        self._sql += " WHERE "
        for k, v in where_data.items():
            self._sql += (k + " = %s and ")
            self._values.append(v)
        self._sql = self._sql[:-5]
        print(self._sql)
        return self

    def get_sql(self):
        return self._sql

class Insert(Operation):
    def __init__(self, table, insert_data):
        super(Insert, self).__init__(table, insert_data)

    def sql(self):
        self._sql = ("INSERT INTO " + self._table.__name__ + " ")
        keys = []
        placeholder = []
        for k, v in self._op_data.items():
            keys.append(k)
            self._values.append(v)
            placeholder.append("%s")
        self._sql += ("(" + ", ".join(keys) + ")")
        self._sql += (" VALUES (" + ", ".join(placeholder) + ")")


class Delete(Operation):
    def __init__(self, table, delete_data=None):
        super(Delete, self).__init__(table, delete_data)

    def sql(self):
        self._sql = ("DELETE FROM " + self._table.__name__)

class Update(Operation):
    def __init__(self, table, update_data):
        super(Update, self).__init__(table, update_data)

    def sql(self):
        self._sql = ("UPDATE " + self._table.__name__ + " SET ")
        for k, v in self._op_data.items():
            self._sql += (k + " = %s, ")
            self._values.append(v)
        self._sql = self._sql[:-2]

class Select(Operation):
    def __init__(self, table, select_data):
        super(Select, self).__init__(table, select_data)

    def sql(self):
        self._sql = ("SELECT " + ", ".join(self._op_data))
        self._sql += (" FROM " + self._table.__name__)

    def get(self):
        res = []
        for res_tuple in self._table.database.get_all():
            _instance = self._table()
            for index, field_name in enumerate(self._op_data):
                setattr(_instance, field_name, res_tuple[index])
            res.append(_instance)

        return res

class BaseModel(type):
    def __new__(cls, name, bases, attrs):
        if name == "metaclass_helper":
            return super(BaseModel, cls).__new__(cls, name, bases, attrs)

        meta = attrs.pop("Meta", None)
        if meta:
            for k, v in meta.__dict__.items():
                if not k.startswith("_"):
                    attrs[k] = v

        cls = super(BaseModel, cls).__new__(cls, name, bases, attrs)
        
        fields = []
        for name, attr in cls.__dict__.items():
            if isinstance(attr, Field):
                fields.append((attr, name,))

        #cls._data = {}
        #cls._dirty = set()
        cls._fields = []
        for attr, name in fields:
            #cls._data[name] = attr._default
            cls._fields.append(name)
            setattr(cls, name, attr._default)
            attr.add_to_class(cls, name)

        return cls


class Model(with_metaclass(BaseModel)):
    def __init__(self, *args, **kwargs):
        self._data = {}
        self._dirty = set()
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def insert(cls, __data=None, **insert):
        insert_data = __data or {}
        insert_data.update([(k, insert[k]) for k in insert.keys()])
        return Insert(cls, insert_data)

    @classmethod
    def delete(cls):
        return Delete(cls)

    @classmethod
    def select(cls, *select):
        select_data = []
        if select:
            select_data.extend(select)
        else:
            select_data.extend(cls._fields)
        return Select(cls, select_data)

    @classmethod
    def update(cls, **update):
        update_data = {}
        update_data.update(update)
        return Update(cls, update_data)

    def save(self):
        field_dict = dict(self._data)
        res = self.insert(**field_dict).execute()
        self._dirty.clear()
        return res

    @classmethod
    def execute(cls):
        pass


class Database(object):
    def __init__(self, database, **connect_kwargs):
        self._database = database
        self._connect_kwargs = connect_kwargs
        self._conn = self._connect(database, **connect_kwargs)
        self._cursor = self._get_cursor()

    def connect(self):
        if not self._conn:
            self._conn = self._connect(self._database, **self._connect_kwargs)

    def _connect(self, database, **connect_kwargs):
        raise NotImplementedError

    def _get_cursor(self):
        return self._conn.cursor()

    def get_all(self):
        return self._cursor.fetchall()

    def get_many(self, size):
        return self._cursor.fetchmany(size=size)

    def get_one(self):
        return self._cursor.fetchone()

    def begin(self):
        raise NotImplementedError

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def execute_sql(self, sql, args=None):
        if not self._cursor:
            self._cursor = self._get_cursor()
        return self._cursor.execute(sql, args)

    def close(self):
        self._conn.close()

class MySQLDatabase(Database):
    def __init__(self, database, **connect_kwargs):
        super(MySQLDatabase, self).__init__(database, **connect_kwargs)

    def begin(self):
        self._conn.begin()

    def _connect(self, database, **connect_kwargs):
        if not mysql:
            raise EnvironmentError("MySQLdb or pymysql must be installed!")
        connect_kwargs['database'] = database
        return mysql.connect(**connect_kwargs)
