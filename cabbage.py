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

        cls._data = {}
        cls._dirty = set()
        for attr, name in fields:
            cls._data[name] = attr._default
            attr.add_to_class(cls, name)

        return cls

class Model(with_metaclass(BaseModel)):
    @classmethod
    def create(cls, sql):
        pass

    @classmethod
    def select(cls, sql, args):
        cls.database.execute_sql(sql, args)
        cls.database.commit()

    @classmethod
    def execute(cls):
        pass


class Database(object):
    def __init__(self, database, **connect_kwargs):
        self._database = database
        self._connect_kwargs = connect_kwargs
        self._conn = None
        self._cursor = None

    def connect(self):
        if not self._conn:
            self._conn = self._connect(self._database, **self._connect_kwargs)

    def _connect(self, database, **connect_kwargs):
        raise NotImplementedError

    def _get_cursor(self):
        #raise NotImplementedError
        return self._conn.cursor()

    def begin(self):
        raise NotImplementedError

    def commit(self):
        #raise NotImplementedError
        self._conn.commit()

    def rollback(self):
        #raise NotImplementedError
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

    def _connect(self, database, **connect_kwargs):
        if not mysql:
            raise EnvironmentError("MySQLdb or pymysql must be installed!")
        connect_kwargs['database'] = database
        return mysql.connect(**connect_kwargs)
