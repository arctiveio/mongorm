OnModelInit = None

def pack(_val):
    if isinstance(_val, DbDictClass):
        return _val

    elif isinstance(_val, dict):
        return DbDictClass(_val)

    elif hasattr(_val, '__iter__'):
        return [pack(v) for v in _val]

    return _val


class DbDictClass(dict):

    def __getattribute__(self, key):
        try:
            return self[key]
        except KeyError:
            pass
        return super(DbDictClass, self).__getattribute__(key)

    def __setattr__(self, key, value):
        self[key] = pack(value)

    def __delattr__(self, key):
        self.pop(key)

    def copy(self, dbdict=False):
        if dbdict:
            print "DbDict copy is expensive."

        def _format(t, dbdict=False):
            if isinstance(t, (DbDictClass, dict)):
                _obj = {} if not dbdict else DbDictClass({})
                for k, v in t.iteritems():
                    _obj[k] = _format(v, dbdict=dbdict)
                return _obj
            elif isinstance(t, list):
                return [_format(x, dbdict=dbdict) for x in t]
            return t

        return _format(self, dbdict=dbdict)


class ModelDefinition(DbDictClass):
    __baseclass__ = True

    @classmethod
    def using(cls):
        raise NotImplementedError

    @classmethod
    def now(cls):
        raise NotImplementedError

    @classmethod
    def generate_id(cls):
        raise NotImplementedError

    @classmethod
    def insert(cls, document):
        raise NotImplementedError

    @classmethod
    def aggregate(cls, commands):
        raise NotImplementedError

    @classmethod
    def group(cls, *args, **kwargs):
        raise NotImplementedError

    def save(self, validate=True):
        raise NotImplementedError

    @classmethod
    def find_and_modify(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def count(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_one(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_many(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def remove(cls, _id, *args, **kwargs):
        raise NotImplementedError

    def delete(cls, *args, **kwargs):
        raise NotImplementedError


class DataTypeDefinition(object):
    def humanize(cls, value):
        raise NotImplementedError

    def dbfy(self, value):
        raise NotImplementedError

class ModelMeta(type):

    def get_field_defaults(cls, field):
        return cls.defaults.get(field)

    def get_field_choices(cls, field):
        return cls.choices.get(field)

    def attach_fields(cls, model):
        for (field_name, obj) in vars(model).items():
            if not isinstance(obj, DataTypeDefinition):
                continue

            cls.fields[field_name] = obj
            if getattr(obj, 'default', None) is not None:
                cls.defaults.update({field_name: obj.default})
            if getattr(obj, 'choices', None) is not None:
                cls.choices.update({field_name: obj.choices})
            if obj.nullable is False:
                cls.required_fields.add(field_name)
            if obj.searchable is True:
                cls.searchable_fields.append(field_name)

        cls.searchable_fields = list(cls.searchable_fields)

    def __init__(cls, name, base, attrs):
        if cls == ModelDefinition or attrs.get('__dyn__'):
            return

        if not issubclass(cls, ModelDefinition):
            return

        cls.fields = {}
        cls.defaults = {}
        cls.choices = {}
        cls.required_fields = set()
        cls.searchable_fields = []

        if not attrs.get('__baseclass__') and attrs.get("__tablename__"):
            if callable(OnModelInit):
                OnModelInit(cls, name, attrs)

            cls.__tablename__ = attrs["__tablename__"]

        elif not attrs.get("__baseclass__"):
            print ("Skipping Model %s."
                   " Does not expose __tablename__."
                   " This is an error or an inherited model." % name)

        for model in base:
            if model == ModelDefinition:
                continue

            if issubclass(model, ModelDefinition):
                #if "__baseclass__" in model.__dict__:
                #    continue

                cls.fields.update(model.fields)
                cls.defaults.update(model.defaults)
                cls.required_fields.update(model.required_fields)
                cls.searchable_fields.extend(model.searchable_fields)

            else:
                cls.attach_fields(model)

        cls.attach_fields(cls)
