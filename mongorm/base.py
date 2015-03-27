import re
import pymongo
import datetime

from .errors import ORMException
from .meta import ModelMeta, DbDictClass, ModelDefinition
from .datatypes import ObjectId, ID, Boolean, DataType, List, Dict


class ModelBase(ModelDefinition):
    __baseclass__ = True
    __metaclass__ = ModelMeta

    _id = ID(nullable=False)
    deleted = Boolean(default=False)

    @classmethod
    def valid_database(cls):
        using = cls.using()
        if not using:
            raise ORMException(
                '''
                Error in model %s. Using is a required attribute.
                ''' % cls.__name__)

        if not isinstance(using, pymongo.database.Database):
            raise ORMException(
                '''
                Error in model %s.
                Using must be of type pymongo.database.Database
                ''' % cls.__name__)

        return using

    @classmethod
    def using(cls):
        raise NotImplementedError

    def validate(cls):
        pass

    def pre_save(cls):
        pass

    def post_save(cls):
        pass

    def __init__(self, partial_model=False, *args, **kwargs):
        if args or not isinstance(partial_model, bool):
            raise AttributeError("args not supported. Use kwargs instead.")

        # NOTE: Delegating update to dict's update method.
        self.__dict__['update'] = super(ModelBase, self).update
        if partial_model:
            super(ModelBase, self).__init__(kwargs)

        else:
            params = dict(self.defaults, **kwargs)
            super(ModelBase, self).__init__(params)

    def __getattribute__(self, key):
        if (key not in ['fields', 'keys']) and \
           (key in self.fields) and \
           (key not in self.keys()):
            raise AttributeError(
                "%s object has no attribute %s" % (self, key))

        return super(ModelBase, self).__getattribute__(key)

    @classmethod
    def now(cls):
        return datetime.datetime.utcnow()

    @classmethod
    def generate_id(cls):
        return ObjectId()

    @classmethod
    def mongo_collection(cls, database):
        return getattr(database, cls.__tablename__)

    @classmethod
    def validate_type(cls, data_dict, check_required=True):
        model_keys = []
        errors = []

        if check_required:
            for field in cls.required_fields:
                if field not in data_dict:
                    if field in cls.defaults:
                        data_dict[field] = cls.defaults[field]
                    else:
                        errors.append("%s is a required field" % field)

        if not errors:
            for key, value in data_dict.iteritems():
                key_split = key.split('.')
                check_key = key_split[0]
                model_keys.append(check_key)
                typeobj = cls.fields.get(check_key, None)

                if not typeobj or not isinstance(typeobj, DataType):
                    continue

                if len(key_split) > 1:
                    # dots can be used for settings value in array.
                    # using index as key. i.e. {'$set': {myarray.0: 'val'}}
                    # should be allowed
                    if typeobj.datatype == dict or typeobj.datatype == list:
                        continue
                    errors.append("%s should be of type %s. "
                                  % (key, typeobj.datatype))

                try:
                    data_dict[key] = typeobj.dbfy(value)
                except Exception, e:
                    error = getattr(e, "log_message", None) or \
                        getattr(e, "error_message", None) or \
                        "Expected %s. Found %s" % (typeobj.datatype, value)

                    errors.append("Field: %s, Error: %s" % (key, error))

        if errors:
            raise ORMException(errors)

        return model_keys

    @classmethod
    def insert(cls, document):
        if not document:
            return [None, []][document == []]

        documents = document if isinstance(document, list) else [document]
        validated_docs = []

        for d in documents:
            d['_id'] = d.get("_id") or cls.generate_id()
            d['created_on'] = cls.now()
            d['modified_on'] = cls.now()

            cls.prepare_insert_document(d)

            document = dict(cls.defaults, **d)
            cls.validate_type(document)
            validated_docs.append(document)

        if not validated_docs:
            return [None, []][validated_docs == []]

        database = cls.valid_database()
        call = cls.mongo_collection(database)
        ids = call.insert(validated_docs)

        cls.on_insert(ids)
        return ids

    @classmethod
    def aggregate(cls, commands):
        if not isinstance(commands, list):
            raise ORMException(
                "Aggregate accepts only a List of commands as arguments")

        database = cls.valid_database()
        call = cls.mongo_collection(database)
        return call.aggregate(commands)

    @classmethod
    def group(cls, *args, **kwargs):
        database = cls.valid_database()
        call = cls.mongo_collection(database)
        return call.group(*args, **kwargs)

    def prepare_save_document(cls):
        pass

    @classmethod
    def prepare_delete_document(cls, document):
        pass

    @classmethod
    def prepare_update_document(cls, document):
        pass

    @classmethod
    def prepare_insert_document(cls, document):
        pass

    @classmethod
    def prepare_update_query(cls, filter_args):
        pass

    @classmethod
    def prepare_get_query(cls, filter_args):
        pass

    @classmethod
    def on_insert(cls, ids):
        pass

    @classmethod
    def on_update(cls, filter_args, document, updated_fields=None):
        pass

    def save(self, validate=True):
        if callable(self.pre_save):
            self.pre_save()

        database = self.valid_database()

        self.modified_on = self.now()
        existing_id = self.get('_id')
        existing_created_on = self.get("created_on")

        self.prepare_save_document()

        if not existing_id or isinstance(existing_id, DataType):
            self._id = self.generate_id()

        if not existing_created_on:
            self.created_on = self.now()

        if validate:
            self.validate_type(self)

        call = self.mongo_collection(database)

        if existing_id:
            document = {'$set': self}
            filter_args = {"_id": existing_id}

            self.prepare_update_document(document)
            self.prepare_update_query(filter_args)

            self.on_update(filter_args, document, updated_fields=self.keys())
        else:
            self.prepare_insert_document(self)
            self.on_insert(self._id)

        try:
            call.save(self)
        except pymongo.errors.DuplicateKeyError, e:
            pattern = re.compile(
                r'.+\s+(?P<db_name>\w+)\.(?P<table_name>\w+)\.\$(?P<field_name>\w+)_\d+.*\"(?P<value>.*)\".*}.*')
            errors = pattern.findall(e.args[0])
            if errors:
                error_list = map(lambda i: "%s already exists for value %s" %
                                 (i[2], i[3]), errors)

                raise ORMException(error_list)

            raise ORMException('%s' % e.args[0])

        if callable(self.post_save):
            self.post_save()

        return self

    @classmethod
    def __update(cls, filter_args, document, silent=False, **kwargs):
        errors = []
        updated_fields = []
        document = document or {}

        if not document:
            return False

        database = cls.valid_database()
        call = cls.mongo_collection(database)

        if not cls.fields:
            return call, filter_args, document, kwargs

        for op, value in document.iteritems():
            if op == '$set':
                model_keys = cls.validate_type(value, check_required=False)
                updated_fields.extend(model_keys)

            elif op == '$unset':
                for field in value:
                    fields = field.split('.')
                    if len(fields) == 1 and fields[0] in cls.required_fields:
                        errors.append('%s is a required field' % fields[0])
                    else:
                        updated_fields.append(fields[0])

            elif op in ['$pull', '$pullAll', '$push', '$pushAll', '$addToSet']:
                errors = []
                for field in value.keys():
                    field_split = field.split('.')
                    field = field_split[0]
                    datatype = cls.fields.get(field)

                    if not datatype or not isinstance(datatype, DataType):
                        continue

                    if (
                        len(field_split) == 1 and
                        not isinstance(datatype, List)
                    ) or (
                        len(field_split) > 1 and
                        not isinstance(datatype, Dict)
                    ):
                        errors.append("%s should be of type %s. Found %s"
                                      % (field, datatype.datatype, list))
                    else:
                        updated_fields.append(field)

            elif isinstance(value, dict):
                updated_fields.extend(value.keys())

        if errors:
            raise ORMException(errors)

        if not document.get('$set'):
            document["$set"] = {}

        if silent is False:
            document['$set']['modified_on'] = cls.now()

        document["$set"]["updated_on"] = cls.now()

        cls.prepare_update_document(document)
        cls.prepare_update_query(filter_args)
        cls.check_fields(filter_args)

        cls.on_update(filter_args, document, updated_fields)

        return call, filter_args, document, kwargs

    @classmethod
    def find_and_modify(cls, *args, **kwargs):
        _sort = {}
        sort = kwargs.pop('sort', None)
        sortkey = kwargs.pop('sortkey', None)

        if sort is None and sortkey is None:
            pass
        elif isinstance(sort, dict):
            _sort = sort
        elif isinstance(sortkey, dict):
            _sort = sortkey
        elif isinstance(sortkey, basestring):
            _sort[sortkey] = sort or -1
        else:
            raise ORMException(
                '''
                Sort/SortKey must be provided as a {field: direction},
                Alternatively use sortkey=field & sort=direction
                ''')

        call, _f, _d, _k = cls.__update(*args, **kwargs)
        return call.find_and_modify(query=_f, update=_d, sort=_sort, **_k)

    @classmethod
    def update(cls, *args, **kwargs):
        if kwargs.get("upsert"):
            raise ORMException(
                '''
                An upsert with empty second argument, erases entire document.
                To avoid any accidental update, drop upsert support.

                Proof of the Problem:

                Mongo-IN [95]>

                con.test.test.insert({"x": 1}),
                con.test.test.find_one()

                Mongo-Out [95]:

                (ObjectId('52134596785c1e073a04692b'),
                {u'_id': ObjectId('52134596785c1e073a04692b'), u'x': 1})

                Mongo-IN [96]>

                con.test.test.update({"x":1}, {}, upsert=True),
                con.test.test.find_one()

                Mongo-Out [96]:

                ({u'connectionId': 1,
                u'err': None,
                u'n': 1,
                u'ok': 1.0,
                u'updatedExisting': True},
                {u'_id': ObjectId('52134596785c1e073a04692b')})
                ''')

        call, _f, _d, _k = cls.__update(*args, **kwargs)
        _k['safe'] = [True, kwargs.get('safe')]['safe' in kwargs]
        _k['multi'] = [True, kwargs.get('multi')]['multi' in kwargs]

        return call.update(_f, document=_d, **_k)

    @classmethod
    def _get(cls, filter_args=None, limit=None, skip=0, sort=-1,
             sortkey='_id', max_scan=None, fields=None, **kwargs):

        if isinstance(filter_args, basestring) and \
           len(filter_args) == 24 and \
           filter_args.isdigit():
            filter_args = {"_id": filter_args}

        if isinstance(fields, list):
            if "_id" not in fields:
                fields.append('_id')
        elif not isinstance(fields, dict):
            fields = None

        if isinstance(filter_args, dict):
            filter_args = filter_args.copy()
            filter_args.update(kwargs)
        else:
            filter_args = kwargs

        if sort or sortkey:
            if isinstance(sort, (list, tuple)):
                sort = sort
            elif isinstance(sortkey, (list, tuple)):
                sort = sortkey
            else:
                sort = [(sortkey, sort)]

        cls.check_fields(filter_args)

        cls.prepare_get_query(filter_args)

        coll = cls.mongo_collection(cls.valid_database())
        return coll.find(filter_args, sort=sort, fields=fields,
                         limit=limit or 0,
                         skip=skip or 0, max_scan=max_scan,
                         as_class=DbDictClass,
                         manipulate=False)

    @classmethod
    def count(cls, *args, **kwargs):
        kwargs["fields"] = ["_id"]
        cursor = cls._get(*args, **kwargs)
        return cursor.count()

    @classmethod
    def get_one(cls, *args, **kwargs):
        kwargs['limit'] = 1
        y = cls._get(*args, **kwargs)
        return cls(partial_model=True, **y[0]) if y.count() else None

    @classmethod
    def get_many(cls, *args, **kwargs):
        return cls._get(*args, **kwargs)

    @classmethod
    def check_fields(cls, filter_args):
        for field in filter_args.iterkeys():
            if not field.startswith('$'):
                if "." in field:
                    _field = field.split(".")[0]
                else:
                    _field = field

                if _field not in cls.fields:
                    raise ORMException(
                        '''
                        Invalid query on %s in %s
                        ''' % (_field, cls.__tablename__))

    @classmethod
    def remove(cls, _id, *args, **kwargs):
        database = cls.valid_database()
        call = cls.mongo_collection(database)

        if isinstance(_id, list):
            filter_args = {'_id': {'$in': _id}}
        elif isinstance(_id, dict):
            filter_args = _id
        else:
            filter_args = {'_id': _id}

        cls.check_fields(filter_args)

        on_delete = getattr(cls, "on_delete", None)
        if callable(on_delete):
            documents = [x for x in call.find(filter_args)]
            cls.on_delete(documents)

        delete_doc = {
            'deleted': True,
            "deleted_on": cls.now()
        }

        cls.prepare_delete_document(delete_doc)
        call.update(filter_args, {'$set': delete_doc}, multi=True)

    def delete(cls, *args, **kwargs):
        return cls.remove(cls._id)
