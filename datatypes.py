import re
import datetime
from bson.objectid import ObjectId


class DataTypeMismatch(Exception):

    def __init__(self, message, *args, **kwargs):
        self.error_message = message
        super(DataTypeMismatch, self).__init__(message, *args, **kwargs)


def check_defaults(func):
    def inner(self, value):
        if value is None:
            if self.default is not None:
                return self.default

            elif self.nullable is True:
                return None

            else:
                raise DataTypeMismatch(
                    "You have left a required field in this form empty.")

        return func(self, value)
    return inner


class DataType(object):
    datatype = None
    default = None
    nullable = True
    searchable = False
    forbidden = False
    choices = None

    def __init__(cls, **kwargs):
        for i in kwargs:
            setattr(cls, i, kwargs[i])

    def humanize(cls, value):
        return value

    @check_defaults
    def dbfy(self, value):
        if self.datatype == type(value):
            return value
        return self.datatype(value)


class Unichar(DataType):
    datatype = unicode

    @check_defaults
    def dbfy(cls, value):
        if not isinstance(value, basestring):
            raise DataTypeMismatch(
                "A field in this form requires a text "
                "string but you entered this instead: %s" % value)

        return super(Unichar, cls).dbfy(value)


class Regex(Unichar):

    def __init__(cls, regex, **kwargs):
        if not regex:
            raise DataTypeMismatch(
                "regex is required param for Regex DataType")

        if isinstance(regex, str):
            regex = re.compile(regex)

        if not isinstance(regex, re._pattern_type):
            raise DataTypeMismatch("Invalid regex in Regex DataType")

        cls.regex = regex
        Unichar.__init__(cls, **kwargs)

    @check_defaults
    def dbfy(cls, value):
        if cls.nullable and value in ['', None]:
            return value
        if cls.regex.search(value):
            return value
        raise DataTypeMismatch("Invalid value %s" % value)

id_re = re.compile('^\d{24}$')
url_re = re.compile(
    r'^file:///|https?://'  # http:// or https://
    # domain...
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|/\S+)$', re.IGNORECASE)

email_re = re.compile(r'\w+(\.\w+)*@[-+\w]+(\.\w+)+')
epoch_re = re.compile(r'^\d{13}$')


class ID(Regex):

    def __init__(cls, **kwargs):
        Regex.__init__(cls, id_re, **kwargs)

    @check_defaults
    def dbfy(self, value):
        if isinstance(value, ObjectId):
            return value
        return super(ID, self).dbfy(value)


class Email(Regex):

    def __init__(cls, **kwargs):
        Regex.__init__(cls, email_re, **kwargs)

    @check_defaults
    def dbfy(cls, value):
        return Regex.dbfy(cls, value.strip())


class URL(Regex):

    def __init__(cls, **kwargs):
        Regex.__init__(cls, url_re, **kwargs)

    @check_defaults
    def dbfy(cls, value):
        if value and not value.startswith(('ftp:', 'http:', 'https', 'file:')):
            value = "http://%s" % value

        try:
            val = Regex.dbfy(cls, value)
        except DataTypeMismatch, e:
            raise DataTypeMismatch(
                "%s does not match a valid URL scheme" % value)
        else:
            return val


class Boolean(DataType):
    datatype = bool
    nullable = False
    default = False


class Integer(DataType):
    datatype = int
    nullable = False
    default = 0

    @check_defaults
    def dbfy(cls, value):
        if cls.datatype != type(value):
            value = cls.datatype(value)

        if value and ((value > 2 ** 64 / 2 - 1) or (value < -2 ** 64 / 2)):
            raise DataTypeMismatch('Only 8-byte integer are supported')
        return value


class Dateday(Integer):
    default = 1
    valid = lambda self, x: x <= 31 and x >= 1

    @check_defaults
    def dbfy(cls, value):
        value = Integer.dbfy(cls, value)
        if not cls.valid(value):
            raise DataTypeMismatch('%s is not a valid Date number [1-31]')
        return value


class Datemonth(Integer):
    default = 1
    valid = lambda self, x: x <= 12 and x >= 1

    @check_defaults
    def dbfy(cls, value):
        value = Integer.dbfy(cls, value)
        if not cls.valid(value):
            raise DataTypeMismatch('%s is not a valid Month number. [1-12]')
        return value


class Dateyear(Integer):
    default = 2010


class Timehour(Integer):
    default = 0
    valid = lambda self, x: x < 24 and x >= 0

    @check_defaults
    def dbfy(cls, value):
        value = Integer.dbfy(cls, value)
        if not cls.valid(value):
            raise DataTypeMismatch(
                '%s is not a valid Hour format. [0-23 in 24 hours format]')
        return value


class Timeminutes(Integer):
    default = 0
    valid = lambda self, x: x < 60 and x >= 0

    @check_defaults
    def dbfy(cls, value):
        value = Integer.dbfy(cls, value)
        if not cls.valid(value):
            raise DataTypeMismatch(
                '%s is not a valid Minute number format. [0-59]')
        return value


class Decimal(DataType):
    datatype = float
    nullable = False
    default = 0

    def humanize(cls, value):
        return '%.2f' % float(value)

    @check_defaults
    def dbfy(cls, value):
        return float(value)


class Currency(Decimal):
    datatype = float
    nullable = False
    default = 0

    @check_defaults
    def dbfy(cls, value):
        value = float(value)
        if value < 0:
            raise Excpetion("Currency cannot be a negative value")
        return value


class Html(Unichar):
    pass


class Dict(DataType):
    datatype = dict
    default = {}


class List(DataType):
    datatype = list
    default = []

    @check_defaults
    def dbfy(cls, value):
        if value is None:
            return value
        if not isinstance(value, (list, tuple, set)):
            raise DataTypeMismatch("Expected a List but found %s" % value)
        return list(value)


class Datetime(DataType):
    datatype = datetime.datetime

    @check_defaults
    def dbfy(cls, value):
        if not isinstance(value, cls.datatype):
            raise DataTypeMismatch(
                "Expected datetime. Found %s instead" % value)
        return value


class Timestamp(Regex):

    def __init__(cls, **kwargs):
        Regex.__init__(cls, epoch_re, **kwargs)

    @check_defaults
    def dbfy(cls, value):
        return int(Regex.dbfy(cls, str(value)))
