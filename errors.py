class DataTypeMismatch(Exception):

    def __init__(self, message, *args, **kwargs):
        self.error_message = message
        super(DataTypeMismatch, self).__init__(message, *args, **kwargs)

class ORMException(Exception):
    pass
