""" This module provides a set of custom Exceptions. """
class InvalidArgumentToFunction(Exception):
    def __init__(self) -> None:
        message = 'Please pass the correct parameters to the function!'
        Exception.__init__(self, message)

class ErrorDuringConnection(Exception):
    pass

class ScheduledQueryIdWrongFormat(Exception):
    def __init__(self) -> None:
        message = """
            The given ID isn't in the correct format. Please use the format 'projects/[]/locations/[]/transferConfigs/[]'
        """
        Exception.__init__(self, message)
