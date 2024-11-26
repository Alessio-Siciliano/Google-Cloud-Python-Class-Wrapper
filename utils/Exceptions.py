class InvalidArgumentToFunction(Exception):
    def __init__(self) -> None:
        message = 'Please pass the correct parameters to the function!'
        Exception.__init__(self, message)

class ErrorDuringConnection(Exception):
    pass