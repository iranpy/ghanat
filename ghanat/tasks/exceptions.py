class ghanatBaseException(Exception):
    """Base class for ghanat errors and exceptions"""

    def __init__(self, message: str = "") -> None:
        self.message = message

    def __str__(self) -> str:
        return self.message


class InvalidValueException(ghanatBaseException):
    pass


class ParsingException(InvalidValueException):
    pass
