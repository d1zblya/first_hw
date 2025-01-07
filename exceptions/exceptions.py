from fastapi import HTTPException, status


class BaseServiceException(HTTPException):
    status_code = 500
    detail = ""

    def __init__(self):
        super().__init__(status_code=self.status_code, detail=self.detail)


# class ErrorBookNotFound(BaseServiceException):
#     status_code = status.HTTP_404_NOT_FOUND
#     detail = "Book not Found"


class ErrorBookNotFound(Exception):
    pass


class ErrorBookCreation(Exception):
    pass


class ErrorBookUpdate(Exception):
    pass


class ErrorBookDelete(Exception):
    pass


class UserAlreadyExists(Exception):
    pass


class IncorrectEmailOrPassword(Exception):
    pass


class ErrorUserRead(Exception):
    pass


class InCorrectPasswordOrEmail(Exception):
    pass


class ErrorUserAuthenticate(Exception):
    pass
