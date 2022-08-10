from uuid import uuid4


def suffix() -> str:
    return " - " + str(uuid4())[:5]
