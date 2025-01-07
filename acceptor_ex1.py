import re
def acceptor(string: str) -> bool:
    return re.fullmatch('(^0[01]*0$)', string) is not None and len(string) > 1

print(acceptor("00"))       # True
print(acceptor("01110"))    # True
print(acceptor("0101010"))  # True
print(acceptor("0"))        # False
print(acceptor("01"))       # False
print(acceptor("10001"))    # False