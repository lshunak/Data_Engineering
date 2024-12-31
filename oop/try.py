class Person:
    pass

a = Person()
b = Person()
print(a==b)

class X:
    """Example class"""
    count = 0
    avg = 0
    def __init__(self, a, b):
        """init function for class X"""
        self.a = a
        self.b = b

x = X(1, 2)
print(x.a, x.b)