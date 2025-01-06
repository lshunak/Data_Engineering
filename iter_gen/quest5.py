def foo():
    for i in range(1,4):
        yield i
        

a = foo()
print(next(a))  # Output: 1
print(next(a))  # Output: 2
print(next(a))  # Output: 3


for i in foo():
    print(i)

gen = (x**2 for x in range(10))  # Generator expression

# Using the generator
for val in gen:
    print(val)