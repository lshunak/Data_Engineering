def is_name_in_globals(name):
    return name in globals()

x = 10
print(is_name_in_globals('x'))
print(is_name_in_globals('y'))