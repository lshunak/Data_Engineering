def read_first_n_lines(filename, n):
    with open(filename, "r") as file:
        for _ in range(n):
            print(file.readline().strip())

read_first_n_lines('../functions.py', 5)
