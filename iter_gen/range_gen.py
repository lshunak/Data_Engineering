def MyRange(n):
    if not isinstance(n, int):
        raise TypeError("n must be an integer.")
    current = 0
    while current < n:
        yield current
        current += 1
    


if __name__ == "__main__":
    range_4 = MyRange(4)
    for i in range_4:
        print(i)
    #print(len(range_4))

    range_5 = MyRange('5')