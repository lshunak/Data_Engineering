class MyRange():
    def __init__(self, n):
        if not isinstance(n, int):
            raise TypeError("n must be an integer.")
        self.n = n

    def __len__(self):
        return self.n

    def __getitem__(self, index):
        if index < self.n:
            return index
        else:
            raise IndexError

if __name__ == "__main__":
    range_4 = MyRange(4)
    for i in range_4:
        print(i)
    print(len(range_4))

    #range_5 = MyRange('5')
    r = MyRange(100)
    print(r[14])
    