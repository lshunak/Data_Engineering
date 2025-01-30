
class InfiniteRange():

    def __iter__(self):
        self.current = 0
        return self
    
    def __next__(self):
        result = self.current
        self.current += 1
        return result


if __name__ == "__main__":
    import time  
    a = InfiniteRange()
    for i in a:
        print(i)
        time.sleep(0.5)  # just so you can see what's happening
