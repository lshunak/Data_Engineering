from InfiniteRange import InfiniteRange

infinite_range = InfiniteRange()
even_numbers = (x for x in infinite_range if x % 2 == 0)

if __name__ == "__main__":
    import time

    for i in even_numbers:
        print(i)
        time.sleep(0.5)  # just so you can see what's happening