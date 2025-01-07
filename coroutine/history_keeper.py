
def history_keeper(n: int):
    history = []
    while True:
        if (len(history) > n):
            history.pop(0)
        new_value = yield history
        if new_value is not None:
            history.append(new_value)

    return inner()

if __name__ == "__main__":
    hk = history_keeper(3)

    print(next(hk))  # Initial state: []

    hk.send(1)  # Add 1 to history
    print(next(hk))  # History: [1]

    hk.send(2)  # Add 2 to history
    print(next(hk))  # History: [1, 2]

    hk.send(3)  # Add 3 to history
    print(next(hk))  # History: [1, 2, 3]

    hk.send(4)  # Add 4 to history, removing 1
    print(next(hk))  # History: [2, 3, 4]
