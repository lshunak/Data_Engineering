def remove_non_string(lst):
    return [item for item in lst if isinstance(item, str)]

print(remove_non_string([1, 'hello', 2, 'world', 3, 'python'])) 

def count_letter_occurence(word: str):
    return {letter: word.count(letter) for letter in word}

print(count_letter_occurence('hello'))

def common_elements(lst1, lst2):
    return list(set(lst1) & set(lst2))

print(common_elements([1, 2, 3, 4, 5], [4, 5, 6, 7, 8]))

def unique_vals(dict):
    return list(set(dict.values()))

print(unique_vals({'a': 1, 'b': 2, 'c': 1, 'd': 3}))

def left_rotation(lst, n):
    n = n % len(lst)
    return lst[n:] + lst[:n]

a = [1, 2, 3, 4, 5]
print(a)
print(left_rotation(a, 2))


def dict_to_tuple(dict):
    return list(dict.items())

print(dict_to_tuple({'a': 1, 'b': 2, 'c': 3}))

def maximum_minimum_of_dict(dict):
    max_key = max(dict, key=dict.get)
    min_key = min(dict, key=dict.get)
    print(f'Max key: {max_key}, min key: {min_key}')

maximum_minimum_of_dict({'a': 1, 'b': 2, 'c': 3})

def remove_and_print(lst):
    while len(lst) > 1:
        print(lst[1::2])
        lst = lst[::2]

    if lst:
        print(lst) 
            

remove_and_print([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])