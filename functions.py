#ex1:
def remove_specific_word(words_lst, word):
    filtered = list(filter(lambda x: x != word, words_lst))
    print(filtered)

remove_specific_word(['hello', 'world', 'python', 'java', 'hello', 'python'], \
                     'hello')

#Ex 2:

def sort_list(lst, lambda_func):
    return sorted(lst, key=lambda_func)

print(sort_list([10, 15, 2, 6, 21], lambda x: x ))

def sum_list(lst, lambda_func):
    return sum(filter(lambda_func, lst))

list = [10, -3, 7, -8, 5, -2, -1]

print(sum_list(list, lambda x: x > 0))
print(sum_list(list, lambda x: x < 0))

def squares_of_evens(lst):
    return [x ** 2 for x in lst if x % 2 == 0]

print (squares_of_evens([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

def apply_to_dict(dict, lambda_func):
    return {k: lambda_func(v) for k, v in dict.items()}

products = {'apple': 10, 'banana': 5, 'orange': 8, 'kiwi': 7}
print(products)
print(apply_to_dict(products, lambda x: x * 0.9))
