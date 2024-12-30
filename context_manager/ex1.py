import string

def generate_alphabets_files():
    for letter in string.ascii_uppercase:
        with open(f'{letter}.txt', 'w') as file:
            file.write(f"This file is for letter {letter}")

generate_alphabets_files()