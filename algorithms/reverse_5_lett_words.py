def reverse_big_words(sentence):
    return " ".join(
        map(
            lambda word: word[::-1] if len(word) >= 5 else word,
            sentence.split()
        )
    )

if __name__ == "__main__":
    example_sentence = "Hello everyone welcome to the programming world"
    modified_sentence = reverse_big_words(example_sentence)
    print(modified_sentence) # Output: "olleH enoyreve emoclew to the gnimmargorp world"