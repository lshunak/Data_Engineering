import re

split = (re.findall('\w+', 'how much wood would a woodchuck chuck if a woodchuck could chuck wood?')
)
print(split)
print(type(split))
print(len(split))

