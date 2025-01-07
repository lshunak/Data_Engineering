import re
from collections import namedtuple
from typing import Generator
from itertools import chain
Token = namedtuple('Token', ['category', 'value'])

def Lexer(s: str) -> Generator[Token, None, None]:
    token_specification = [
        ('NUMBER',  r'-?\d+(\.\d*)?([eE][+-]?\d+)?'),  # Handle scientific notation
        ('STRING', r'"([^"\\]*(\\.[^"\\]*)*)"' ),
        ("LBRACE", r'\{'),  # Match left curly brace
        ("RBRACE", r'\}'),  # Match right curly brace
        ("LBRACKET", r'\['),  # Match left square bracket
        ("RBRACKET", r'\]'),  # Match right square bracket
        ("COMMA", r','),    # Match comma
        ("COLON", r':'),    # Match colon
        ("BOOL", r'(true|false)'),  # Match boolean
        ("NULL", r'null'),  # Match null
        ("SKIP", r'[ \t\n\r]+'),  # Skip spaces, tabs, newlines
    ]
        
    # Combine the token patterns into a single regex pattern
    master_pattern = '|'.join(f'(?P<{pair[0]}>{pair[1]})' for pair in token_specification)
    
    # Compile the regex
    prog = re.compile(master_pattern)
    
    # Iterate through the input string and generate tokens
    position = 0
    while position < len(s):
        match = prog.match(s, position)
        if match:
            # Get the matched category and value
            category = match.lastgroup
            value = match.group(category)
            
            if category == "STRING":
                value = value[1:-1]

            if category == "NUMBER":
                if isinstance(value, str) and ('.' in value or 'e' in value.lower()):
                    value = float(value)
                else:
                    value = int(value)

            if category != "SKIP":
                yield Token(category, value)
                
            position = match.end()
        else:
            raise SyntaxError(f"Unexpected character: {s[position]}")

def parse_value(tokens: Generator[Token, None, None])->any:
    token = next(tokens, None)
    
    if token.category == "STRING":
        return token.value
    elif token.category == "NUMBER":
        return token.value
    elif token.category == "BOOL":
        return token.value == "true"
    elif token.category == "NULL":
        return None
    elif token.category == "LBRACE":
        return parse_object(tokens)
    elif token.category == "LBRACKET":
        return parse_array(tokens)
    else:
        raise SyntaxError(f"Unexpected token {token.category}.")


def parse_array(tokens: Generator[Token, None, None])->list:
    result_array = []
    for token in tokens:
        if token.category == "RBRACKET":
            return result_array
        elif token.category == "COMMA":
            continue  # Skip commas
        else:
            result_array.append(parse_value(chain([token], tokens)))  # Chain current token and remaining tokens

    raise SyntaxError("Unexpected end of array")

def parse_object(tokens: Generator[Token, None, None])->dict:
    result_dict = {}
    while True:
        token = next(tokens)
        if token.category == "RBRACE":
            return result_dict
        elif token.category == "STRING":
            key = token.value
            colon_token = next(tokens)
            if colon_token.category != "COLON":
                raise SyntaxError("Expected colon after key.")
            value = parse_value(tokens)
            result_dict[key] = value
        elif token.category == "COMMA":
            continue
        else:
            raise SyntaxError(f"Unexpected token {token.category}.")
        

def parse_json(s: str)->dict:
    token_gen = Lexer(s)
    # Let parse_value handle top-level dispatch
    root = parse_value(token_gen)
    
    # Check that we've consumed all tokens
    leftover = next(token_gen, None)
    if leftover is not None:
        raise SyntaxError("Extra data after valid JSON.")
    return root


if __name__ == "__main__":
    input_str = '{"hello" : 3.2 , "world": true}'
    input_str2 = '{"hello" : 3.2 , "world": true, "nested": {"key": "value", "key2": 3.4}}'
    input_str3 = '{"array": [1, 2, 3, 4]}'
    input_str4 = '{"array": [1, 2, 3, 4], "nested": {"key": "value", "key2": 3.4}}'
    input_str5 = '{"array": [1, 2, 3, 4], "nested": {"key": "'

    parsed_data = parse_json(input_str)
    print(parsed_data)  
    print(parse_json(input_str2))
    print(parse_json(input_str3))
    print(parse_json(input_str4))
    try:
        print(parse_json(input_str5))  # Raises SyntaxError
    except SyntaxError as e:
        print(e)