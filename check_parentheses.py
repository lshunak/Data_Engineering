import re
# Remove pairs of '()' until no more are found
def check_balanced_parentheses(s: str) -> bool:
    
    s = re.sub(r"[^\(\)]", "", s)
    
    pattern = r"\(\)"

    while re.search(pattern, s):  
        s = re.sub(pattern, "", s)
    
    return not s

if __name__ == "__main__":
    print(check_balanced_parentheses("(())"))        # True
    print(check_balanced_parentheses("((()))"))      # True
    print(check_balanced_parentheses("(()"))         # False
    print(check_balanced_parentheses(")("))          # False
    print(check_balanced_parentheses("()()"))        # True
    print(check_balanced_parentheses("(a + b)"))     # True
    print(check_balanced_parentheses("(a + b * (c - d))"))  # True
    print(check_balanced_parentheses("(a + b) * (c - d"))   # False
