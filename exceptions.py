class PasswordTooShortError(Exception):
    pass

class MissingUpperCaseError(Exception):
    pass

class MissingLowerCaseError(Exception):
    pass

class MissingDigitError(Exception):
    pass

class MissingSpecialCharError(Exception):
    pass

def check_password_comp(password):
    if len(password) < 8:
        raise PasswordTooShortError("Password is too short")

    if not any(c.isupper() for c in password):
        raise MissingUpperCaseError("Password is missing an uppercase letter")
    
    if not any(c.islower() for c in password):
        raise MissingLowerCaseError("Password is missing a lowercase letter")
    
    if not any(c.isdigit() for c in password):
        raise MissingDigitError("Password is missing a digit")

    special_chars = set('@#%&')

    if not any(c in special_chars for c in password):
        raise MissingSpecialCharError("Password is missing a special character")

    return True


if __name__ == "__main__":
    test_passwords = [
    "abc123",      # Too short, missing uppercase, special character
    "abcdefgh",    # Missing digit, special character, and uppercase
    "Abcd1234",    # Missing special character
    "Abcd@1234",   # Valid password
    "Abcd%ABC123", # Valid password
    ]

    for password in test_passwords:
        try:
            result = check_password_comp(password)
            print(f"Password '{password}' is valid: {result}")
        except Exception as e:
            print(f"Password '{password}' is invalid: {e}")