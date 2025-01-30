import pytest
from bank_account import BankAccount  

@pytest.fixture
def bank_account():
    # Provide a BankAccount object for each test
    return BankAccount(id=1)

def test_initial_balance(bank_account):
    # Test that the initial balance is 0
    assert bank_account.balance == 0, "Initial balance should be 0."

def test_deposit(bank_account):
    # Test depositing an amount
    assert bank_account.deposit(100), "Deposit should return True."
    assert bank_account.balance == 100, "Balance should be updated after deposit."

def test_withdraw_successful(bank_account):
    # Test withdrawing an amount within the balance
    bank_account.deposit(200)  # Add initial balance
    assert bank_account.withdraw(50), "Withdraw should return True when funds are sufficient."
    assert bank_account.balance == 150, "Balance should decrease after a successful withdrawal."

def test_withdraw_insufficient_funds(bank_account):
    # Test withdrawing an amount exceeding the balance
    bank_account.deposit(50)  # Add some balance
    assert not bank_account.withdraw(100), "Withdraw should return False when funds are insufficient."
    assert bank_account.balance == 50, "Balance should remain unchanged when withdrawal fails."

def test_multiple_transactions(bank_account):
    # Test a series of deposits and withdrawals
    bank_account.deposit(300)
    bank_account.withdraw(100)
    bank_account.deposit(50)
    assert bank_account.balance == 250, "Balance should reflect all transactions."
