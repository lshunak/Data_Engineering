import unittest
from bank_account import BankAccount

class TestBankAccount(unittest.TestCase):
    def setUp(self):
        # Create a BankAccount object before each test
        self.account = BankAccount(id=1)

    def test_initial_balance(self):
        # Test that the initial balance is 0
        self.assertEqual(self.account.balance, 0, "Initial balance should be 0.")

    def test_deposit(self):
        # Test depositing an amount
        self.assertTrue(self.account.deposit(100), "Deposit should return True.")
        self.assertEqual(self.account.balance, 100, "Balance should be updated after deposit.")

    def test_withdraw_successful(self):
        # Test withdrawing an amount within the balance
        self.account.deposit(200)  # Add initial balance
        self.assertTrue(self.account.withdraw(50), "Withdraw should return True when funds are sufficient.")
        self.assertEqual(self.account.balance, 150, "Balance should decrease after a successful withdrawal.")

    def test_withdraw_insufficient_funds(self):
        # Test withdrawing an amount exceeding the balance
        self.account.deposit(50)  # Add some balance
        self.assertFalse(self.account.withdraw(100), "Withdraw should return False when funds are insufficient.")
        self.assertEqual(self.account.balance, 50, "Balance should remain unchanged when withdrawal fails.")

    def test_multiple_transactions(self):
        # Test a series of deposits and withdrawals
        self.account.deposit(300)
        self.account.withdraw(100)
        self.account.deposit(50)
        self.assertEqual(self.account.balance, 250, "Balance should reflect all transactions.")

if __name__ == "__main__":
    unittest.main()  
