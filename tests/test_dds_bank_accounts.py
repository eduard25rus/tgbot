from __future__ import annotations

import unittest

from webapp import bank_account_balance_lines_html


class DdsBankAccountSummaryTests(unittest.TestCase):
    def test_treasury_account_is_rendered_after_two_settlement_accounts(self) -> None:
        settlement_accounts = [
            {"account_number": "40702810650000037577", "closing_balance": 0.0},
            {"account_number": "40702810650710001806", "closing_balance": 107_371.21},
        ]
        treasury_accounts = [
            {"account_number": "treasury_khor", "closing_balance": 852_248.95},
        ]

        html = bank_account_balance_lines_html(settlement_accounts, treasury_accounts)

        self.assertIn("Фелис Сбербанк", html)
        self.assertIn("Резерв Сбербанк", html)
        self.assertIn("Казначейский счет Хор", html)
        self.assertIn("852 248,95 ₽", html)
        self.assertLess(html.index("Фелис Сбербанк"), html.index("Резерв Сбербанк"))
        self.assertLess(html.index("Резерв Сбербанк"), html.index("Казначейский счет Хор"))

    def test_treasury_account_stays_third_when_settlement_account_is_missing(self) -> None:
        treasury_accounts = [
            {"account_number": "treasury_khor", "closing_balance": 10_000.0},
        ]

        html = bank_account_balance_lines_html([], treasury_accounts)

        self.assertLess(html.index("Фелис Сбербанк"), html.index("Резерв Сбербанк"))
        self.assertLess(html.index("Резерв Сбербанк"), html.index("Казначейский счет Хор"))


if __name__ == "__main__":
    unittest.main()
