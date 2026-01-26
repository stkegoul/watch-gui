rule ForeignCurrencyTx {
  description "Transaction occurs in currency different from account's base currency."

  when currency != metadata.account_base_currency
   and amount > 1000

  then review
       score   0.4
       reason  "Large transaction in foreign currency"
}
