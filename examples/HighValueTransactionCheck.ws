rule HighValueTransactionCheck {
  description "Monitors for unusually large transactions requiring review."

  when amount > 10000

  then review
       score   0.5
       reason  "Transaction amount exceeds 10,000 in any currency."
}
