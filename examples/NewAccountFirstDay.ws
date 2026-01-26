rule NewAccountFirstDay {
  description "High-volume activity on first day of account creation."

  when metadata.account_age_days < 1
   and amount > 1000

  then review
       score   0.6
       reason  "Large transaction from newly created account"
}
