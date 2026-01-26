rule WeekendTransactionCheck {
  description "Large transactions on weekends receive additional scrutiny."

  when day_of_week(timestamp) in (6, 7)
   and amount > 5000

  then review
       score   0.4
       reason  "Large weekend transaction flagged for review"
}
