rule ReviewChristmasTransactions {
  description "Review transactions made during Christmas"

  when month_of_year(timestamp) == 12
    and day_of_month(timestamp) == 25

  then review
       score   0.6
       reason  "Christmas transaction"
}