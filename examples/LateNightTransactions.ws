rule LateNightTransactions {
  description "Detects transactions made late at night."

  when hour_of_day(timestamp) >= 21
  or hour_of_day(timestamp) <= 03
  and amount > 1000

  then review
       score   0.6
       reason  "Large transaction during late night hours"
}