rule UnusualTransactionTime {
  description "Large transactions during unusual hours receive extra scrutiny."

  when hour_of_day(timestamp) >= 1
   and hour_of_day(timestamp) < 5
   and amount > 1000

  then review
       score   0.6
       reason  "Large transaction during unusual hours (1 AM - 5 AM)"
}
