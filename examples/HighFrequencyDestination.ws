rule HighFrequencyDestination {
  description "Unusually frequent payments to the same destination may require scrutiny."

  when count(when destination == $current.destination, "PT24H") > 10
   and amount > 100

  then review
       score   0.5
       reason  "High frequency of transactions to same destination in 24 hours"
}
