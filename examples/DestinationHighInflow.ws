rule DestinationHighInflow {
  description "High volume of funds flowing into a single destination in 24h."

  when sum(amount when destination == $current.destination, "PT24H") > 100

  then review
       score   0.5
       reason  "High inflow to same destination in 24 hours."
}
