rule RepeatedIdenticalAmount {
  description "Multiple transactions with identical amounts may indicate structuring."

  when count(when amount == $current.amount, "PT1H") > 2

  then review
       score   0.6
       reason  "Multiple identical amount transactions in a short time period"
}
