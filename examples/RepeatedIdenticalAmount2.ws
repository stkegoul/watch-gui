rule RepeatedIdenticalAmount2 {
  description "Multiple transactions with identical amounts may indicate structuring."

  when count(when source == $current.source, "P7D") >= 3
  and count(when amount == $current.amount, "P7D") >= 3

  then review
       score   0.55
       reason  "Multiple identical amount transactions detected from same source"
}
