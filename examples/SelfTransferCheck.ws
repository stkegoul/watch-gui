 rule SelfTransferCheck {
  description "Large self-transfers may indicate structuring activity."

  when source == destination
   and amount > 3000

  then review
       score   0.45
       reason  "Large self-transfer detected"
}
