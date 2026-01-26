rule HighAmountVelocitySource {
  description "Monitors for rapid high-value spending from a single source in the last hour."

  when sum(amount when source == $current.source, "PT1H") > 3000

  then review
       score   0.7
       reason  "Source account shows high-velocity spending pattern"
}
