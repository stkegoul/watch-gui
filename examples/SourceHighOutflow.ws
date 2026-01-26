rule SourceHighOutflow {
  description "Source account has high outflow volume in 24h."

  when sum(when source == $current.source, "PT24H") > 5000

  then review
       score   0.5
       reason  "High cumulative outflow from source in 24 hours"
}
