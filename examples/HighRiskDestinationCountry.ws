rule HighRiskDestinationCountry {
  description "Destination country on high-risk list."

  when metadata.destination_country in $high_risk_countries

  then block
       score   0.5
       reason  "Destination country classified as high risk"
}
