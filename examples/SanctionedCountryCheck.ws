rule SanctionedCountryCheck {
  description "Blocks transactions to sanctioned countries."

  when metadata.destination_country in $sanctioned_countries

  then block
       score   1.0
       reason  "Destination country is on global sanctions list"
}
