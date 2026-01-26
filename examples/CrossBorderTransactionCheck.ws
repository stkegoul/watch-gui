rule CrossBorderTransactionCheck {
  description "High-value transaction crossing international borders."

  when metadata.source_country != metadata.destination_country
   and amount > 1000

  then review
       score   0.5
       reason  "Large cross-border transaction requires review"
}
