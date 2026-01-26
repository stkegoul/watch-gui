rule MerchantIssuerMismatch {
  description "Card issuer country does not match merchant country."

  when metadata.merchant_country != metadata.card_issuer_country

  then review
       score   0.4
       reason  "Card issuer country does not match merchant country"
}
