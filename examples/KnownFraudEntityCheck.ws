rule KnownFraudEntityCheck {
  description "Automatically blocks transactions with entities on fraud list."

  when destination in ("1234567890", "12345678911")

  then block
       score   1.0
       reason  "Destination is on known fraud entities list"
}
