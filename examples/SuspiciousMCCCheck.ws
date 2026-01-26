rule SuspiciousMCCCheck {
  description "Flags transactions with merchants in potentially risky categories."

  when metadata.mcc in ("7995", "5912")

  then review
       score   0.5
       reason  "Transaction involves high-risk merchant category code"
}
