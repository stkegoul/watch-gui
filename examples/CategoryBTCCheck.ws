rule CategoryBTCCheck {
    description "Cryptocurrency transaction requires manual verification."

    when metadata.category == "cryptocurrency"

    then review
         score   0.6
         reason  "Cryptocurrency transaction requires verification"
}
