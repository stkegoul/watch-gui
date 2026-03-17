rule highValueUSD {
  when amount >= 5000
    and currency == "USD"

  then review
       score 0.45
       reason "User is sending above 5,000 USD"
}
