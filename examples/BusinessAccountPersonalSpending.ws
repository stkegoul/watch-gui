rule BusinessAccountPersonalSpendingd {
  description "Business accounts should generally not be used for personal purchases."

  when meta_data.account_type == "business"
  and meta_data.merchant_category in ("retail", "entertainment", "restaurant", "personal_services")

  then review
       score   0.4
       reason  "Potential personal spending from business account"
}
