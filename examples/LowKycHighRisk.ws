rule LowKycHighRisk {
  description "Low-tier KYC customer engaging in high-risk activity."

  when metadata.kyc_tier == 1
   and metadata.merchant_category in ("gambling", "cryptocurrency", "adult", "high_value_goods")
   and amount > 500

  then review
       score   0.8
       reason  "Low-KYC account engaging in high-risk category transaction"
}
