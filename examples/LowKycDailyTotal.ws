rule LowKycDailyTotal {
  description "Low-tier KYC total spend in 24 h above threshold."

  when metadata.kyc_tier == 1
   and sum(amount when source == $current.source, "PT24H") > 5000

  then review
       score   0.5
       reason  "Total transacted amount exceeds tier-1 limit"
}
