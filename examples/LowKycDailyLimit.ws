rule LowKycDailyLimit {
  description "Low-tier KYC customer exceeding daily transaction limit."

  when metadata.kyc_tier == 1
   and amount > metadata.daily_limit
   or metadata.kyc_tier == 2

  then review
       score   0.5
       reason  "Transaction amount exceeds daily limit for tier-1 user"
}
