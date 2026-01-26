rule DormantAccountActivity {
  description "Unusual activity after long period of account dormancy."

  when metadata.days_since_last_transaction > 90
   and amount > 1000

  then review
       score   0.7
       reason  "High-value transaction after extended account inactivity"
}
