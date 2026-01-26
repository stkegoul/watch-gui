rule SuspiciousDescriptionCheck {
  description "Detects suspicious keywords or patterns in transaction descriptions."

  when description regex "(?i)(btc|bitcoin|crypto|wallet|transfer|gift.?card|western.?union)"
  and amount > 1000

  then review
       score   0.7
       reason  "Suspicious description pattern."
}