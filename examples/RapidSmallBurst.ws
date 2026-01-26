rule RapidSmallBurst {
  description "Multiple small transactions in quick succession."

  when amount < 500
   and count(when source == $current.source, "PT30M") >= 5

  then review
       score   0.65
       reason  "Rapid succession of small transactions"
}
