
rule TX223333 {

    description "Jerry blackk lists 33"

    when transaction_id == "abcdefg"

    then block
         score   1.0
         reason  "transaction id is on the black list"
}

