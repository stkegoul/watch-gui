rule BlockWhenPreviousTransactionFailed {
    description "Block whe previous transaction failed for same source"

    when previous_transaction(
        within: "PT1H",
        match: {
            status: "failed",
            source: "$current.source"
        }
    )
    and amount > 700000

    then block
         score   1.0    
}