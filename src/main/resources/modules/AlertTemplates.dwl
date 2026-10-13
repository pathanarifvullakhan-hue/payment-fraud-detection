%dw 2.0
import modules::Masking

fun ruleLines(hits) = ((hits default []) map (r) -> "• $(r.ruleCode) – $(r.ruleName) (weight $(r.ruleWeight))") joinBy "\n"

fun fmt(n) = (n as Number) as String { format: "#,##0.00" }

fun subject(a) = "HIGH RISK Payment – $(a.transactionId) – Score $(a.riskScore)"

fun body(a) =
  "A payment transaction has been flagged as high risk and requires review.\n\n" ++
  "Transaction ID: $(a.transactionId)\n" ++
  "Event ID: $(a.eventId)\n" ++
  "Date and Time: $(a.eventTimestamp)\n" ++
  "Amount: $(a.currency) $(fmt(a.amount))\n" ++
  "Card: $(Masking::maskCard(a.maskedCard)) ($(a.cardType default 'N/A'))\n" ++
  "Customer: $(a.customerName default 'N/A') ($(a.customerId))\n" ++
  "Merchant: $(a.merchantName default 'N/A'), $(a.merchantCountry default '--')\n" ++
  "Risk Score: $(a.riskScore) – Band $(a.riskBand)\n" ++
  "System Decision: $(a.decision)\n" ++
  "Correlation ID: $(a.correlationId default 'n/a')\n\n" ++
  "Triggered Rules:\n" ++ ruleLines(a.ruleHits) ++
  "\n\nPlease action this transaction in the fraud review console.\n\nRegards,\nAutomated Fraud Monitoring"

fun aggregateSubject(masked, n) = "HIGH RISK Payments (AGGREGATED) – Card $(Masking::maskCard(masked)) – $(n) transactions"

fun aggregateBody(masked, items) =
  "Multiple high-risk transactions were detected on card $(Masking::maskCard(masked)) within the alert throttling window.\n" ++
  "Individual alerts were suppressed to avoid flooding the risk mailbox. ALL affected transactions are listed below.\n\n" ++
  ((items map (i, idx) -> do {
      var rc = (i.ruleCodes default []) joinBy ", "
      ---
      "$(idx + 1). Txn $(i.transactionId) | Event $(i.eventId) | $(i.eventTimestamp) | $(i.currency) $(fmt(i.amount)) | $(i.merchantName default 'N/A') | Score $(i.riskScore) | Rules: $(rc)"
  }) joinBy "\n") ++
  "\n\nPlease action these transactions in the fraud review console.\n\nRegards,\nAutomated Fraud Monitoring"
