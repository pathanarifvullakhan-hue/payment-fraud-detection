%dw 2.0
import try from dw::Runtime

/* ------------- individual rules: each returns a hit object or null ------------- */
fun ruleAmount(e, cfg) =
  if (e.transaction.amount > cfg.amountLimit)
    { ruleCode: "R01_AMOUNT", ruleName: "Transaction amount above configured limit", ruleWeight: cfg.wAmount }
  else null

fun ruleGeo(e, cfg) =
  if ((!isEmpty(e.merchant.merchantCountry) and e.merchant.merchantCountry != e.card.issuingCountry)
      or (!isEmpty(e.device.geoCountry) and e.device.geoCountry != e.card.issuingCountry))
    { ruleCode: "R02_GEO_MISMATCH", ruleName: "Transaction country differs from card issuing country", ruleWeight: cfg.wGeo }
  else null

fun ruleVelocity(ctx, cfg) =
  if ((ctx.velocityCount default 0) > cfg.velocityMax)
    { ruleCode: "R03_VELOCITY", ruleName: "Too many transactions on the same card in the velocity window", ruleWeight: cfg.wVelocity }
  else null

fun ruleMerchant(e, cfg) =
  if (cfg.highRiskCategories contains upper(e.merchant.merchantCategory default ""))
    { ruleCode: "R04_MERCHANT_CAT", ruleName: "High risk merchant category", ruleWeight: cfg.wMerchant }
  else null

fun ruleHour(e, ctx, cfg) = do {
  var hr = (e.eventTimestamp as DateTime).hour
  var s = ctx.profile.usualHourStart default cfg.usualHourStart
  var en = ctx.profile.usualHourEnd default cfg.usualHourEnd
  ---
  if (hr < s or hr > en)
    { ruleCode: "R05_ODD_HOUR", ruleName: "Transaction outside customer usual hour range", ruleWeight: cfg.wHour }
  else null
}

fun ruleWatchlist(ctx, cfg) =
  if (upper(ctx.profile.riskProfile default "") == "WATCHLIST")
    { ruleCode: "R06_WATCHLIST", ruleName: "Customer is on the risk watchlist", ruleWeight: cfg.wWatchlist }
  else null

/* ------------- scoring + banding + decision ------------- */
fun evaluate(e, ctx, cfg) = do {
  var hits = [ruleAmount(e, cfg), ruleGeo(e, cfg), ruleVelocity(ctx, cfg),
              ruleMerchant(e, cfg), ruleHour(e, ctx, cfg), ruleWatchlist(ctx, cfg)] filter ($ != null)
  var raw = sum(hits.ruleWeight)
  ---
  { score: if (raw > 100) 100 else raw, hits: hits }
}

fun band(score, cfg) =
  if (score >= cfg.highMin) "HIGH"
  else if (score >= cfg.mediumMin) "MEDIUM"
  else "LOW"

fun decide(score, band, forceReview, cfg) =
  if (forceReview) "REVIEW"
  else if (score >= cfg.blockMin) "BLOCK"
  else if (band == "LOW") "APPROVE"
  else "REVIEW"

/* Consumer-side finalisation. Re-scores with the customer profile; FAILS SAFE to REVIEW if anything throws. */
fun finalize(env, profile, cfg) = do {
  var attempt = try(() -> evaluate(env.event, { velocityCount: env.velocityCount default 0, profile: profile }, cfg))
  var failed = (env.scoring.scoringFailed default false) or (not attempt.success)
  var base = if (attempt.success) attempt.result
             else { score: env.scoring.riskScore default cfg.failSafeScore, hits: env.scoring.ruleHits default [] }
  var score = if (failed) max([base.score, cfg.failSafeScore]) else base.score
  var hits = if (failed) (base.hits ++ [{ ruleCode: "SYS_SCORING_FAILURE", ruleName: "Scoring failed - routed to manual review", ruleWeight: 0 }])
             else base.hits
  var b = band(score, cfg)
  ---
  { riskScore: score, riskBand: b, decision: decide(score, b, failed, cfg), ruleHits: hits }
}
