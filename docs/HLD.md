# High Level Design - Payment Fraud Detection (MuleSoft, Mule 4)

## 1. Purpose
Accept payment events over HTTP, score them for fraud risk with configurable rules, queue them by priority, persist the final decision, alert the risk team (without flooding), and never lose an acknowledged event.

## 2. Architecture
```
Client --POST /api/payments/events--> HTTP Listener -> APIkit router (RAML + JSON schema validation)
   -> dedupe (Object Store) -> velocity count (Object Store) -> score + band (DataWeave RiskRules)
   -> HIGH: payments-high-q (8 consumers)      LOW/MEDIUM: payments-std-q (4 consumers)    [VM, PERSISTENT]
   -> 202 returned to caller
Consumer: enrich from customer_risk_profile (MySQL) -> finalise decision -> save fraud_events + fraud_rule_hits (one transaction)
   -> HIGH/BLOCK -> alertQueue (1 consumer) -> throttle per card -> SMTP email
Failures: DB/SMTP down -> re-queue with retry counter | unprocessable -> payments-dlq -> dead_letter_events table
```

## 3. Components
| Component | File | Responsibility |
|---|---|---|
| API contract | `api/payment-events-api.raml`, `api/schemas/payment-event.json` | Required fields, positive amount, currency list, masked-card pattern (full card numbers rejected at the door) |
| Global config / error handler | `global-config.xml` | Connectors, VM queues, Object Stores, consistent error JSON with correlation ID |
| Intake flow | `payment-events-api.xml` | Validate, dedupe, velocity, score, route, publish, respond 202 |
| Consumer | `scoring-consumer.xml` | Enrich, finalise decision, persist, hand off alerts, retry/DLQ logic |
| Persistence | `persistence.xml` | Idempotent upsert of event + rule hits in one DB transaction |
| Alerting | `alerting.xml` | Single-threaded throttle, aggregate email, reconciler for PENDING alerts, HIGH-spike monitor |
| Dead letters | `dead-letter.xml` | Store reason + masked payload, notify ops when DLQ depth exceeds threshold |
| Business rules | `modules/RiskRules.dwl` | Rules R01-R06, banding, APPROVE/REVIEW/BLOCK, fail-safe to REVIEW |
| Safety | `modules/Masking.dwl`, `log4j2.xml` | Mask card numbers; redact any 13+ digit run from every log line |

## 4. Scoring
R01 amount over limit (35), R02 geography mismatch (30), R03 velocity (20), R04 high-risk merchant category (17), R05 unusual hour (15), R06 watchlist customer (10). Score capped at 100. Bands: LOW < 30, MEDIUM 30-59, HIGH >= 60. Decision: BLOCK >= 85, REVIEW for MEDIUM/HIGH, APPROVE for LOW. Every number lives in `config-dev.yaml`. The sample payment scores 82 (35 + 30 + 17) = HIGH.

## 5. Data model
`fraud_events` (one row per event), `fraud_rule_hits` (one row per fired rule), `customer_risk_profile` (reference data), `dead_letter_events` (reason for every dead-lettered event).

## 6. Reliability
| Failure | Behaviour |
|---|---|
| Invalid payload | 400 with field paths only; nothing queued |
| Duplicate eventId | 409; one row, one email |
| Queue unavailable | Publish retried, then 503 "not accepted"; dedupe key released so caller can retry |
| DB down | Message re-queued with counter (up to 720 x 5 s); processed when DB returns |
| Unprocessable message | Moved to DLQ with reason; row in `dead_letter_events` |
| SMTP down | `alert_status = PENDING`; reconciler re-sends |
| Scoring crash | Decision REVIEW with rule `SYS_SCORING_FAILURE`; never APPROVE |

## 7. Security
No full card number in logs, DB, queues, API responses or emails. Secrets are encrypted (Secure Properties, Blowfish/CBC); the key is passed at runtime and never committed. App uses a least-privilege DB user (`fraud_app`). Error responses contain no stack traces.

## 8. Design decision and trade-off: VM queues instead of Anypoint MQ
Anypoint MQ was not available, so Mule's built-in VM connector with persistent queues is used. Priority is modelled with two queues and different consumer counts. **Trade-off:** VM queues live inside the Mule runtime rather than in an external broker. Persistent messages survive a normal restart, and re-queue logic covers DB outages, but a production system should use a real broker (Anypoint MQ, JMS/ActiveMQ) for stronger durability and independent scaling.

## 9. Test coverage
MUnit suites cover each rule in isolation, band boundaries, decisions, masking, alert throttling, DB-down re-queue and DLQ routing, with DB, SMTP and queues mocked. 100% coverage is not reachable because these are infrastructure wiring and not business logic: scheduler flows (`*-scheduler-flow`, monitors), autodiscovery, HTTP listener and APIkit router wiring.
