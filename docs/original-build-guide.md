# Payment Fraud Detection in MuleSoft - Build It From Scratch (Anypoint Studio, NO Anypoint MQ)

**Who this is for:** you, with zero MuleSoft knowledge. Every step says **what** you do, **why** you do it, and gives a **Check** so you know it worked before moving on.

**Key change from the other guide:** Anypoint MQ is replaced by Mule's built-in **VM connector** (persistent queues). Everything runs in Anypoint Studio on your laptop with Docker for MySQL and a fake mail server. Cloud deployment is optional at the end.

**Honest note:** I could not run Anypoint Studio while writing this, so a connector attribute or version may need a small fix. The Troubleshooting table at the end covers the likely ones. Do each Check; do not skip ahead.

---

## PART 0 - The big picture (read once, 5 minutes)

### 0.1 What we are building
A payment platform sends a "payment event" (JSON). Our app must:
1. **Accept** it over HTTP and check it is valid.
2. **Score** it for fraud risk with simple rules, and label it LOW / MEDIUM / HIGH.
3. **Queue** it: HIGH events go to a fast "priority" queue.
4. **Return 202** immediately (the caller does not wait for the slow work).
5. In the background, a **consumer** takes events off the queues, looks up the customer in the database, makes a final decision (APPROVE / REVIEW / BLOCK) and **saves** it.
6. For HIGH risk or BLOCK it **emails the risk team** (but not 1000 emails for one card: **throttling**).
7. Nothing may be lost: failures are retried, and hopeless ones go to a **dead-letter queue (DLQ)** with a reason.

### 0.2 Picture
```
 Client --POST--> [ HTTP Listener -> APIkit router ]
                     validate -> duplicate check -> velocity count -> score + band
                           |
              HIGH ------->|-------> LOW/MEDIUM
                 v                       v
        payments-high-q (8 workers)  payments-std-q (4 workers)       <- VM queues
                 \                      /
                  enrich from DB -> final decision -> save (2 tables)
                           |
                 HIGH or BLOCK ? -> alertQueue (1 worker) -> throttle -> EMAIL
   any failure: DB down -> put back on queue and retry later | broken message -> payments-dlq -> dead_letter_events table
```

### 0.3 Vocabulary (you will see these words constantly)
| Word | Plain meaning |
|---|---|
| **Mule app / flow** | A program built from a chain of steps (processors). A *flow* starts with a **source** (e.g. HTTP listener) then steps run in order. |
| **Sub-flow / flow-ref** | A reusable mini-flow; `flow-ref` calls it. Like a function call. |
| **Connector / module** | A plug-in for a technology: HTTP, Database, Email, VM, Object Store... |
| **DataWeave (DW)** | MuleSoft's data language. We use it to transform JSON and to hold scoring rules. |
| **RAML + APIkit** | RAML describes the API (URL, JSON shape). APIkit router automatically checks requests against it. |
| **VM queue** | A queue inside the Mule runtime. `PERSISTENT` = saved to disk, survives restart. Our replacement for Anypoint MQ. |
| **Object Store** | Built-in key-value storage with expiry (TTL). Used for duplicate detection, velocity counters, alert throttling. |
| **Correlation ID** | One id that follows a request through every log line so you can trace it. |
| **MUnit** | MuleSoft's unit-test framework. |
| **DLQ** | Dead-letter queue: where messages go when they cannot be processed. |

### 0.4 Why VM queues instead of Anypoint MQ - and the one trade-off
* VM queues give us everything the brief needs: publish, consume, persistence, several consumers.
* **Priority** = two queues. `payments-high-q` has 8 consumer threads, `payments-std-q` has 4, so HIGH events are always picked up first. (The brief allows "a high priority queue".)
* **Trade-off (write it in your HLD):** VM queues live *inside* the Mule runtime, not in a separate broker. If the app is stopped normally, persistent messages are kept on disk. We also add re-queue logic so DB outages never lose a message. In production a real broker (Anypoint MQ, JMS) is stronger. Say this openly in your demo.

---

## PART A - Install tools (about 1 hour)

| # | Tool | Why you need it | Check |
|---|---|---|---|
| A1 | **Anypoint Studio 7.x** (free, from MuleSoft downloads page; it bundles a Mule 4.4+ runtime) | The IDE where you build and run | Studio opens, you can pick a workspace folder |
| A2 | **JDK 17** (Studio 7.17+ needs 17; older Studio needs 8/11 - use what your Studio version says) | Mule runs on Java | `java -version` in a terminal |
| A3 | **Docker Desktop** | Runs MySQL and a fake mail server in 2 commands, no installs | `docker --version` |
| A4 | **Git** | Hackathon rule 14 | `git --version` |
| A5 | **Postman** (or curl) | To send test requests | opens |
| A6 | **MySQL client** (MySQL Workbench or DBeaver) - optional, you can also use `docker exec` | To look inside the database | connects later in Part C |

---

## PART B - Start MySQL and a fake mail server

**What:** In a terminal run:
```bash
docker run -d --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root mysql:8
docker run -d --name mailhog -p 1025:1025 -p 8025:8025 mailhog/mailhog
```
**Why:** The brief needs a database and an email server. MySQL is the database. **MailHog** pretends to be an SMTP server and shows every email in a web page, so you can test alert emails without sending real mail. Port 1025 = SMTP, 8025 = web inbox.

**Check:** `docker ps` shows both running. Open http://localhost:8025 - an empty inbox appears. (Stop/start later with `docker stop mysql` / `docker start mysql`.)

---

## PART C - Create the database

**What:** Create the schema file Save the SQL below as `schema.sql` anywhere and run:
```bash
docker exec -i mysql mysql -uroot -proot < schema.sql
```
(On Windows PowerShell use: `Get-Content schema.sql | docker exec -i mysql mysql -uroot -proot`.)

**Why each table exists**
* `fraud_events` - one row per scored payment (required by the brief).
* `fraud_rule_hits` - one row per rule that fired, so an analyst sees *why* it was flagged (required).
* `customer_risk_profile` - reference data the consumer reads to "enrich" the event (requirement 6).
* `dead_letter_events` - the **recorded reason** for any event that ends in the DLQ (so "every acknowledged event is in the DB *or* the DLQ with a reason" is provable by SQL).
* A limited user `fraud_app` - the app should not run as root (least privilege).

`src/main/resources/sql/schema.sql`

```sql
CREATE DATABASE IF NOT EXISTS frauddb;
USE frauddb;

CREATE TABLE IF NOT EXISTS fraud_events (
  event_id VARCHAR(30) PRIMARY KEY,
  transaction_id VARCHAR(30) NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  customer_id VARCHAR(20) NOT NULL,
  card_token VARCHAR(40) NOT NULL,
  masked_card VARCHAR(20) NOT NULL,
  amount DECIMAL(14,2) NOT NULL,
  currency CHAR(3) NOT NULL,
  merchant_id VARCHAR(20),
  merchant_country CHAR(2),
  geo_country CHAR(2),
  risk_score INT NOT NULL,
  risk_band VARCHAR(10) NOT NULL,
  decision VARCHAR(10) NOT NULL,
  alert_status VARCHAR(20),
  correlation_id VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_rule_hits (
  hit_seq INT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(30) NOT NULL,
  rule_code VARCHAR(30) NOT NULL,
  rule_name VARCHAR(120) NOT NULL,
  rule_weight INT NOT NULL,
  CONSTRAINT fk_hits_event FOREIGN KEY (event_id) REFERENCES fraud_events (event_id)
);

-- Reference data used by the scoring consumer (profile enrichment)
CREATE TABLE IF NOT EXISTS customer_risk_profile (
  customer_id VARCHAR(20) PRIMARY KEY,
  risk_profile VARCHAR(20) NOT NULL DEFAULT 'STANDARD',
  usual_hour_start INT DEFAULT 6,
  usual_hour_end INT DEFAULT 23
);

-- Dead-letter store: the "recorded reason" the brief demands for events that could not be processed
CREATE TABLE IF NOT EXISTS dead_letter_events (
  dlq_id INT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(30),
  correlation_id VARCHAR(50),
  source_queue VARCHAR(60),
  reason VARCHAR(1000) NOT NULL,
  transient_retries INT DEFAULT 0,
  original_payload TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
  failed_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fe_alert_status ON fraud_events (alert_status, created_at);
CREATE INDEX idx_fe_band_created ON fraud_events (risk_band, created_at);

INSERT IGNORE INTO customer_risk_profile VALUES
 ('CUST-77219','STANDARD',6,23),
 ('CUST-10001','TRUSTED',5,23),
 ('CUST-10002','WATCHLIST',8,20);

-- Least-privilege application user
CREATE USER IF NOT EXISTS 'fraud_app'@'%' IDENTIFIED BY 'ChangeMe!123';
GRANT SELECT, INSERT, UPDATE, DELETE ON frauddb.* TO 'fraud_app'@'%';
```

**Check:**
```bash
docker exec -it mysql mysql -uroot -proot -e "USE frauddb; SHOW TABLES;"
```
You must see: `customer_risk_profile`, `dead_letter_events`, `fraud_events`, `fraud_rule_hits`.

---

## PART D - Create the project in Anypoint Studio

### D1. New project
1. Studio -> **File -> New -> Mule Project**.
2. Project name: `payment-fraud-detection`. Runtime: **Mule 4.4 or newer**. Click **Finish**.
3. In **Package Explorer** (left), open `src/main/mule` and **delete** the empty generated `.xml` file.

*Why:* we bring our own flow files; an empty extra file just causes confusion.

### D2. Add the modules (connectors)
1. Open any flow file later; for now: right-click the project -> **New -> Mule Configuration File** named `temp.xml` (we delete it afterwards). It opens the canvas with the **Mule Palette** on the right.
2. In Mule Palette click **(+) Add Modules**.
3. Drag each of these into the left "Selected modules" list and click Finish: **HTTP, APIkit, Object Store, Database, Email, VM, Secure Properties, API Gateway (Autodiscovery)**. Studio adds the right dependency versions to `pom.xml`. (The **Validation** and **MUnit** modules are added automatically if needed.)
4. Delete `temp.xml`.

*Why:* a Mule app only understands the XML tags of modules it has downloaded. Adding via Studio picks versions that work with your runtime. **Do NOT add Anypoint MQ.**

### D3. MySQL driver (Database module cannot talk to MySQL without it)
Open `pom.xml` (double-click, bottom tab "pom.xml"). Inside `<dependencies>` add:
```xml
<dependency>
  <groupId>com.mysql</groupId>
  <artifactId>mysql-connector-j</artifactId>
  <version>8.3.0</version>
</dependency>
```
Then inside the `mule-maven-plugin` block, in `<configuration>`, add:
```xml
<sharedLibraries>
  <sharedLibrary>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
  </sharedLibrary>
</sharedLibraries>
```
Also add this so MUnit can decrypt secure properties (inside the `munit-maven-plugin` `<configuration>`; if you do not see that plugin yet, skip and instead use the Run-configuration arguments in Part N):
```xml
<systemPropertyVariables><secure.key>mySecretKey</secure.key></systemPropertyVariables>
```
*Why:* the driver is the "translator" for MySQL; `sharedLibraries` makes it visible to the connector.

**Check:** right-click project -> **Maven -> Update Project** (tick Force Update). Wait for the bottom-right progress to finish. `pom.xml` should show no red X.

### D4. Create folders
In Package Explorer right-click `src/main/resources` -> **New -> Folder**, create: `api`, `api/schemas`, `modules`, `sql`. Under `src/test` the folder `munit` already exists once MUnit is added; create it if not.
Also create `docs` and `scripts` at the project root.

---

## PART E - Configuration, secrets, logging

### E1. Properties file (every threshold lives here)
**Why:** the brief says thresholds must change **without redeploying code**. We read all numbers from a YAML file via `${...}` / `p('...')`. In Studio create file `src/main/resources/config-dev.yaml` (right-click folder -> New -> File) and paste:

```yaml
app:
  logCategory: "com.cts.fraud"
http:
  port: "8081"
api:
  id: "REPLACE_WITH_API_MANAGER_ID"
vm:
  queue:
    high: "payments-high-q"
    standard: "payments-std-q"
    dlq: "payments-dlq"
  maxOutstanding: "100000"
db:
  host: "localhost"
  port: "3306"
  user: "fraud_app"
  name: "frauddb"
smtp:
  host: "localhost"
  port: "1025"
  from: "fraud-monitor@example.com"
dedupe:
  ttlHours: "24"
consumer:
  high:
    concurrency: "8"
  standard:
    concurrency: "4"
  transient:
    maxRetries: "720"
    delayMs: "5000"
retry:
  queue:
    max: "3"
    delayMs: "500"
  db:
    max: "3"
    delayMs: "1000"
  smtp:
    max: "3"
    delayMs: "2000"
risk:
  amountLimit: "100000"
  velocityMax: "5"
  velocityWindowSeconds: "300"
  highRiskCategories: "ELECTRONICS,CRYPTO,GAMBLING,JEWELRY,MONEY_TRANSFER"
  usualHourStart: "6"
  usualHourEnd: "23"
  weight:
    amount: "35"
    geo: "30"
    velocity: "20"
    merchant: "17"
    hour: "15"
    watchlist: "10"
  band:
    mediumMin: "30"
    highMin: "60"
    blockMin: "85"
  failSafeScore: "45"
alert:
  distributionList: "risk-team@example.com"
  throttle:
    windowSeconds: "300"
    storeTtlMinutes: "60"
  flushFrequencyMs: "30000"
  reconcileFrequencyMs: "60000"
monitor:
  opsList: "ops-team@example.com"
  dlqThreshold: "0"
  highSpikeThreshold: "20"
  spikeWindowMinutes: "5"
```

What matters most: `risk.*` (rule limits, weights, band boundaries), `alert.*` (throttle), `vm.queue.*` (queue names), `consumer.*` (worker counts and the retry-forever policy for outages). With these weights the sample payment scores **82** = amount 35 + geography 30 + merchant category 17.

### E2. Encrypted secrets (passwords)
**Why:** passwords must never sit in plain text in Git (hackathon rule 7).
1. Search the web for **"MuleSoft Secure Properties Tool"** and download `secure-properties-tool.jar` (from docs.mulesoft.com).
2. Encrypt each value (use single quotes, the `!` is special in some shells):
```bash
java -cp secure-properties-tool.jar com.mulesoft.tools.SecurePropertiesTool string encrypt Blowfish CBC mySecretKey 'ChangeMe!123'
```
Output is a long string. Put it in the file wrapped as `![THE_LONG_STRING]`.
3. Do this three times: DB password (`ChangeMe!123`, the one in schema.sql), SMTP user (anything, e.g. `x`), SMTP password (anything, e.g. `x`).
4. Create `src/main/resources/secure-dev.yaml`:

```yaml
db:
  password: "![ENCRYPTED_DB_PASSWORD]"
smtp:
  user: "![ENCRYPTED_SMTP_USER]"
  password: "![ENCRYPTED_SMTP_PASSWORD]"
```

(Replace the three placeholders with your encrypted strings.)

5. Tell Studio the key: **Run -> Run Configurations -> (your project) -> Arguments tab -> VM arguments** and add:
`-M-Dsecure.key=mySecretKey`
*Why:* the key opens the encryption; it is given at run time and **never committed** to Git.

### E3. Logging with card-number redaction
**Why:** Requirement 10 - a full card number must never appear in logs. This log4j2 file adds a safety net: any run of 13+ digits in any log line is replaced with `[REDACTED-PAN]`, and every line shows the correlation ID. Replace `src/main/resources/log4j2.xml` with:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration>
  <Properties>
    <!-- %replace redacts any run of 13+ contiguous digits (a raw PAN) from every log message: defence in depth.
         Brace-free regex on purpose: log4j2 option parsing dislikes {n,m} inside %replace. -->
    <Property name="LOG_PATTERN">%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%t] [%X{correlationId}] %c{1} - %replace{%m}{\d\d\d\d\d\d\d\d\d\d\d\d\d\d*}{[REDACTED-PAN]}%n</Property>
  </Properties>
  <Appenders>
    <Console name="Console"><PatternLayout pattern="${LOG_PATTERN}"/></Console>
    <RollingFile name="File" fileName="${sys:mule.home}${sys:file.separator}logs${sys:file.separator}fraud-detection.log"
                 filePattern="${sys:mule.home}${sys:file.separator}logs${sys:file.separator}fraud-detection-%i.log">
      <PatternLayout pattern="${LOG_PATTERN}"/>
      <SizeBasedTriggeringPolicy size="10 MB"/>
      <DefaultRolloverStrategy max="5"/>
    </RollingFile>
  </Appenders>
  <Loggers>
    <AsyncLogger name="com.cts.fraud" level="INFO"/>
    <AsyncLogger name="org.mule.runtime.core.internal.processor.LoggerMessageProcessor" level="INFO"/>
    <AsyncRoot level="WARN"><AppenderRef ref="Console"/><AppenderRef ref="File"/></AsyncRoot>
  </Loggers>
</Configuration>
```

**Check (Parts E):** nothing runs yet. Just make sure all three files exist in `src/main/resources`.

---

## PART F - The API contract (RAML + JSON schema)

**Why:** The RAML file is the *contract*: URL `POST /payments/events`, headers, and the JSON shape. APIkit reads it and automatically rejects bad requests with **400** before our code runs - that gives the "validate the payload" requirement. The JSON schema is where the rules live: required fields, positive amount, currency list, and the very important `maskedCardNumber` pattern `^[Xx*]{6,15}[0-9]{4}$`, which means **a real card number is rejected at the door** and can never reach logs, queues or the database.

Create `src/main/resources/api/payment-events-api.raml`:

```yaml
#%RAML 1.0
title: Payment Fraud Detection - Intake API
version: v1
mediaType: application/json
baseUri: https://localhost:8081/api

types:
  PaymentEvent: !include schemas/payment-event.json
  AcceptedResponse:
    type: object
    properties:
      eventId: string
      transactionId: string
      riskScore: integer
      riskBand: { enum: [LOW, MEDIUM, HIGH] }
      status: { enum: [QUEUED_FOR_REVIEW, QUEUED_FOR_SCORING] }
      correlationId: string
  ErrorResponse:
    type: object
    properties:
      errorCode: string
      message: string
      correlationId: string
      fields?: string[]

/payments:
  /events:
    post:
      description: Accept a payment event, apply lightweight risk score, queue by priority.
      headers:
        X-Correlation-ID:
          required: false
          type: string
          description: Optional inbound correlation id. Generated if absent.
      body:
        application/json:
          type: PaymentEvent
      responses:
        202:
          body:
            application/json:
              type: AcceptedResponse
        400:
          body: { application/json: { type: ErrorResponse } }
        409:
          body: { application/json: { type: ErrorResponse } }
        503:
          body: { application/json: { type: ErrorResponse } }
        500:
          body: { application/json: { type: ErrorResponse } }
```

Create `src/main/resources/api/schemas/payment-event.json`:

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "required": ["eventId", "eventTimestamp", "transaction", "card", "customer", "merchant"],
  "properties": {
    "eventId": { "type": "string", "minLength": 1, "maxLength": 30, "pattern": "^[A-Za-z0-9_-]+$" },
    "eventTimestamp": { "type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})$" },
    "transaction": {
      "type": "object",
      "required": ["transactionId", "amount", "currency", "channel"],
      "properties": {
        "transactionId": { "type": "string", "minLength": 1, "maxLength": 30 },
        "amount": { "type": "number", "minimum": 0, "exclusiveMinimum": true, "maximum": 999999999999.99 },
        "currency": { "enum": ["INR", "USD", "EUR", "GBP", "SGD", "AED", "AUD", "JPY"] },
        "channel": { "enum": ["ONLINE", "POS", "ATM", "MOBILE", "IVR"] },
        "entryMode": { "type": "string", "maxLength": 30 }
      }
    },
    "card": {
      "type": "object",
      "required": ["cardToken", "maskedCardNumber", "issuingCountry"],
      "properties": {
        "cardToken": { "type": "string", "minLength": 1, "maxLength": 40 },
        "maskedCardNumber": { "type": "string", "pattern": "^[Xx*]{6,15}[0-9]{4}$" },
        "cardType": { "enum": ["CREDIT", "DEBIT", "PREPAID"] },
        "issuingCountry": { "type": "string", "pattern": "^[A-Z]{2}$" }
      }
    },
    "customer": {
      "type": "object",
      "required": ["customerId"],
      "properties": {
        "customerId": { "type": "string", "minLength": 1, "maxLength": 20 },
        "name": { "type": "string", "maxLength": 100 },
        "email": { "type": "string", "maxLength": 120 },
        "riskProfile": { "enum": ["STANDARD", "TRUSTED", "WATCHLIST"] }
      }
    },
    "merchant": {
      "type": "object",
      "required": ["merchantId", "merchantCategory"],
      "properties": {
        "merchantId": { "type": "string", "minLength": 1, "maxLength": 20 },
        "merchantName": { "type": "string", "maxLength": 120 },
        "merchantCategory": { "type": "string", "maxLength": 40 },
        "merchantCountry": { "type": "string", "pattern": "^[A-Z]{2}$" }
      }
    },
    "device": {
      "type": "object",
      "properties": {
        "ipAddress": { "type": "string" },
        "deviceId": { "type": "string" },
        "geoCountry": { "type": "string", "pattern": "^[A-Z]{2}$" }
      }
    }
  }
}
```

**Check:** open the RAML in Studio - no red markers. Do **not** use "Generate flows from RAML": our flow file already contains them (generating would duplicate).

---

## PART G - DataWeave modules (the "business brain")

Create these four files in `src/main/resources/modules/`. Each is plain text; right-click folder -> New -> File.

### G1. `RiskRules.dwl` - rules, banding, decisions (requirements 2, 3, 6)
**Why a separate file:** the brief asks for scoring "in an externalised DataWeave module". Each rule is a small function returning a *hit* or `null`. `evaluate` adds the weights (max 100), `band` converts score to LOW/MEDIUM/HIGH, `decide` gives APPROVE/REVIEW/BLOCK, and `finalize` wraps everything in `try` so **any scoring crash fails safe to REVIEW** (negative scenario 5).

```dataweave
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
```

### G2. `Masking.dwl` (requirement 10)
`maskCard` keeps only the last four characters. `sanitize` wipes anything that looks like a card number from free text (error messages, DLQ reasons).

```dataweave
%dw 2.0
/* Keep only the last four characters; everything else becomes X. */
fun maskCard(s) = do {
  var str = (s default "") as String
  var n = sizeOf(str)
  ---
  if (n <= 4) "XXXX" else (((1 to (n - 4)) map "X") joinBy "") ++ str[-4 to -1]
}

/* Strip anything that looks like a raw card number from free text (error messages, reasons). */
fun sanitize(s) = (s default "") as String replace /(\d[ -]?){13,19}/ with "[REDACTED]"
```

### G3. `ErrorResponse.dwl`
One place that builds the error JSON `{errorCode, message, correlationId, fields}`. `fieldPaths` extracts only the **names/paths** of bad fields, never their values (a value might be a card number).

```dataweave
%dw 2.0
fun build(code, message, corrId, fields = []) = {
  errorCode: code,
  message: message,
  correlationId: corrId,
  (fields: fields) if (!isEmpty(fields))
}

/* Only JSON-pointer paths are echoed back (never the offending VALUE, which could be a card number). */
fun fieldPaths(desc) = ((desc default "") scan /#\/[A-Za-z0-9_\/\[\]]+/) map ($[0]) distinctBy $
```

### G4. `AlertTemplates.dwl`
Builds the email subject/body exactly in the brief's format, plus the aggregate email.

```dataweave
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
```

---

## PART H - Global configuration and global error handler

**Why:** Mule needs each connector *configured once* (host, port, credentials) and referenced by name from flows. We also set a **global error handler** (requirement 11): any error in any flow lands here and is turned into a clean response containing a correlation ID and no internals:
* bad request -> **400** (field paths only)
* duplicate event -> **409**
* queue/DB down -> **503** ("not accepted, retry")
* anything else -> **500** with only the correlation ID

Also defined here: the persistent **VM queues**, four **Object Stores** (dedupe, velocity, alert-throttle, spike/DLQ cooldown), and the sub-flow that loads all `risk.*` properties into one variable.

Create `src/main/mule/global-config.xml` (right-click `src/main/mule` -> New -> Mule Configuration File; then switch to the **Configuration XML** tab at the bottom and paste everything):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- ===== Environment, properties, secure properties ===== -->
  <global-property name="env" value="dev" doc:name="Global Property"/>
  <configuration-properties file="config-${env}.yaml" doc:name="Config properties"/>
  <secure-properties:config name="Secure_Properties_Config" file="secure-${env}.yaml" key="${secure.key}" doc:name="Secure Properties">
    <secure-properties:encrypt algorithm="Blowfish"/>
  </secure-properties:config>
  <configuration defaultErrorHandler-ref="globalErrorHandler" doc:name="Configuration"/>


  <!-- ===== Connector configs ===== -->
  <http:listener-config name="HTTP_Listener_config" doc:name="HTTP Listener config">
    <http:listener-connection host="0.0.0.0" port="${http.port}"/>
  </http:listener-config>

  <apikit:config name="payment-api-config" api="api/payment-events-api.raml" outboundHeadersMapName="outboundHeaders" httpStatusVarName="httpStatus" doc:name="APIkit Config"/>


  <db:config name="Database_Config" doc:name="Database Config">
    <db:my-sql-connection host="${db.host}" port="${db.port}" user="${db.user}" password="${secure::db.password}" database="${db.name}">
      <reconnection><reconnect frequency="2000" count="3"/></reconnection>
    </db:my-sql-connection>
  </db:config>

  <!-- For real SMTP use <email:smtps-connection>; for local dev run MailHog/smtp4dev on port 1025 -->
  <email:smtp-config name="SMTP_Config" doc:name="SMTP Config">
    <email:smtp-connection host="${smtp.host}" port="${smtp.port}" user="${secure::smtp.user}" password="${secure::smtp.password}"/>
  </email:smtp-config>

  <vm:config name="VM_Config" doc:name="VM Config">
    <vm:queues>
      <!-- PERSISTENT = written to disk, survives an app restart. Three payment queues + one alert queue. -->
      <vm:queue queueName="${vm.queue.high}" queueType="PERSISTENT" maxOutstandingMessages="${vm.maxOutstanding}"/>
      <vm:queue queueName="${vm.queue.standard}" queueType="PERSISTENT" maxOutstandingMessages="${vm.maxOutstanding}"/>
      <vm:queue queueName="${vm.queue.dlq}" queueType="PERSISTENT" maxOutstandingMessages="${vm.maxOutstanding}"/>
      <vm:queue queueName="alertQueue" queueType="PERSISTENT"/>
    </vm:queues>
  </vm:config>

  <!-- ===== Object stores ===== -->
  <os:object-store name="dedupStore" persistent="true" entryTtl="${dedupe.ttlHours}" entryTtlUnit="HOURS" maxEntries="500000" doc:name="Dedupe Store"/>
  <os:object-store name="velocityStore" persistent="true" entryTtl="${risk.velocityWindowSeconds}" entryTtlUnit="SECONDS" maxEntries="500000" doc:name="Velocity Store"/>
  <os:object-store name="alertThrottleStore" persistent="true" entryTtl="${alert.throttle.storeTtlMinutes}" entryTtlUnit="MINUTES" maxEntries="100000" doc:name="Alert Throttle Store"/>
  <os:object-store name="spikeStore" persistent="true" entryTtl="10" entryTtlUnit="MINUTES" doc:name="Spike cooldown Store"/>

  <!-- ===== GLOBAL ERROR HANDLER (default for every flow without its own handler) ===== -->
  <error-handler name="globalErrorHandler">

    <!-- 400: malformed JSON / schema violation. logException=false so offending values never hit the log -->
    <on-error-propagate type="APIKIT:BAD_REQUEST" enableNotifications="false" logException="false">
      <logger level="WARN" category="${app.logCategory}.error" message="#['INVALID_PAYMENT_EVENT corrId=' ++ correlationId]"/>
      <ee:transform doc:name="400 body">
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::ErrorResponse
output application/json
---
ErrorResponse::build("INVALID_PAYMENT_EVENT", "Payment event failed validation. See 'fields' for offending paths.", correlationId, ErrorResponse::fieldPaths(error.description))]]></ee:set-payload></ee:message>
        <ee:variables><ee:set-variable variableName="httpStatus">400</ee:set-variable></ee:variables>
      </ee:transform>
    </on-error-propagate>

    <!-- other APIkit router errors -->
    <on-error-propagate type="APIKIT:NOT_FOUND, APIKIT:METHOD_NOT_ALLOWED, APIKIT:NOT_ACCEPTABLE, APIKIT:UNSUPPORTED_MEDIA_TYPE, APIKIT:NOT_IMPLEMENTED" enableNotifications="false" logException="false">
      <logger level="WARN" category="${app.logCategory}.error" message="#['API_ROUTING_ERROR type=' ++ error.errorType.identifier ++ ' corrId=' ++ correlationId]"/>
      <ee:transform doc:name="4xx body">
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::ErrorResponse
output application/json
---
ErrorResponse::build(error.errorType.identifier, "Request could not be processed.", correlationId)]]></ee:set-payload></ee:message>
        <ee:variables><ee:set-variable variableName="httpStatus"><![CDATA[%dw 2.0
output application/java
---
{ "NOT_FOUND": 404, "METHOD_NOT_ALLOWED": 405, "NOT_ACCEPTABLE": 406, "UNSUPPORTED_MEDIA_TYPE": 415, "NOT_IMPLEMENTED": 501 }[error.errorType.identifier] default 400]]></ee:set-variable></ee:variables>
      </ee:transform>
    </on-error-propagate>

    <!-- 409: duplicate event id -->
    <on-error-propagate type="OS:KEY_ALREADY_EXISTS" enableNotifications="false" logException="false">
      <logger level="WARN" category="${app.logCategory}.error" message="#['DUPLICATE_EVENT eventId=' ++ (vars.eventId default 'n/a') ++ ' corrId=' ++ correlationId]"/>
      <ee:transform doc:name="409 body">
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::ErrorResponse
output application/json
---
ErrorResponse::build("DUPLICATE_EVENT", "This eventId was already accepted. No further processing performed.", correlationId)]]></ee:set-payload></ee:message>
        <ee:variables><ee:set-variable variableName="httpStatus">409</ee:set-variable></ee:variables>
      </ee:transform>
    </on-error-propagate>

    <!-- 503: queue / dependency unavailable -> NOT accepted, caller must retry -->
    <on-error-propagate type="MULE:RETRY_EXHAUSTED, DB:CONNECTIVITY" enableNotifications="true" logException="false">
      <logger level="ERROR" category="${app.logCategory}.error" message="#['DEPENDENCY_UNAVAILABLE eventId=' ++ (vars.eventId default 'n/a') ++ ' type=' ++ error.errorType.identifier ++ ' corrId=' ++ correlationId]"/>
      <ee:transform doc:name="503 body">
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::ErrorResponse
output application/json
---
ErrorResponse::build("SERVICE_UNAVAILABLE", "Event could not be queued. It was NOT accepted; please retry.", correlationId)]]></ee:set-payload></ee:message>
        <ee:variables>
          <ee:set-variable variableName="httpStatus">503</ee:set-variable>
          <ee:set-variable variableName="outboundHeaders"><![CDATA[%dw 2.0
output application/java
---
{ "Retry-After": "5" }]]></ee:set-variable>
        </ee:variables>
      </ee:transform>
    </on-error-propagate>

    <!-- 500: anything else. No internal details leak; only the correlation id -->
    <on-error-propagate type="ANY" enableNotifications="true" logException="false">
      <logger level="ERROR" category="${app.logCategory}.error" message="#['UNEXPECTED_ERROR type=' ++ error.errorType.namespace ++ ':' ++ error.errorType.identifier ++ ' eventId=' ++ (vars.eventId default 'n/a') ++ ' corrId=' ++ correlationId]"/>
      <ee:transform doc:name="500 body">
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::ErrorResponse
output application/json
---
ErrorResponse::build("INTERNAL_ERROR", "An unexpected error occurred. Quote the correlationId to support.", correlationId)]]></ee:set-payload></ee:message>
        <ee:variables><ee:set-variable variableName="httpStatus">500</ee:set-variable></ee:variables>
      </ee:transform>
    </on-error-propagate>
  </error-handler>

  <!-- ===== Shared: risk config loaded from externalised properties ===== -->
  <sub-flow name="load-risk-config-sf">
    <ee:transform doc:name="Build riskCfg">
      <ee:variables>
        <ee:set-variable variableName="riskCfg"><![CDATA[%dw 2.0
output application/java
---
{
  amountLimit: p('risk.amountLimit') as Number,
  velocityMax: p('risk.velocityMax') as Number,
  highRiskCategories: (p('risk.highRiskCategories') splitBy ",") map upper(trim($)),
  usualHourStart: p('risk.usualHourStart') as Number,
  usualHourEnd: p('risk.usualHourEnd') as Number,
  wAmount: p('risk.weight.amount') as Number,
  wGeo: p('risk.weight.geo') as Number,
  wVelocity: p('risk.weight.velocity') as Number,
  wMerchant: p('risk.weight.merchant') as Number,
  wHour: p('risk.weight.hour') as Number,
  wWatchlist: p('risk.weight.watchlist') as Number,
  mediumMin: p('risk.band.mediumMin') as Number,
  highMin: p('risk.band.highMin') as Number,
  blockMin: p('risk.band.blockMin') as Number,
  failSafeScore: p('risk.failSafeScore') as Number
}]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
  </sub-flow>
</mule>
```

**Studio tip:** the three tabs at the bottom of a flow file are *Message Flow* (canvas), *Global Elements* (the configs) and *Configuration XML* (raw). Pasting XML is the fast way; the canvas then shows it graphically so you can click boxes to see settings.

**Check:** the Global Elements tab lists HTTP Listener config, APIkit config, Database Config, SMTP Config, VM Config. No red errors. A red "unknown namespace" error means a module from D2 is missing - add it.

### H2. Autodiscovery (separate file on purpose)
Autodiscovery connects the app to API Manager (Part P). It needs Anypoint credentials, so **for local runs we keep it switched off**. Create `src/main/mule/autodiscovery.xml` with the content below, then **rename it to `autodiscovery.xml.off`** (right-click -> Rename). Rename back when you deploy to CloudHub.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- API Manager auto-discovery. Needs: api.id (config-*.yaml) + runtime properties anypoint.platform.client_id / client_secret.
       For the first LOCAL runs, rename this file to autodiscovery.xml.off so Studio does not try to reach Anypoint. -->
  <api-gateway:autodiscovery apiId="${api.id}" ignoreBasePath="true" flowRef="payment-events-api-main" doc:name="API Autodiscovery"/>
</mule>
```

---

## PART I - The intake API flow (requirements 1, 2, 3, 4, 5)

**What happens for one request**
1. APIkit router validates JSON against the RAML/schema (bad -> 400).
2. `dedupe-event-sf`: stores the `eventId` in the Object Store with *fail if present* -> a repeat gives **409** (no double scoring, rows or emails).
3. `velocity-update-sf`: keeps a list of timestamps per **card token** in the Object Store (entries expire = the "time to live"). Counting those in the last N seconds feeds the velocity rule.
4. `score-event-sf`: runs the rules and band. If scoring crashes it assigns MEDIUM + a "failed" flag so the consumer forces REVIEW (never approves).
5. `publish-scored-event-sf`: HIGH -> `payments-high-q`, others -> `payments-std-q`. It logs the routing decision, retries publishing, and if the HIGH queue fails it logs a **PRIORITY_FALLBACK** and uses the standard queue (the consumer re-checks). If nothing works the caller gets **503** - the event is *not* reported as accepted.
6. Returns **202** with score, band, status and correlation ID.
7. If anything fails after step 2, the dedupe key is **released** so the caller can retry safely.

Create `src/main/mule/payment-events-api.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- ===== MAIN FLOW: listener -> APIkit router. Errors fall to globalErrorHandler ===== -->
  <flow name="payment-events-api-main">
    <http:listener config-ref="HTTP_Listener_config" path="/api/*" doc:name="Listener">
      <http:response statusCode="#[vars.httpStatus default 200]">
        <http:headers><![CDATA[#[((vars.outboundHeaders default {}) ++ { "X-Correlation-ID": correlationId })]]]></http:headers>
      </http:response>
      <http:error-response statusCode="#[vars.httpStatus default 500]">
        <http:body>#[payload]</http:body>
        <http:headers><![CDATA[#[((vars.outboundHeaders default {}) ++ { "X-Correlation-ID": correlationId })]]]></http:headers>
      </http:error-response>
    </http:listener>
    <apikit:router config-ref="payment-api-config" doc:name="APIkit Router"/>
  </flow>

  <!-- ===== POST /payments/events ===== -->
  <flow name="post:\payments\events:application\json:payment-api-config">
    <set-variable variableName="event" value="#[payload]" doc:name="Keep event"/>
    <set-variable variableName="eventId" value="#[payload.eventId]" doc:name="eventId"/>
    <set-variable variableName="corrId" value="#[correlationId]" doc:name="corrId"/>
    <logger level="INFO" category="${app.logCategory}.intake" message="#['INTAKE_RECEIVED corrId=' ++ vars.corrId ++ ' eventId=' ++ vars.eventId ++ ' txn=' ++ vars.event.transaction.transactionId ++ ' card=' ++ (vars.event.card.maskedCardNumber[-4 to -1] default '')]"/>

    <flow-ref name="load-risk-config-sf" doc:name="Load risk config"/>
    <flow-ref name="dedupe-event-sf" doc:name="Dedupe"/>
    <flow-ref name="velocity-update-sf" doc:name="Velocity"/>
    <flow-ref name="score-event-sf" doc:name="Score + Band"/>
    <flow-ref name="publish-scored-event-sf" doc:name="Publish to priority queue"/>

    <ee:transform doc:name="202 response">
      <ee:message><ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{
  eventId: vars.event.eventId,
  transactionId: vars.event.transaction.transactionId,
  riskScore: vars.scoring.riskScore,
  riskBand: vars.scoring.riskBand,
  status: if (vars.scoring.riskBand == "HIGH") "QUEUED_FOR_REVIEW" else "QUEUED_FOR_SCORING",
  correlationId: vars.corrId
}]]></ee:set-payload></ee:message>
      <ee:variables><ee:set-variable variableName="httpStatus">202</ee:set-variable></ee:variables>
    </ee:transform>
    <logger level="INFO" category="${app.logCategory}.intake" message="#['INTAKE_ACCEPTED corrId=' ++ vars.corrId ++ ' eventId=' ++ vars.eventId ++ ' score=' ++ vars.scoring.riskScore ++ ' band=' ++ vars.scoring.riskBand ++ ' queue=' ++ vars.targetQueue]"/>

    <error-handler>
      <!-- COMPENSATION: anything except "duplicate" must release the dedupe key so the caller's retry is not rejected as duplicate -->
      <on-error-propagate when="#[error.errorType.identifier != 'KEY_ALREADY_EXISTS']" logException="false" enableNotifications="false">
        <try doc:name="Release dedupe key">
          <os:remove key="#[vars.eventId]" objectStore="dedupStore"/>
          <error-handler>
            <on-error-continue>
              <logger level="WARN" category="${app.logCategory}.intake" message="#['DEDUPE_RELEASE_SKIPPED eventId=' ++ (vars.eventId default 'n/a')]"/>
            </on-error-continue>
          </error-handler>
        </try>
      </on-error-propagate>
    </error-handler>
  </flow>

  <!-- Atomic duplicate detection: store with failIfPresent => OS:KEY_ALREADY_EXISTS -->
  <sub-flow name="dedupe-event-sf">
    <os:store key="#[vars.eventId]" objectStore="dedupStore" failIfPresent="true" doc:name="Store eventId (fail if present)">
      <os:value>#[vars.corrId]</os:value>
    </os:store>
  </sub-flow>

  <!-- Velocity: sliding window of epoch-seconds per card token -->
  <sub-flow name="velocity-update-sf">
    <os:retrieve key="#['vel-' ++ vars.event.card.cardToken]" objectStore="velocityStore" target="velHistory" doc:name="Get history">
      <os:default-value>#[[]]</os:default-value>
    </os:retrieve>
    <ee:transform doc:name="Slide window">
      <ee:variables>
        <ee:set-variable variableName="velWindow"><![CDATA[%dw 2.0
output application/java
var nowEpoch = now() as Number
var windowSec = p('risk.velocityWindowSeconds') as Number
---
((vars.velHistory default []) filter ($ > nowEpoch - windowSec)) ++ [nowEpoch]]]></ee:set-variable>
        <ee:set-variable variableName="velocityCount"><![CDATA[%dw 2.0
output application/java
var nowEpoch = now() as Number
var windowSec = p('risk.velocityWindowSeconds') as Number
---
sizeOf(((vars.velHistory default []) filter ($ > nowEpoch - windowSec)) ++ [nowEpoch])]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <os:store key="#['vel-' ++ vars.event.card.cardToken]" objectStore="velocityStore" failIfPresent="false" doc:name="Save history">
      <os:value>#[vars.velWindow]</os:value>
    </os:store>
  </sub-flow>

  <!-- Score + band. FAIL-SAFE: any exception => MEDIUM + scoringFailed flag => consumer forces REVIEW -->
  <sub-flow name="score-event-sf">
    <try doc:name="Try score">
      <ee:transform doc:name="Evaluate rules">
        <ee:variables>
          <ee:set-variable variableName="scoring"><![CDATA[%dw 2.0
import modules::RiskRules
output application/java
var r = RiskRules::evaluate(vars.event, { velocityCount: vars.velocityCount, profile: null }, vars.riskCfg)
---
{ riskScore: r.score, riskBand: RiskRules::band(r.score, vars.riskCfg), ruleHits: r.hits, scoringFailed: false }]]></ee:set-variable>
        </ee:variables>
      </ee:transform>
      <error-handler>
        <on-error-continue enableNotifications="true" logException="false">
          <logger level="ERROR" category="${app.logCategory}.scoring" message="#['SCORING_FAILURE failSafe=REVIEW eventId=' ++ vars.eventId ++ ' corrId=' ++ vars.corrId ++ ' type=' ++ error.errorType.identifier]"/>
          <ee:transform doc:name="Fail-safe scoring">
            <ee:variables>
              <ee:set-variable variableName="scoring"><![CDATA[%dw 2.0
output application/java
---
{ riskScore: vars.riskCfg.failSafeScore, riskBand: "MEDIUM", ruleHits: [], scoringFailed: true }]]></ee:set-variable>
            </ee:variables>
          </ee:transform>
        </on-error-continue>
      </error-handler>
    </try>
  </sub-flow>

  <!-- Priority routing + explicit assertion + logged fallback -->
  <sub-flow name="publish-scored-event-sf">
    <ee:transform doc:name="Build envelope">
      <ee:variables>
        <ee:set-variable variableName="envelope"><![CDATA[%dw 2.0
output application/json
---
{ correlationId: vars.corrId, receivedAt: now(), velocityCount: vars.velocityCount, scoring: vars.scoring, event: vars.event }]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <set-variable variableName="targetQueue" value="#[if (vars.scoring.riskBand == 'HIGH') p('vm.queue.high') else p('vm.queue.standard')]" doc:name="Routing decision"/>
    <logger level="INFO" category="${app.logCategory}.routing" message="#['ROUTING eventId=' ++ vars.eventId ++ ' band=' ++ vars.scoring.riskBand ++ ' queue=' ++ vars.targetQueue ++ ' corrId=' ++ vars.corrId]"/>
    <try doc:name="Publish with fallback">
      <flow-ref name="queue-publish-sf"/>
      <error-handler>
        <on-error-continue>
          <choice>
            <when expression="#[vars.scoring.riskBand == 'HIGH']">
              <!-- HIGH could not reach the high queue: fall back to standard (consumer re-checks band, never approves silently) -->
              <logger level="WARN" category="${app.logCategory}.routing" message="#['PRIORITY_FALLBACK eventId=' ++ vars.eventId ++ ' HIGH event diverted to standard queue corrId=' ++ vars.corrId]"/>
              <set-variable variableName="targetQueue" value="#[p('vm.queue.standard')]"/>
              <flow-ref name="queue-publish-sf"/>
            </when>
            <otherwise>
              <!-- re-throw: the caller must NOT be told the event was accepted -->
              <raise-error type="MULE:RETRY_EXHAUSTED" description="Queue unavailable"/>
            </otherwise>
          </choice>
        </on-error-continue>
      </error-handler>
    </try>
  </sub-flow>

  <sub-flow name="queue-publish-sf">
    <until-successful maxRetries="${retry.queue.max}" millisBetweenRetries="${retry.queue.delayMs}" doc:name="Retry publish">
      <vm:publish config-ref="VM_Config" queueName="#[vars.targetQueue]" timeout="5" timeoutUnit="SECONDS" doc:name="Publish to VM queue">
        <vm:content>#[vars.envelope]</vm:content>
      </vm:publish>
    </until-successful>
  </sub-flow>
</mule>
```

### FIRST RUN (milestone 1) - run the app
1. Right-click the project -> **Run As -> Mule Application**. (VM argument `-M-Dsecure.key=mySecretKey` from E2 must be set.)
2. Wait for `Started app 'payment-fraud-detection'` / status **DEPLOYED** in the Console.
3. Postman: `POST http://localhost:8081/api/payments/events`, header `Content-Type: application/json`, body = the sample JSON from the brief.

Expected:
```json
{ "eventId": "PAY-EVT-2026-884512", "transactionId": "TXN-99120455", "riskScore": 82,
  "riskBand": "HIGH", "status": "QUEUED_FOR_REVIEW", "correlationId": "..." }
```
Send the **same** body again -> **409**. Change eventId and set `"amount": -5` -> **400**.

> Right now the consumer files are not added, so messages just wait in the VM queue (they are persistent). Stop the app (red square), continue to Part J, run again and they get processed.

**If it does not start**, read the **first red ERROR** in the console - see Troubleshooting at the end.

---

## PART J - Consumer and database persistence (requirements 6, 7)

### J1. The consumer - `src/main/mule/scoring-consumer.xml`
**What:** two **VM listeners**: the HIGH queue with 8 consumer threads, the standard queue with 4. Each message goes through `process-scored-event-sf`:
parse -> assert routing -> enrich with DB profile (missing customer = default profile + log "enrichment incomplete") -> finalise decision (APPROVE/REVIEW/BLOCK) -> save -> hand to the alert queue if HIGH/BLOCK.

**The reliability heart (replaces MQ ack/nack):**
| Situation | What the code does | Why |
|---|---|---|
| DB down / timeout | wait `consumer.transient.delayMs` (5 s), then **publish the same message back to its queue** with a retry counter; up to 720 times (~1 hour) | the event is never lost and is processed when the DB returns (acceptance #7) |
| Retries used up, bad JSON, any other error | publish to **payments-dlq** with reason + masked original payload | "every event ends in DB or DLQ with a reason" |
| DLQ publish itself fails | log the masked payload at ERROR so an operator can replay | last resort, never silent |

Create it:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- ===== Two listeners: the HIGH queue gets MORE consumer threads, so suspicious activity is always scored and alerted first ===== -->
  <flow name="scoring-consumer-high-flow">
    <vm:listener config-ref="VM_Config" queueName="${vm.queue.high}" numberOfConsumers="${consumer.high.concurrency}" transactionalAction="INDIFFERENT" doc:name="Listener HIGH queue"/>
    <set-variable variableName="sourceQueue" value="${vm.queue.high}" doc:name="sourceQueue"/>
    <flow-ref name="process-scored-event-sf" doc:name="Process event"/>
    <error-handler ref="consumerErrorHandler"/>
  </flow>

  <flow name="scoring-consumer-standard-flow">
    <vm:listener config-ref="VM_Config" queueName="${vm.queue.standard}" numberOfConsumers="${consumer.standard.concurrency}" transactionalAction="INDIFFERENT" doc:name="Listener STANDARD queue"/>
    <set-variable variableName="sourceQueue" value="${vm.queue.standard}" doc:name="sourceQueue"/>
    <flow-ref name="process-scored-event-sf" doc:name="Process event"/>
    <error-handler ref="consumerErrorHandler"/>
  </flow>

  <sub-flow name="process-scored-event-sf">
    <!-- keep the raw text FIRST: it is what we re-queue or dead-letter if anything fails later -->
    <set-variable variableName="rawBody" value="#[payload as String]" doc:name="rawBody"/>

    <ee:transform doc:name="Parse envelope">
      <ee:variables>
        <ee:set-variable variableName="envelope"><![CDATA[%dw 2.0
output application/java
---
read(vars.rawBody, "application/json")]]></ee:set-variable>
        <ee:set-variable variableName="corrId"><![CDATA[%dw 2.0
output application/java
---
read(vars.rawBody, "application/json").correlationId default correlationId]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <set-variable variableName="eventId" value="#[vars.envelope.event.eventId]" doc:name="eventId"/>
    <logger level="INFO" category="${app.logCategory}.consumer" message="#['CONSUME_START eventId=' ++ vars.eventId ++ ' corrId=' ++ vars.corrId ++ ' queue=' ++ vars.sourceQueue ++ ' transientRetries=' ++ (vars.envelope.transientRetries default 0)]"/>

    <!-- Priority-misrouting assertion: a HIGH event must never be silently treated as standard -->
    <choice>
      <when expression="#[vars.envelope.scoring.riskBand == 'HIGH' and vars.sourceQueue != p('vm.queue.high')]">
        <logger level="WARN" category="${app.logCategory}.routing" message="#['PRIORITY_MISROUTE eventId=' ++ vars.eventId ++ ' HIGH event consumed from standard queue; handled as HIGH corrId=' ++ vars.corrId]"/>
      </when>
      <otherwise><logger level="DEBUG" category="${app.logCategory}.routing" message="routing ok"/></otherwise>
    </choice>

    <flow-ref name="load-risk-config-sf" doc:name="Load risk config"/>
    <flow-ref name="enrich-customer-profile-sf" doc:name="Enrich with profile"/>

    <ee:transform doc:name="Finalize decision">
      <ee:variables>
        <ee:set-variable variableName="final"><![CDATA[%dw 2.0
import modules::RiskRules
output application/java
---
RiskRules::finalize(vars.envelope, vars.profile, vars.riskCfg)]]></ee:set-variable>
        <ee:set-variable variableName="alertRequired"><![CDATA[%dw 2.0
import modules::RiskRules
output application/java
var f = RiskRules::finalize(vars.envelope, vars.profile, vars.riskCfg)
---
f.riskBand == "HIGH" or f.decision == "BLOCK"]]></ee:set-variable>
      </ee:variables>
    </ee:transform>

    <!-- If this throws (DB down) the message is put back on the queue by consumerErrorHandler - it is never dropped -->
    <flow-ref name="persist-fraud-event-sf" doc:name="Persist"/>

    <choice>
      <when expression="#[vars.alertRequired]">
        <!-- The DB row already says alert_status=PENDING, so if this hand-off fails the reconciler recovers it -->
        <try doc:name="Hand off to alert pipeline">
          <flow-ref name="enqueue-alert-sf"/>
          <error-handler>
            <on-error-continue logException="false">
              <logger level="ERROR" category="${app.logCategory}.alert" message="#['ALERT_ENQUEUE_FAILED eventId=' ++ vars.eventId ++ ' reconciler will recover corrId=' ++ vars.corrId]"/>
            </on-error-continue>
          </error-handler>
        </try>
      </when>
    </choice>

    <logger level="INFO" category="${app.logCategory}.consumer" message="#['CONSUME_DONE eventId=' ++ vars.eventId ++ ' score=' ++ vars.final.riskScore ++ ' band=' ++ vars.final.riskBand ++ ' decision=' ++ vars.final.decision ++ ' corrId=' ++ vars.corrId]"/>
  </sub-flow>

  <!-- Customer profile: missing => default profile + enrichment flag. DB outage => retried, then bubbles up => re-queue -->
  <sub-flow name="enrich-customer-profile-sf">
    <until-successful maxRetries="${retry.db.max}" millisBetweenRetries="${retry.db.delayMs}">
      <db:select config-ref="Database_Config" target="profileRows" doc:name="Select profile">
        <db:sql>SELECT customer_id, risk_profile, usual_hour_start, usual_hour_end FROM customer_risk_profile WHERE customer_id = :customerId</db:sql>
        <db:input-parameters>#[{ customerId: vars.envelope.event.customer.customerId }]</db:input-parameters>
      </db:select>
    </until-successful>
    <choice>
      <when expression="#[isEmpty(vars.profileRows)]">
        <logger level="WARN" category="${app.logCategory}.consumer" message="#['PROFILE_NOT_FOUND eventId=' ++ vars.eventId ++ ' default profile applied (enrichment incomplete) corrId=' ++ vars.corrId]"/>
        <set-variable variableName="enrichmentIncomplete" value="#[true]"/>
        <set-variable variableName="profile" value="#[{ riskProfile: 'STANDARD', usualHourStart: p('risk.usualHourStart') as Number, usualHourEnd: p('risk.usualHourEnd') as Number }]"/>
      </when>
      <otherwise>
        <set-variable variableName="enrichmentIncomplete" value="#[false]"/>
        <set-variable variableName="profile" value="#[{ riskProfile: vars.profileRows[0].risk_profile, usualHourStart: vars.profileRows[0].usual_hour_start, usualHourEnd: vars.profileRows[0].usual_hour_end }]"/>
      </otherwise>
    </choice>
  </sub-flow>

  <!-- A deliberate pause. until-successful waits between attempts, so we make it fail twice on purpose. -->
  <sub-flow name="transient-backoff-sf">
    <try doc:name="Backoff wait">
      <until-successful maxRetries="1" millisBetweenRetries="${consumer.transient.delayMs}">
        <raise-error type="APP:BACKOFF" description="intentional wait before re-queue"/>
      </until-successful>
      <error-handler>
        <on-error-continue logException="false">
          <logger level="DEBUG" category="${app.logCategory}.consumer" message="backoff finished"/>
        </on-error-continue>
      </error-handler>
    </try>
  </sub-flow>

  <!-- ===== Consumer error handler ===== -->
  <error-handler name="consumerErrorHandler">

    <!-- 1) TRANSIENT infrastructure problem (DB down etc.) and attempts remain => wait, then put the SAME event back on its queue.
            The event is never lost and never acknowledged as processed. Does not count towards poison-message limits. -->
    <on-error-continue type="DB:CONNECTIVITY, DB:QUERY_EXECUTION, MULE:RETRY_EXHAUSTED"
                       when="#[vars.envelope != null and (vars.envelope.transientRetries default 0) &lt; (p('consumer.transient.maxRetries') as Number)]" logException="false">
      <logger level="WARN" category="${app.logCategory}.consumer" message="#['TRANSIENT_FAILURE_REQUEUE eventId=' ++ (vars.eventId default 'n/a') ++ ' type=' ++ error.errorType.identifier ++ ' retries=' ++ (vars.envelope.transientRetries default 0) ++ ' corrId=' ++ (vars.corrId default 'n/a')]"/>
      <flow-ref name="transient-backoff-sf"/>
      <try doc:name="Re-queue">
        <ee:transform doc:name="Requeue message">
          <ee:variables>
            <ee:set-variable variableName="requeueMsg"><![CDATA[%dw 2.0
output application/json
---
vars.envelope ++ { transientRetries: (vars.envelope.transientRetries default 0) + 1 }]]></ee:set-variable>
          </ee:variables>
        </ee:transform>
        <vm:publish config-ref="VM_Config" queueName="#[vars.sourceQueue]" timeout="5" timeoutUnit="SECONDS" doc:name="Publish back to source queue">
          <vm:content>#[vars.requeueMsg]</vm:content>
        </vm:publish>
        <error-handler>
          <!-- last resort: log the (already card-masked) payload so an operator can replay it -->
          <on-error-continue logException="false">
            <logger level="ERROR" category="${app.logCategory}.consumer" message="#['REQUEUE_FAILED eventId=' ++ (vars.eventId default 'n/a') ++ ' corrId=' ++ (vars.corrId default 'n/a') ++ ' payload=' ++ modules::Masking::sanitize(vars.rawBody)]"/>
          </on-error-continue>
        </error-handler>
      </try>
    </on-error-continue>

    <!-- 2) Everything else (poison message, bad JSON, transient retries exhausted) => DEAD-LETTER with reason + original payload -->
    <on-error-continue type="ANY" logException="false">
      <logger level="ERROR" category="${app.logCategory}.consumer" message="#['DEAD_LETTER eventId=' ++ (vars.eventId default 'n/a') ++ ' type=' ++ error.errorType.identifier ++ ' corrId=' ++ (vars.corrId default 'n/a')]"/>
      <try doc:name="Publish DLQ">
        <ee:transform doc:name="DLQ message">
          <ee:variables>
            <ee:set-variable variableName="dlqMsg"><![CDATA[%dw 2.0
import modules::Masking
output application/json
---
{
  reason: error.errorType.namespace ++ ":" ++ error.errorType.identifier ++ " - " ++ Masking::sanitize(error.description),
  failedAt: now(),
  correlationId: vars.corrId default correlationId,
  eventId: vars.eventId default null,
  sourceQueue: vars.sourceQueue,
  transientRetries: vars.envelope.transientRetries default 0,
  originalPayload: Masking::sanitize(vars.rawBody)
}]]></ee:set-variable>
          </ee:variables>
        </ee:transform>
        <vm:publish config-ref="VM_Config" queueName="${vm.queue.dlq}" timeout="5" timeoutUnit="SECONDS" doc:name="Publish to DLQ">
          <vm:content>#[vars.dlqMsg]</vm:content>
        </vm:publish>
        <error-handler>
          <on-error-continue logException="false">
            <logger level="ERROR" category="${app.logCategory}.consumer" message="#['DLQ_PUBLISH_FAILED eventId=' ++ (vars.eventId default 'n/a') ++ ' corrId=' ++ (vars.corrId default 'n/a') ++ ' payload=' ++ modules::Masking::sanitize(vars.rawBody)]"/>
          </on-error-continue>
        </error-handler>
      </try>
    </on-error-continue>
  </error-handler>
</mule>
```

### J2. Persistence - `src/main/mule/persistence.xml`
**What:** inside **one DB transaction** it upserts the `fraud_events` row, deletes any old rule hits for that event, then inserts the new rule-hit rows. The whole thing is wrapped in `until-successful` (retries transient DB errors).
**Why this shape:** if a message is re-queued and processed twice, you still get exactly one parent row and the right child rows - no duplicates. The card is stored masked.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- Idempotent + atomic: parent upsert, child delete+insert in ONE DB transaction. Redelivery can never duplicate rows. -->
  <sub-flow name="persist-fraud-event-sf">
    <ee:transform doc:name="Build DB params">
      <ee:variables>
        <ee:set-variable variableName="dbEvent"><![CDATA[%dw 2.0
import modules::Masking
output application/java
var e = vars.envelope.event
---
{
  eventId: e.eventId,
  transactionId: e.transaction.transactionId,
  eventTimestamp: ((e.eventTimestamp as DateTime) >> "UTC") as String { format: "yyyy-MM-dd HH:mm:ss" },
  customerId: e.customer.customerId,
  cardToken: e.card.cardToken,
  maskedCard: Masking::maskCard(e.card.maskedCardNumber),
  amount: e.transaction.amount,
  currency: e.transaction.currency,
  merchantId: e.merchant.merchantId,
  merchantCountry: e.merchant.merchantCountry,
  geoCountry: e.device.geoCountry,
  riskScore: vars.final.riskScore,
  riskBand: vars.final.riskBand,
  decision: vars.final.decision,
  alertStatus: if (vars.alertRequired) "PENDING" else "NOT_REQUIRED",
  correlationId: vars.corrId
}]]></ee:set-variable>
        <ee:set-variable variableName="dbHits"><![CDATA[%dw 2.0
output application/java
---
vars.final.ruleHits map (h) -> { eventId: vars.envelope.event.eventId, ruleCode: h.ruleCode, ruleName: h.ruleName, ruleWeight: h.ruleWeight }]]></ee:set-variable>
      </ee:variables>
    </ee:transform>

    <until-successful maxRetries="${retry.db.max}" millisBetweenRetries="${retry.db.delayMs}" doc:name="Retry transient DB errors">
      <try transactionalAction="ALWAYS_BEGIN" doc:name="DB transaction">
        <db:insert config-ref="Database_Config" doc:name="Upsert fraud_events">
          <db:sql><![CDATA[INSERT INTO fraud_events
 (event_id, transaction_id, event_timestamp, customer_id, card_token, masked_card, amount, currency,
  merchant_id, merchant_country, geo_country, risk_score, risk_band, decision, alert_status, correlation_id)
 VALUES (:eventId, :transactionId, :eventTimestamp, :customerId, :cardToken, :maskedCard, :amount, :currency,
  :merchantId, :merchantCountry, :geoCountry, :riskScore, :riskBand, :decision, :alertStatus, :correlationId)
 ON DUPLICATE KEY UPDATE risk_score = VALUES(risk_score), risk_band = VALUES(risk_band), decision = VALUES(decision)]]></db:sql>
          <db:input-parameters>#[vars.dbEvent]</db:input-parameters>
        </db:insert>
        <db:delete config-ref="Database_Config" doc:name="Clear old hits (redelivery safe)">
          <db:sql>DELETE FROM fraud_rule_hits WHERE event_id = :eventId</db:sql>
          <db:input-parameters>#[{ eventId: vars.dbEvent.eventId }]</db:input-parameters>
        </db:delete>
        <choice>
          <when expression="#[sizeOf(vars.dbHits) &gt; 0]">
            <db:bulk-insert config-ref="Database_Config" doc:name="Insert rule hits">
              <db:bulk-input-parameters>#[vars.dbHits]</db:bulk-input-parameters>
              <db:sql>INSERT INTO fraud_rule_hits (event_id, rule_code, rule_name, rule_weight) VALUES (:eventId, :ruleCode, :ruleName, :ruleWeight)</db:sql>
            </db:bulk-insert>
          </when>
        </choice>
      </try>
    </until-successful>
    <logger level="INFO" category="${app.logCategory}.persist" message="#['PERSISTED eventId=' ++ vars.eventId ++ ' hits=' ++ sizeOf(vars.dbHits) ++ ' enrichmentIncomplete=' ++ (vars.enrichmentIncomplete default false) ++ ' corrId=' ++ vars.corrId]"/>
  </sub-flow>
</mule>
```

### Milestone 2 - run again
Run the app. Then in your SQL tool (or terminal):
```bash
docker exec -it mysql mysql -uroot -proot -e "USE frauddb; SELECT event_id,risk_score,risk_band,decision,alert_status,masked_card FROM fraud_events; SELECT * FROM fraud_rule_hits;"
```
For the sample you must see 1 event (score 82, HIGH, `masked_card = XXXXXXXXXXXX4417`) and **3 rule hits**: `R01_AMOUNT`, `R02_GEO_MISMATCH`, `R04_MERCHANT_CAT`. `alert_status` will be `PENDING` until Part K is added.

---

## PART K - Alerts, throttling, reconciliation, monitoring (requirements 8, 9, 12)

**Design in plain words**
* `enqueue-alert-sf` (called by the consumer) puts the alert on the internal VM `alertQueue`.
* `alert-dispatch-flow` has **exactly one** consumer thread. Why: the throttle decision (read store -> decide -> write store) must not race with itself.
* For each **card token** the Object Store holds `{windowEnd, suppressed[]}`:
  * no open window -> **send email now** and open a window (`IMMEDIATE`)
  * inside window -> **don't send**, remember the transaction (`SUPPRESS`)
  * window over and something was suppressed -> send **one aggregate email listing every transaction** (`AGGREGATE`)
* `alert-flush-scheduler-flow` pings every 30 s so an aggregate still goes out even if no new event arrives.
* `pending-alert-reconciler-flow`: any HIGH/BLOCK row still `PENDING` for > 1 minute (email server was down, or app crashed) is re-queued. **Nothing flagged goes unreported** (negative scenario 8).
* `high-band-spike-monitor-flow`: emails the ops list if too many HIGH events occur in a few minutes (requirement 12).

Create `src/main/mule/alerting.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- Hand-off from consumer to the alert pipeline. DB row already says alert_status=PENDING, so a crash here is recovered by the reconciler. -->
  <sub-flow name="enqueue-alert-sf">
    <ee:transform doc:name="Build alert message">
      <ee:variables>
        <ee:set-variable variableName="alertMsg"><![CDATA[%dw 2.0
import modules::Masking
output application/json
var e = vars.envelope.event
---
{
  "type": "ALERT",
  eventId: e.eventId,
  transactionId: e.transaction.transactionId,
  eventTimestamp: e.eventTimestamp,
  amount: e.transaction.amount,
  currency: e.transaction.currency,
  maskedCard: Masking::maskCard(e.card.maskedCardNumber),
  cardToken: e.card.cardToken,
  cardType: e.card.cardType,
  customerId: e.customer.customerId,
  customerName: e.customer.name,
  merchantName: e.merchant.merchantName,
  merchantCountry: e.merchant.merchantCountry,
  riskScore: vars.final.riskScore,
  riskBand: vars.final.riskBand,
  decision: vars.final.decision,
  ruleHits: vars.final.ruleHits,
  correlationId: vars.corrId
}]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <vm:publish config-ref="VM_Config" queueName="alertQueue" doc:name="Publish alert">
      <vm:content>#[vars.alertMsg]</vm:content>
    </vm:publish>
  </sub-flow>

  <!-- ONE consumer thread => throttle decisions are serialised (no race on the throttle store). Run with 1 worker. -->
  <flow name="alert-dispatch-flow">
    <vm:listener config-ref="VM_Config" queueName="alertQueue" numberOfConsumers="1" doc:name="Alert listener"/>
    <ee:transform doc:name="Parse alert msg">
      <ee:variables>
        <ee:set-variable variableName="alert"><![CDATA[%dw 2.0
output application/java
---
read(payload as String, "application/json")]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <set-variable variableName="corrId" value="#[vars.alert.correlationId default correlationId]"/>
    <choice>
      <when expression="#[vars.alert.&quot;type&quot; == 'FLUSH']"><flow-ref name="alert-flush-sf"/></when>
      <otherwise><flow-ref name="alert-process-sf"/></otherwise>
    </choice>
    <error-handler>
      <!-- never lose an alert: failures leave alert_status=PENDING and the reconciler re-queues it -->
      <on-error-continue logException="false">
        <logger level="ERROR" category="${app.logCategory}.alert" message="#['ALERT_PIPELINE_ERROR type=' ++ error.errorType.identifier ++ ' corrId=' ++ (vars.corrId default 'n/a')]"/>
      </on-error-continue>
    </error-handler>
  </flow>

  <!-- ===== Throttle + send. Three outcomes: IMMEDIATE | SUPPRESS | AGGREGATE ===== -->
  <sub-flow name="alert-process-sf">
    <os:retrieve key="#[vars.alert.cardToken]" objectStore="alertThrottleStore" target="entry" doc:name="Get throttle entry">
      <os:default-value>#[null]</os:default-value>
    </os:retrieve>
    <set-variable variableName="alertAction" doc:name="Decide action"
      value="#[if (vars.entry != null and vars.entry.windowEnd &gt; (now() as Number)) 'SUPPRESS' else if (vars.entry != null and sizeOf(vars.entry.suppressed default []) &gt; 0) 'AGGREGATE' else 'IMMEDIATE']"/>
    <ee:transform doc:name="Summary of this txn">
      <ee:variables>
        <ee:set-variable variableName="summary"><![CDATA[%dw 2.0
output application/java
---
{
  eventId: vars.alert.eventId, transactionId: vars.alert.transactionId, eventTimestamp: vars.alert.eventTimestamp,
  amount: vars.alert.amount, currency: vars.alert.currency, merchantName: vars.alert.merchantName,
  riskScore: vars.alert.riskScore, ruleCodes: vars.alert.ruleHits.ruleCode
}]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <logger level="INFO" category="${app.logCategory}.alert" message="#['ALERT_DECISION action=' ++ vars.alertAction ++ ' eventId=' ++ vars.alert.eventId ++ ' corrId=' ++ vars.corrId]"/>

    <choice>
      <!-- 1) inside window: suppress, remember the txn for the aggregate -->
      <when expression="#[vars.alertAction == 'SUPPRESS']">
        <os:store key="#[vars.alert.cardToken]" objectStore="alertThrottleStore" failIfPresent="false" doc:name="Append suppressed txn">
          <os:value><![CDATA[#[{ windowEnd: vars.entry.windowEnd, maskedCard: vars.alert.maskedCard, suppressed: ((vars.entry.suppressed default []) ++ [vars.summary]) }]]]></os:value>
        </os:store>
        <set-variable variableName="statusIds" value="#[[vars.alert.eventId]]"/>
        <set-variable variableName="newStatus" value="SUPPRESSED"/>
        <flow-ref name="update-alert-status-sf"/>
      </when>

      <!-- 2) window ended with suppressed txns: send ONE aggregate listing every txn (incl. this one) -->
      <when expression="#[vars.alertAction == 'AGGREGATE']">
        <set-variable variableName="aggKey" value="#[vars.alert.cardToken]"/>
        <set-variable variableName="aggMasked" value="#[vars.alert.maskedCard]"/>
        <set-variable variableName="aggItems" value="#[((vars.entry.suppressed default []) ++ [vars.summary])]"/>
        <flow-ref name="send-aggregate-sf"/>
      </when>

      <!-- 3) first alert in window: send immediately, open window -->
      <otherwise>
        <try doc:name="Send single alert">
          <until-successful maxRetries="${retry.smtp.max}" millisBetweenRetries="${retry.smtp.delayMs}">
            <email:send config-ref="SMTP_Config" fromAddress="${smtp.from}" subject="#[modules::AlertTemplates::subject(vars.alert)]" doc:name="Send email">
              <email:to-addresses><email:to-address value="${alert.distributionList}"/></email:to-addresses>
              <email:body contentType="text/plain; charset=UTF-8"><email:content><![CDATA[#[modules::AlertTemplates::body(vars.alert)]]]></email:content></email:body>
            </email:send>
          </until-successful>
          <os:store key="#[vars.alert.cardToken]" objectStore="alertThrottleStore" failIfPresent="false" doc:name="Open window">
            <os:value><![CDATA[#[{ windowEnd: (now() as Number) + (p('alert.throttle.windowSeconds') as Number), maskedCard: vars.alert.maskedCard, suppressed: [] }]]]></os:value>
          </os:store>
          <set-variable variableName="statusIds" value="#[[vars.alert.eventId]]"/>
          <set-variable variableName="newStatus" value="SENT"/>
          <flow-ref name="update-alert-status-sf"/>
          <logger level="INFO" category="${app.logCategory}.alert" message="#['ALERT_SENT eventId=' ++ vars.alert.eventId ++ ' corrId=' ++ vars.corrId]"/>
          <error-handler>
            <!-- SMTP down after retries: keep alert_status=PENDING; reconciler will try again. Nothing is silently dropped. -->
            <on-error-continue logException="false">
              <logger level="ERROR" category="${app.logCategory}.alert" message="#['ALERT_EMAIL_FAILED eventId=' ++ vars.alert.eventId ++ ' status=PENDING type=' ++ error.errorType.identifier ++ ' corrId=' ++ vars.corrId]"/>
            </on-error-continue>
          </error-handler>
        </try>
      </otherwise>
    </choice>
  </sub-flow>

  <sub-flow name="send-aggregate-sf">
    <try doc:name="Send aggregate">
      <until-successful maxRetries="${retry.smtp.max}" millisBetweenRetries="${retry.smtp.delayMs}">
        <email:send config-ref="SMTP_Config" fromAddress="${smtp.from}" subject="#[modules::AlertTemplates::aggregateSubject(vars.aggMasked, sizeOf(vars.aggItems))]" doc:name="Send aggregate email">
          <email:to-addresses><email:to-address value="${alert.distributionList}"/></email:to-addresses>
          <email:body contentType="text/plain; charset=UTF-8"><email:content><![CDATA[#[modules::AlertTemplates::aggregateBody(vars.aggMasked, vars.aggItems)]]]></email:content></email:body>
        </email:send>
      </until-successful>
      <os:remove key="#[vars.aggKey]" objectStore="alertThrottleStore" doc:name="Close window"/>
      <set-variable variableName="statusIds" value="#[vars.aggItems.eventId]"/>
      <set-variable variableName="newStatus" value="AGGREGATED_SENT"/>
      <flow-ref name="update-alert-status-sf"/>
      <logger level="INFO" category="${app.logCategory}.alert" message="#['AGGREGATE_ALERT_SENT card=' ++ vars.aggMasked ++ ' txns=' ++ sizeOf(vars.aggItems)]"/>
      <error-handler>
        <!-- entry is kept, so the next flush tick retries; nothing lost -->
        <on-error-continue logException="false">
          <logger level="ERROR" category="${app.logCategory}.alert" message="#['AGGREGATE_EMAIL_FAILED card=' ++ vars.aggMasked ++ ' will retry on next flush type=' ++ error.errorType.identifier]"/>
        </on-error-continue>
      </error-handler>
    </try>
  </sub-flow>

  <sub-flow name="update-alert-status-sf">
    <until-successful maxRetries="${retry.db.max}" millisBetweenRetries="${retry.db.delayMs}">
      <db:bulk-update config-ref="Database_Config" doc:name="Update alert_status">
        <db:bulk-input-parameters>#[vars.statusIds map (id) -> { eventId: id, status: vars.newStatus }]</db:bulk-input-parameters>
        <db:sql>UPDATE fraud_events SET alert_status = :status WHERE event_id = :eventId</db:sql>
      </db:bulk-update>
    </until-successful>
  </sub-flow>

  <!-- ===== FLUSH: scheduler asks the serialised alert flow to close expired windows ===== -->
  <flow name="alert-flush-scheduler-flow">
    <scheduler doc:name="Every N sec"><scheduling-strategy><fixed-frequency frequency="${alert.flushFrequencyMs}"/></scheduling-strategy></scheduler>
    <vm:publish config-ref="VM_Config" queueName="alertQueue"><vm:content>#[output application/json --- { "type": "FLUSH" }]</vm:content></vm:publish>
  </flow>

  <sub-flow name="alert-flush-sf">
    <os:retrieve-all-keys objectStore="alertThrottleStore" target="throttleKeys" doc:name="All keys"/>
    <foreach collection="#[vars.throttleKeys]">
      <try>
        <set-variable variableName="aggKey" value="#[payload]"/>
        <os:retrieve key="#[vars.aggKey]" objectStore="alertThrottleStore" target="entry"><os:default-value>#[null]</os:default-value></os:retrieve>
        <choice>
          <when expression="#[vars.entry != null and vars.entry.windowEnd &lt;= (now() as Number) and sizeOf(vars.entry.suppressed default []) &gt; 0]">
            <set-variable variableName="aggMasked" value="#[vars.entry.maskedCard]"/>
            <set-variable variableName="aggItems" value="#[vars.entry.suppressed]"/>
            <flow-ref name="send-aggregate-sf"/>
          </when>
          <when expression="#[vars.entry != null and vars.entry.windowEnd &lt;= (now() as Number)]">
            <os:remove key="#[vars.aggKey]" objectStore="alertThrottleStore"/>
          </when>
        </choice>
        <error-handler>
          <on-error-continue logException="false">
            <logger level="WARN" category="${app.logCategory}.alert" message="#['FLUSH_ITEM_FAILED type=' ++ error.errorType.identifier]"/>
          </on-error-continue>
        </error-handler>
      </try>
    </foreach>
  </sub-flow>

  <!-- ===== RECONCILER: any HIGH/BLOCK event still PENDING after 1 min is re-queued (at-least-once) ===== -->
  <flow name="pending-alert-reconciler-flow">
    <scheduler doc:name="Every N sec"><scheduling-strategy><fixed-frequency frequency="${alert.reconcileFrequencyMs}"/></scheduling-strategy></scheduler>
    <db:select config-ref="Database_Config" doc:name="Pending alerts">
      <db:sql>SELECT event_id, transaction_id, event_timestamp, customer_id, card_token, masked_card, amount, currency, merchant_id, merchant_country, risk_score, risk_band, decision, correlation_id
               FROM fraud_events WHERE alert_status = 'PENDING' AND created_at &lt; (NOW() - INTERVAL 1 MINUTE) ORDER BY created_at LIMIT 50</db:sql>
    </db:select>
    <foreach>
      <set-variable variableName="row" value="#[payload]"/>
      <db:select config-ref="Database_Config" target="hitRows">
        <db:sql>SELECT rule_code, rule_name, rule_weight FROM fraud_rule_hits WHERE event_id = :id</db:sql>
        <db:input-parameters>#[{ id: vars.row.event_id }]</db:input-parameters>
      </db:select>
      <ee:transform doc:name="Rebuild alert">
        <ee:variables>
          <ee:set-variable variableName="alertMsg"><![CDATA[%dw 2.0
output application/json
---
{
  "type": "ALERT", eventId: vars.row.event_id, transactionId: vars.row.transaction_id, eventTimestamp: vars.row.event_timestamp as String,
  amount: vars.row.amount, currency: vars.row.currency, maskedCard: vars.row.masked_card, cardToken: vars.row.card_token,
  customerId: vars.row.customer_id, merchantName: vars.row.merchant_id, merchantCountry: vars.row.merchant_country,
  riskScore: vars.row.risk_score, riskBand: vars.row.risk_band, decision: vars.row.decision,
  ruleHits: vars.hitRows map (h) -> { ruleCode: h.rule_code, ruleName: h.rule_name, ruleWeight: h.rule_weight },
  correlationId: vars.row.correlation_id
}]]></ee:set-variable>
        </ee:variables>
      </ee:transform>
      <vm:publish config-ref="VM_Config" queueName="alertQueue"><vm:content>#[vars.alertMsg]</vm:content></vm:publish>
    </foreach>
  </flow>

  <!-- ===== MONITOR: sudden spike in HIGH band events (DLQ monitor lives in dead-letter.xml) ===== -->
  <flow name="high-band-spike-monitor-flow">
    <scheduler doc:name="Every minute"><scheduling-strategy><fixed-frequency frequency="60000"/></scheduling-strategy></scheduler>
    <db:select config-ref="Database_Config" target="spikeRows">
      <db:sql>SELECT COUNT(*) AS cnt FROM fraud_events WHERE risk_band = 'HIGH' AND created_at &gt;= (NOW() - INTERVAL ${monitor.spikeWindowMinutes} MINUTE)</db:sql>
    </db:select>
    <os:contains key="spike-cooldown" objectStore="spikeStore" target="cooling"/>
    <choice>
      <when expression="#[(vars.spikeRows[0].cnt as Number) &gt; (p('monitor.highSpikeThreshold') as Number) and not vars.cooling]">
        <logger level="ERROR" category="${app.logCategory}.monitor" message="#['HIGH_BAND_SPIKE count=' ++ vars.spikeRows[0].cnt ++ ' window=${monitor.spikeWindowMinutes}min']"/>
        <email:send config-ref="SMTP_Config" fromAddress="${smtp.from}" subject="#['[MONITOR] Spike in HIGH-risk payments: ' ++ vars.spikeRows[0].cnt ++ ' in last ${monitor.spikeWindowMinutes} min']">
          <email:to-addresses><email:to-address value="${monitor.opsList}"/></email:to-addresses>
          <email:body contentType="text/plain; charset=UTF-8"><email:content>Possible coordinated attack. Check the fraud review console.</email:content></email:body>
        </email:send>
        <os:store key="spike-cooldown" objectStore="spikeStore" failIfPresent="false"><os:value>#[true]</os:value></os:store>
      </when>
    </choice>
  </flow>
</mule>
```

### K2. Dead-letter writer and DLQ monitor - `src/main/mule/dead-letter.xml`
**Why:** without Anypoint MQ there is no web page to browse a DLQ. So a listener drains `payments-dlq` into the table `dead_letter_events` (reason, payload, time) where you can query it, and a monitor emails ops when OPEN dead letters exist. This replaces "Anypoint Monitoring alert on DLQ depth". If the DB is down, the dead letter is put back on the DLQ - never discarded.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <!-- ===== DEAD-LETTER WRITER: drains payments-dlq into the dead_letter_events table so every failed event has a recorded reason ===== -->
  <flow name="dead-letter-writer-flow">
    <vm:listener config-ref="VM_Config" queueName="${vm.queue.dlq}" numberOfConsumers="1" transactionalAction="INDIFFERENT" doc:name="Listener DLQ"/>
    <set-variable variableName="rawDlq" value="#[payload as String]" doc:name="rawDlq"/>
    <ee:transform doc:name="Build DLQ row">
      <ee:variables>
        <ee:set-variable variableName="dlRow"><![CDATA[%dw 2.0
output application/java
var d = read(vars.rawDlq, "application/json")
---
{
  eventId: d.eventId,
  correlationId: d.correlationId,
  sourceQueue: d.sourceQueue,
  reason: ((d.reason default "unknown") as String)[0 to 998],
  retries: d.transientRetries default 0,
  payload: d.originalPayload,
  failedAt: ((d.failedAt as DateTime) >> "UTC") as String { format: "yyyy-MM-dd HH:mm:ss" }
}]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
    <until-successful maxRetries="${retry.db.max}" millisBetweenRetries="${retry.db.delayMs}" doc:name="Retry DB">
      <db:insert config-ref="Database_Config" doc:name="Insert dead_letter_events">
        <db:sql>INSERT INTO dead_letter_events (event_id, correlation_id, source_queue, reason, transient_retries, original_payload, failed_at)
                VALUES (:eventId, :correlationId, :sourceQueue, :reason, :retries, :payload, :failedAt)</db:sql>
        <db:input-parameters>#[vars.dlRow]</db:input-parameters>
      </db:insert>
    </until-successful>
    <logger level="WARN" category="${app.logCategory}.dlq" message="#['DLQ_RECORDED eventId=' ++ (vars.dlRow.eventId default 'n/a') ++ ' corrId=' ++ (vars.dlRow.correlationId default 'n/a')]"/>
    <error-handler>
      <!-- DB down: put the dead letter back on the DLQ after a pause. A dead letter is never discarded. -->
      <on-error-continue logException="false">
        <logger level="ERROR" category="${app.logCategory}.dlq" message="#['DLQ_WRITE_FAILED type=' ++ error.errorType.identifier ++ ' -> requeue to DLQ']"/>
        <flow-ref name="transient-backoff-sf"/>
        <vm:publish config-ref="VM_Config" queueName="${vm.queue.dlq}" timeout="5" timeoutUnit="SECONDS">
          <vm:content>#[vars.rawDlq]</vm:content>
        </vm:publish>
      </on-error-continue>
    </error-handler>
  </flow>

  <!-- ===== DLQ DEPTH MONITOR: replaces the Anypoint MQ "queue depth" alert. Emails ops when OPEN dead letters exceed the threshold ===== -->
  <flow name="dead-letter-monitor-flow">
    <scheduler doc:name="Every minute"><scheduling-strategy><fixed-frequency frequency="60000"/></scheduling-strategy></scheduler>
    <db:select config-ref="Database_Config" target="dlqRows" doc:name="Count OPEN dead letters">
      <db:sql>SELECT COUNT(*) AS cnt FROM dead_letter_events WHERE status = 'OPEN'</db:sql>
    </db:select>
    <os:contains key="dlq-cooldown" objectStore="spikeStore" target="dlqCooling"/>
    <choice>
      <when expression="#[(vars.dlqRows[0].cnt as Number) &gt; (p('monitor.dlqThreshold') as Number) and not vars.dlqCooling]">
        <logger level="ERROR" category="${app.logCategory}.monitor" message="#['DLQ_DEPTH_ALERT open=' ++ vars.dlqRows[0].cnt]"/>
        <email:send config-ref="SMTP_Config" fromAddress="${smtp.from}" subject="#['[MONITOR] Dead-letter events waiting: ' ++ vars.dlqRows[0].cnt]">
          <email:to-addresses><email:to-address value="${monitor.opsList}"/></email:to-addresses>
          <email:body contentType="text/plain; charset=UTF-8"><email:content>Payment events could not be processed. See table dead_letter_events (status = OPEN) for the recorded reason of each.</email:content></email:body>
        </email:send>
        <os:store key="dlq-cooldown" objectStore="spikeStore" failIfPresent="false"><os:value>#[true]</os:value></os:store>
      </when>
    </choice>
    <error-handler>
      <on-error-continue logException="false">
        <logger level="WARN" category="${app.logCategory}.monitor" message="#['DLQ_MONITOR_FAILED type=' ++ error.errorType.identifier]"/>
      </on-error-continue>
    </error-handler>
  </flow>
</mule>
```

### Milestone 3 - run and check email
Run the app, send the sample request. Open http://localhost:8025: one email, subject `HIGH RISK Payment – TXN-99120455 – Score 82`, card `XXXXXXXXXXXX4417`, three rule lines. In MySQL `alert_status` becomes `SENT`.

---

## PART L - Check the whole app (test every scenario)

Optional helper (a deliberately broken message to test the DLQ). Rename `test-tools.xml.off` -> `test-tools.xml` (content below), run, call it, then **delete the file before demo/deploy**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!-- TEST ONLY. Rename to test-tools.xml to enable, delete before the demo/deploy.
     POST http://localhost:8081/test/poison drops a broken message on the standard queue to prove the dead-letter path. -->
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd">
  <flow name="test-poison-flow">
    <http:listener config-ref="HTTP_Listener_config" path="/test/poison" doc:name="Listener"/>
    <vm:publish config-ref="VM_Config" queueName="${vm.queue.standard}" doc:name="Publish junk">
      <vm:content>#["this is { not json"]</vm:content>
    </vm:publish>
    <set-payload value="poison message published" doc:name="Reply"/>
  </flow>
</mule>
```

Test script `scripts/curl-tests.sh` (works in Git Bash / WSL / macOS; on Windows you can send the same bodies from Postman):

```bash
#!/usr/bin/env bash
# Usage: ./curl-tests.sh http://localhost:8081
B=${1:-http://localhost:8081}; U=$B/api/payments/events
good='{"eventId":"PAY-EVT-2026-884512","eventTimestamp":"2026-09-11T14:22:07+05:30","transaction":{"transactionId":"TXN-99120455","amount":248500.00,"currency":"INR","channel":"ONLINE","entryMode":"CARD_NOT_PRESENT"},"card":{"cardToken":"TKN-4d9a77e1c0","maskedCardNumber":"XXXXXXXXXXXX4417","cardType":"CREDIT","issuingCountry":"IN"},"customer":{"customerId":"CUST-77219","name":"Meena Raghavan","email":"meena.raghavan@example.com","riskProfile":"STANDARD"},"merchant":{"merchantId":"MER-5581","merchantName":"Global Gadgets Online","merchantCategory":"ELECTRONICS","merchantCountry":"SG"},"device":{"ipAddress":"203.0.113.77","deviceId":"DEV-ax91kk","geoCountry":"SG"}}'
H='Content-Type: application/json'
echo "1) happy path -> 202 HIGH";           curl -s -i -X POST $U -H "$H" -d "$good"; echo
echo "2) duplicate -> 409";                 curl -s -i -X POST $U -H "$H" -d "$good"; echo
echo "3) negative amount -> 400";           curl -s -X POST $U -H "$H" -d "${good//248500.00/-5}"; echo
echo "4) bad currency -> 400";              curl -s -X POST $U -H "$H" -d "${good//\"INR\"/\"XYZ\"}"; echo
echo "5) malformed JSON -> 400";            curl -s -X POST $U -H "$H" -d '{bad json'; echo
echo "6) FULL PAN in masked field -> 400, value must NOT be echoed"; curl -s -X POST $U -H "$H" -d "${good//XXXXXXXXXXXX4417/4111111111114417}"; echo
echo "7) missing eventId -> 400";           curl -s -X POST $U -H "$H" -d "${good//\"eventId\":\"PAY-EVT-2026-884512\",/}"; echo
echo "8) burst on one card (alert flooding: expect 1 immediate + 1 aggregate mail)"
for i in $(seq 1 8); do curl -s -o /dev/null -w "%{http_code} " -X POST $U -H "$H" -d "${good//PAY-EVT-2026-884512/BURST-$i}"; done; echo
```

| # | Test | Expected |
|---|---|---|
| 1 | Sample body | 202, score 82, HIGH; 1 row in `fraud_events`, 3 in `fraud_rule_hits`; 1 email in MailHog |
| 2 | Same body again | 409; no new row, no new email |
| 3 | Negative amount / bad currency / missing eventId / bad JSON | 400 with `fields` listing **paths only** |
| 4 | Full 16-digit number in `maskedCardNumber` | 400; response **and logs** contain no card digits (acceptance #6) |
| 5 | Burst of 8 on one card (set `alert.throttle.windowSeconds: "20"` and `alert.flushFrequencyMs: "5000"` in config to see it fast) | 1 immediate email, then ONE aggregate listing the rest |
| 6 | **DB outage:** `docker stop mysql`, send 3 events, wait, `docker start mysql` | requests still return 202; logs show `TRANSIENT_FAILURE_REQUEUE`; after restart rows appear (acceptance #7) |
| 7 | **SMTP outage:** `docker stop mailhog`, send HIGH event | log `ALERT_EMAIL_FAILED`, `alert_status=PENDING`; `docker start mailhog` -> reconciler sends it within ~1-2 min |
| 8 | Poison message: `POST http://localhost:8081/test/poison` | log `DEAD_LETTER`, then a row in `dead_letter_events` with a reason; ops email from the monitor |
| 9 | Unknown customer id | log `PROFILE_NOT_FOUND`, event still saved with default profile |
| 10 | Scoring failure (MUnit covers this) | decision REVIEW with rule `SYS_SCORING_FAILURE` |

---

## PART M - Sharing the HTTP listener: Domain project (hackathon rule 12)
1. **File -> New -> Mule Domain Project**, name `payments-domain`.
2. Move the `HTTP_Listener_config` element into the domain's XML.
3. In your app: right-click project -> **Properties -> Mule Domain** (or Project Properties) -> select `payments-domain`.
*Why:* a domain lets several apps share one HTTP port. It works in Studio/on-prem only; CloudHub ignores domains, so before deploying to CloudHub put the listener config back into the app.

---

## PART N - MUnit testing (requirement 13, acceptance #8)

### N1. What MUnit is
MUnit is MuleSoft's unit-test framework. A test (1) sets up inputs, (2) **mocks** anything external (database, SMTP, queue) so no Docker is needed, (3) runs a flow / sub-flow / DataWeave function, (4) **asserts** the result. The brief requires: each scoring rule in isolation, the banding boundaries and the alert throttling, with DB and SMTP mocked.

### N2. One-time setup in Studio
1. Right-click any flow on the canvas -> **MUnit -> Create blank test for this flow**. Studio adds the MUnit dependencies/plugin to `pom.xml` and creates `src/test/munit`. Delete the blank test it generated.
2. Give MUnit the secure key (the app decrypts `secure-dev.yaml` at startup). Either:
   * in `pom.xml`, inside the `munit-maven-plugin` `<configuration>`, add `<systemPropertyVariables><secure.key>mySecretKey</secure.key></systemPropertyVariables>`, **or**
   * Run -> Run Configurations -> MUnit -> your suite -> Arguments -> VM arguments: `-M-Dsecure.key=mySecretKey`.
3. Tests use your normal `config-dev.yaml`; nothing else to configure.

### N3. Anatomy of a test (so you can write more)
| Block | Purpose |
|---|---|
| `munit:before-test` | runs before every test in the file (sample data + mocks) |
| `munit:behavior` | set up THIS test: mock `db:select` to throw `DB:CONNECTIVITY`, set payload |
| `munit:execution` | run the thing under test (`flow-ref`, or a DataWeave call) |
| `munit:validation` | `assert-that` (check a value) and `verify-call` (check a mocked processor was called N times) |
| `mock-when` | replaces a real processor (`db:select`, `email:send`, `vm:publish`, `os:retrieve`) with a fake result or fake error |

### N4. What our three suites prove
| Suite | Proves | Brief item |
|---|---|---|
| `fraud-rules-test-suite.xml` | R01-R05 each in isolation (incl. "exactly at limit does NOT fire"); band 29=LOW, 30/59=MEDIUM, 60/100=HIGH; decisions APPROVE/REVIEW/BLOCK; scoring failure -> REVIEW; masking; sample payload = 82 HIGH | Req 13, Neg 5, Neg 10, Acc 2, Acc 8 |
| `alert-throttle-test-suite.xml` | first alert sent immediately; repeat in window suppressed (0 emails); window end sends ONE aggregate listing every txn (SMTP/OS/DB mocked) | Req 9, 13, Neg 9, Acc 5 |
| `consumer-reliability-test-suite.xml` | DB down -> event re-queued with retry counter (not lost); poison message -> DLQ with reason | Req 11, Neg 7, Neg 12, Acc 7 |

Create three files in `src/test/munit/`:

`fraud-rules-test-suite.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd">

  <munit:config name="fraud-rules-test-suite.xml"/>

  <!-- fixtures: config + a LOW-risk baseline event that triggers NO rule -->
  <munit:before-test name="fixtures">
    <ee:transform>
      <ee:variables>
        <ee:set-variable variableName="cfg"><![CDATA[%dw 2.0
output application/json
---
{ amountLimit: 100000, velocityMax: 5, highRiskCategories: ["ELECTRONICS","CRYPTO","GAMBLING"],
  usualHourStart: 6, usualHourEnd: 23, wAmount: 35, wGeo: 30, wVelocity: 20, wMerchant: 17, wHour: 15, wWatchlist: 10,
  mediumMin: 30, highMin: 60, blockMin: 85, failSafeScore: 45 }]]></ee:set-variable>
        <ee:set-variable variableName="evt"><![CDATA[%dw 2.0
output application/json
---
{ eventId: "T-1", eventTimestamp: "2026-09-11T14:22:07+05:30",
  transaction: { transactionId: "TX-1", amount: 1500, currency: "INR", channel: "ONLINE" },
  card: { cardToken: "TKN-1", maskedCardNumber: "XXXXXXXXXXXX4417", issuingCountry: "IN", cardType: "CREDIT" },
  customer: { customerId: "C-1", name: "Test User" },
  merchant: { merchantId: "M-1", merchantName: "Grocer", merchantCategory: "GROCERY", merchantCountry: "IN" },
  device: { geoCountry: "IN" } }]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
  </munit:before-test>

  <!-- ============ R01 amount ============ -->
  <munit:test name="rule-R01-amount-in-isolation" description="Amount rule fires only ABOVE the limit">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ over: RiskRules::ruleAmount(vars.evt update { case a at .transaction.amount -> 100000.01 }, vars.cfg),
  atLimit: RiskRules::ruleAmount(vars.evt update { case a at .transaction.amount -> 100000 }, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.over.ruleCode]" is="#[MunitTools::equalTo('R01_AMOUNT')]"/>
      <munit-tools:assert-that expression="#[payload.atLimit]" is="#[MunitTools::nullValue()]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ R02 geo ============ -->
  <munit:test name="rule-R02-geo-mismatch-in-isolation" description="Fires when merchant/geo country differs from issuing country">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ merchantAbroad: RiskRules::ruleGeo(vars.evt update { case m at .merchant.merchantCountry -> "SG" }, vars.cfg),
  deviceAbroad: RiskRules::ruleGeo(vars.evt update { case g at .device.geoCountry -> "US" }, vars.cfg),
  domestic: RiskRules::ruleGeo(vars.evt, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.merchantAbroad.ruleCode]" is="#[MunitTools::equalTo('R02_GEO_MISMATCH')]"/>
      <munit-tools:assert-that expression="#[payload.deviceAbroad.ruleCode]" is="#[MunitTools::equalTo('R02_GEO_MISMATCH')]"/>
      <munit-tools:assert-that expression="#[payload.domestic]" is="#[MunitTools::nullValue()]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ R03 velocity ============ -->
  <munit:test name="rule-R03-velocity-in-isolation" description="Fires when count is MORE than the max">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ over: RiskRules::ruleVelocity({ velocityCount: 6 }, vars.cfg),
  atMax: RiskRules::ruleVelocity({ velocityCount: 5 }, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.over.ruleCode]" is="#[MunitTools::equalTo('R03_VELOCITY')]"/>
      <munit-tools:assert-that expression="#[payload.atMax]" is="#[MunitTools::nullValue()]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ R04 merchant category ============ -->
  <munit:test name="rule-R04-merchant-category-in-isolation" description="High risk MCC (case-insensitive) fires; grocery does not">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ crypto: RiskRules::ruleMerchant(vars.evt update { case c at .merchant.merchantCategory -> "crypto" }, vars.cfg),
  grocery: RiskRules::ruleMerchant(vars.evt, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.crypto.ruleCode]" is="#[MunitTools::equalTo('R04_MERCHANT_CAT')]"/>
      <munit-tools:assert-that expression="#[payload.grocery]" is="#[MunitTools::nullValue()]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ R05 odd hour ============ -->
  <munit:test name="rule-R05-odd-hour-in-isolation" description="03:xx and 00:xx fire, 14:xx and 23:xx (boundary) do not">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ at3am: RiskRules::ruleHour(vars.evt update { case t at .eventTimestamp -> "2026-09-11T03:10:00+05:30" }, {}, vars.cfg),
  atMidnight: RiskRules::ruleHour(vars.evt update { case t at .eventTimestamp -> "2026-09-11T00:00:00+05:30" }, {}, vars.cfg),
  at2pm: RiskRules::ruleHour(vars.evt, {}, vars.cfg),
  at2359: RiskRules::ruleHour(vars.evt update { case t at .eventTimestamp -> "2026-09-11T23:59:00+05:30" }, {}, vars.cfg),
  customRange: RiskRules::ruleHour(vars.evt, { profile: { usualHourStart: 18, usualHourEnd: 22 } }, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.at3am.ruleCode]" is="#[MunitTools::equalTo('R05_ODD_HOUR')]"/>
      <munit-tools:assert-that expression="#[payload.atMidnight.ruleCode]" is="#[MunitTools::equalTo('R05_ODD_HOUR')]"/>
      <munit-tools:assert-that expression="#[payload.at2pm]" is="#[MunitTools::nullValue()]"/>
      <munit-tools:assert-that expression="#[payload.at2359]" is="#[MunitTools::nullValue()]"/>
      <munit-tools:assert-that expression="#[payload.customRange.ruleCode]" is="#[MunitTools::equalTo('R05_ODD_HOUR')]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ band boundaries: one test each ============ -->
  <munit:test name="band-29-is-LOW"><munit:execution>
    <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ b: RiskRules::band(29, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform></munit:execution>
    <munit:validation><munit-tools:assert-that expression="#[payload.b]" is="#[MunitTools::equalTo('LOW')]"/></munit:validation></munit:test>

  <munit:test name="band-30-is-MEDIUM"><munit:execution>
    <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ b: RiskRules::band(30, vars.cfg), top: RiskRules::band(59, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.b]" is="#[MunitTools::equalTo('MEDIUM')]"/>
      <munit-tools:assert-that expression="#[payload.top]" is="#[MunitTools::equalTo('MEDIUM')]"/>
    </munit:validation></munit:test>

  <munit:test name="band-60-is-HIGH"><munit:execution>
    <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ b: RiskRules::band(60, vars.cfg), max: RiskRules::band(100, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.b]" is="#[MunitTools::equalTo('HIGH')]"/>
      <munit-tools:assert-that expression="#[payload.max]" is="#[MunitTools::equalTo('HIGH')]"/>
    </munit:validation></munit:test>

  <munit:test name="decision-boundaries"><munit:execution>
    <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
{ low: RiskRules::decide(10, "LOW", false, vars.cfg), high: RiskRules::decide(84, "HIGH", false, vars.cfg),
  block: RiskRules::decide(85, "HIGH", false, vars.cfg), forced: RiskRules::decide(10, "LOW", true, vars.cfg) }]]></ee:set-payload></ee:message></ee:transform></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.low]" is="#[MunitTools::equalTo('APPROVE')]"/>
      <munit-tools:assert-that expression="#[payload.high]" is="#[MunitTools::equalTo('REVIEW')]"/>
      <munit-tools:assert-that expression="#[payload.block]" is="#[MunitTools::equalTo('BLOCK')]"/>
      <munit-tools:assert-that expression="#[payload.forced]" is="#[MunitTools::equalTo('REVIEW')]"/>
    </munit:validation></munit:test>

  <!-- ============ acceptance criteria #2: amount + geography => HIGH. Sample payload => 82 ============ -->
  <munit:test name="sample-event-scores-82-HIGH" description="Sample from the brief: amount + geo + electronics = 82 => HIGH">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
var e = vars.evt update { case a at .transaction.amount -> 248500 case m at .merchant.merchantCountry -> "SG" case g at .device.geoCountry -> "SG" case c at .merchant.merchantCategory -> "ELECTRONICS" }
var r = RiskRules::evaluate(e, { velocityCount: 1 }, vars.cfg)
---
{ score: r.score, band: RiskRules::band(r.score, vars.cfg), codes: r.hits.ruleCode }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.score]" is="#[MunitTools::equalTo(82)]"/>
      <munit-tools:assert-that expression="#[payload.band]" is="#[MunitTools::equalTo('HIGH')]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ negative scenario 5: scoring failure fails SAFE to REVIEW ============ -->
  <munit:test name="scoring-failure-fails-safe-to-review">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::RiskRules
output application/json
---
RiskRules::finalize({ event: (vars.evt update { case t at .eventTimestamp -> "not-a-date" }), scoring: { riskScore: 45, ruleHits: [], scoringFailed: false } }, null, vars.cfg)]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.decision]" is="#[MunitTools::equalTo('REVIEW')]"/>
      <munit-tools:assert-that expression="#[payload.ruleHits.ruleCode]" is="#[MunitTools::hasItem('SYS_SCORING_FAILURE')]"/>
    </munit:validation>
  </munit:test>

  <!-- ============ negative scenario 10: data protection ============ -->
  <munit:test name="masking-and-sanitising">
    <munit:execution>
      <ee:transform><ee:message><ee:set-payload><![CDATA[%dw 2.0
import modules::Masking
output application/json
---
{ masked: Masking::maskCard("4111111111114417"), already: Masking::maskCard("XXXXXXXXXXXX4417"),
  clean: Masking::sanitize("failed for card 4111 1111 1111 4417 at db") }]]></ee:set-payload></ee:message></ee:transform>
    </munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[payload.masked]" is="#[MunitTools::equalTo('XXXXXXXXXXXX4417')]"/>
      <munit-tools:assert-that expression="#[payload.already]" is="#[MunitTools::equalTo('XXXXXXXXXXXX4417')]"/>
      <munit-tools:assert-that expression="#[payload.clean]" is="#[MunitTools::not(MunitTools::containsString('4111'))]"/>
    </munit:validation>
  </munit:test>
</mule>
```

`alert-throttle-test-suite.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd">

  <munit:config name="alert-throttle-test-suite.xml"/>

  <munit:before-test name="mocks-and-alert">
    <munit-tools:mock-when processor="email:send"><munit-tools:then-return><munit-tools:payload value="#['sent']"/></munit-tools:then-return></munit-tools:mock-when>
    <munit-tools:mock-when processor="os:store"><munit-tools:then-return><munit-tools:payload value="#['ok']"/></munit-tools:then-return></munit-tools:mock-when>
    <munit-tools:mock-when processor="os:remove"><munit-tools:then-return><munit-tools:payload value="#['ok']"/></munit-tools:then-return></munit-tools:mock-when>
    <munit-tools:mock-when processor="db:bulk-update"><munit-tools:then-return><munit-tools:payload value="#[[]]"/></munit-tools:then-return></munit-tools:mock-when>
    <ee:transform>
      <ee:variables>
        <ee:set-variable variableName="alert"><![CDATA[%dw 2.0
output application/java
---
{ eventId: "E-3", transactionId: "TX-3", eventTimestamp: "2026-09-11T14:22:07+05:30", amount: 248500, currency: "INR",
  maskedCard: "XXXXXXXXXXXX4417", cardToken: "TKN-1", cardType: "CREDIT", customerId: "C-1", customerName: "Test User",
  merchantName: "Global Gadgets", merchantCountry: "SG", riskScore: 82, riskBand: "HIGH", decision: "REVIEW",
  ruleHits: [{ ruleCode: "R01_AMOUNT", ruleName: "Amount", ruleWeight: 35 }], correlationId: "corr-1" }]]></ee:set-variable>
        <ee:set-variable variableName="corrId"><![CDATA[%dw 2.0
output application/java
---
"corr-1"]]></ee:set-variable>
      </ee:variables>
    </ee:transform>
  </munit:before-test>

  <munit:test name="throttle-first-alert-is-sent-immediately">
    <munit:behavior>
      <munit-tools:mock-when processor="os:retrieve"><munit-tools:then-return>
        <munit-tools:payload value="#[{ windowEnd: 1, maskedCard: 'XXXXXXXXXXXX4417', suppressed: [] }]"/></munit-tools:then-return></munit-tools:mock-when>
    </munit:behavior>
    <munit:execution><flow-ref name="alert-process-sf"/></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[vars.alertAction]" is="#[MunitTools::equalTo('IMMEDIATE')]"/>
      <munit-tools:verify-call processor="email:send" times="1"/>
      <munit-tools:verify-call processor="os:store" times="1"/>
    </munit:validation>
  </munit:test>

  <munit:test name="throttle-repeat-inside-window-is-suppressed-no-email">
    <munit:behavior>
      <munit-tools:mock-when processor="os:retrieve"><munit-tools:then-return>
        <munit-tools:payload value="#[{ windowEnd: 99999999999, maskedCard: 'XXXXXXXXXXXX4417', suppressed: [] }]"/></munit-tools:then-return></munit-tools:mock-when>
    </munit:behavior>
    <munit:execution><flow-ref name="alert-process-sf"/></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[vars.alertAction]" is="#[MunitTools::equalTo('SUPPRESS')]"/>
      <munit-tools:verify-call processor="email:send" times="0"/>
      <munit-tools:verify-call processor="os:store" times="1"/>
    </munit:validation>
  </munit:test>

  <munit:test name="throttle-window-end-sends-ONE-aggregate-listing-every-txn">
    <munit:behavior>
      <munit-tools:mock-when processor="os:retrieve"><munit-tools:then-return>
        <munit-tools:payload value="#[{ windowEnd: 1, maskedCard: 'XXXXXXXXXXXX4417', suppressed: [{ eventId: 'E-1', transactionId: 'TX-1', amount: 1, currency: 'INR', riskScore: 70, ruleCodes: ['R01'] }, { eventId: 'E-2', transactionId: 'TX-2', amount: 2, currency: 'INR', riskScore: 71, ruleCodes: ['R02'] }] }]"/></munit-tools:then-return></munit-tools:mock-when>
    </munit:behavior>
    <munit:execution><flow-ref name="alert-process-sf"/></munit:execution>
    <munit:validation>
      <munit-tools:assert-that expression="#[vars.alertAction]" is="#[MunitTools::equalTo('AGGREGATE')]"/>
      <munit-tools:assert-that expression="#[sizeOf(vars.aggItems)]" is="#[MunitTools::equalTo(3)]"/>
      <munit-tools:verify-call processor="email:send" times="1"/>
      <munit-tools:verify-call processor="os:remove" times="1"/>
    </munit:validation>
  </munit:test>
</mule>
```

`consumer-reliability-test-suite.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:doc="http://www.mulesoft.org/schema/mule/documentation"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:apikit="http://www.mulesoft.org/schema/mule/mule-apikit"
      xmlns:os="http://www.mulesoft.org/schema/mule/os"
      xmlns:db="http://www.mulesoft.org/schema/mule/db"
      xmlns:email="http://www.mulesoft.org/schema/mule/email"
      xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
      xmlns:secure-properties="http://www.mulesoft.org/schema/mule/secure-properties"
      xmlns:api-gateway="http://www.mulesoft.org/schema/mule/api-gateway"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/mule-apikit http://www.mulesoft.org/schema/mule/mule-apikit/current/mule-apikit.xsd
        http://www.mulesoft.org/schema/mule/os http://www.mulesoft.org/schema/mule/os/current/mule-os.xsd
        http://www.mulesoft.org/schema/mule/db http://www.mulesoft.org/schema/mule/db/current/mule-db.xsd
        http://www.mulesoft.org/schema/mule/email http://www.mulesoft.org/schema/mule/email/current/mule-email.xsd
        http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/current/mule-vm.xsd
        http://www.mulesoft.org/schema/mule/secure-properties http://www.mulesoft.org/schema/mule/secure-properties/current/mule-secure-properties.xsd
        http://www.mulesoft.org/schema/mule/api-gateway http://www.mulesoft.org/schema/mule/api-gateway/current/mule-api-gateway.xsd">

  <munit:config name="consumer-reliability-test-suite.xml"/>

  <!-- Acceptance #7 in miniature: DB is down -> the event goes BACK on its queue (never lost, never marked done) -->
  <munit:test name="db-down-requeues-event-never-loses-it">
    <munit:behavior>
      <munit-tools:mock-when processor="db:select"><munit-tools:then-return><munit-tools:error typeId="DB:CONNECTIVITY"/></munit-tools:then-return></munit-tools:mock-when>
      <munit-tools:mock-when processor="vm:publish"><munit-tools:then-return><munit-tools:payload value="#['ok']"/></munit-tools:then-return></munit-tools:mock-when>
      <ee:transform>
        <ee:message><ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{ correlationId: "c-1", velocityCount: 1, scoring: { riskScore: 82, riskBand: "HIGH", ruleHits: [], scoringFailed: false },
  event: { eventId: "T-9", eventTimestamp: "2026-09-11T14:22:07+05:30",
    transaction: { transactionId: "TX-9", amount: 248500, currency: "INR" },
    card: { cardToken: "TKN-1", maskedCardNumber: "XXXXXXXXXXXX4417", issuingCountry: "IN" },
    customer: { customerId: "C-1" }, merchant: { merchantId: "M-1", merchantCategory: "ELECTRONICS", merchantCountry: "SG" }, device: { geoCountry: "SG" } } }]]></ee:set-payload></ee:message>
      </ee:transform>
    </munit:behavior>
    <munit:execution><flow-ref name="scoring-consumer-high-flow"/></munit:execution>
    <munit:validation>
      <munit-tools:verify-call processor="vm:publish" times="1"/>
      <munit-tools:assert-that expression="#[vars.requeueMsg.transientRetries]" is="#[MunitTools::equalTo(1)]"/>
    </munit:validation>
  </munit:test>

  <!-- Negative scenario: poison message (not JSON) -> dead-letter queue with a reason, nothing crashes -->
  <munit:test name="poison-message-goes-to-dlq-with-reason">
    <munit:behavior>
      <munit-tools:mock-when processor="vm:publish"><munit-tools:then-return><munit-tools:payload value="#['ok']"/></munit-tools:then-return></munit-tools:mock-when>
      <set-payload value="this is { not json"/>
    </munit:behavior>
    <munit:execution><flow-ref name="scoring-consumer-standard-flow"/></munit:execution>
    <munit:validation>
      <munit-tools:verify-call processor="vm:publish" times="1"/>
      <munit-tools:assert-that expression="#[vars.dlqMsg.reason]" is="#[MunitTools::notNullValue()]"/>
    </munit:validation>
  </munit:test>
</mule>
```

### N5. Run the tests
1. Right-click `src/test/munit` -> **Run MUnit Suite** (or right-click one file to run just that suite).
2. The **MUnit** tab opens: green = pass, red = fail. Click a red test to see the failed assertion and the actual value.
3. When the run ends, click **Show Coverage Report** in the MUnit view to see which flows/processors were covered.
4. Command line alternative: `mvn clean test` (report in `target/site/munit/coverage/summary.html`).

**Expected result:** all tests green. (The consumer DB-down test takes ~10 s because of the intentional back-off wait.)

### N6. About 100% coverage
The brief says: if 100% is impossible give valid reasons. Realistic gaps: schedulers (`*-scheduler-flow`, monitors), autodiscovery, the HTTP listener / APIkit router wiring. These reasons are written in `docs/HLD.md`. To raise coverage, copy an existing test and change the mock or input (e.g. a test where `email:send` throws `EMAIL:CONNECTIVITY` and `alert_status` stays PENDING).

---

## PART O - Git (hackathon rule 14)
```bash
cd <your studio workspace>/payment-fraud-detection
git init
git checkout -b main
```
Create `.gitignore` (content below), then:
```bash
git add .
git commit -m "feat: initial fraud detection app (VM queues)"
git remote add origin <your GitHub/GitLab repo URL>
git push -u origin main
git checkout -b feature/alert-throttle     # work on branches
git push -u origin feature/alert-throttle  # then open a Merge/Pull Request
```
Team practice: each member clones the repo, creates a branch, commits, pulls (`git pull --rebase origin main`), opens a merge request, and **practise resolving one merge conflict** (edit the same line of `config-dev.yaml` on two branches) so you can show it.

`.gitignore`:
```
target/
.mule/
.studio/
*.jar
.DS_Store
*.key
```
Never commit the secure key or real passwords.

---

## PART P - Deploy to Anypoint CloudHub (optional but scores higher)
You do **not** need Anypoint MQ for this. You need a free **Anypoint trial account**.

1. **DB and SMTP must be reachable from the internet** (CloudHub cannot see `localhost`). Use a free cloud MySQL (Aiven / Railway / AWS RDS free tier) and a real SMTP (Mailtrap or Gmail app password). Update `db.host` etc. in a new `config-prod.yaml`, and change the email connection to `<email:smtps-connection>` if the provider needs TLS. Run `schema.sql` on that cloud DB.
2. **Publish to Exchange:** right-click project -> **Anypoint Platform -> Publish to Exchange** (sign in).
3. **API Manager** (anypoint.mulesoft.com): Add API -> *Mule Gateway* -> pick your Exchange asset -> create. Copy the **API ID** into `api.id`.
4. Rename `autodiscovery.xml.off` back to `autodiscovery.xml`.
5. **Deploy:** right-click project -> **Anypoint Platform -> Deploy to CloudHub**. Settings:
   * Application name unique, e.g. `payment-fraud-<yourname>`
   * Worker: smallest, **1 worker** (the alert throttle needs a single consumer)
   * Tick **Persistent queues** (this keeps VM queue messages if the worker restarts) and **Object Store v2**
   * Properties: `env=prod` (or keep `dev`), `secure.key=<your key>`, `http.port=8081`, `anypoint.platform.client_id=<id>`, `anypoint.platform.client_secret=<secret>` (from Anypoint -> Access Management -> Business Groups/Environments -> client credentials)
6. In API Manager the API status must become **Active** (this proves autodiscovery, acceptance #9).
7. **Policies** (API Manager -> Policies -> Apply): Client ID Enforcement, Rate Limiting, IP Allowlist (optional), JSON Threat Protection. (Turn message logging policies OFF, they could log card data.)
8. **Monitoring** (Anypoint Monitoring -> Alerts): alert on application error count / CPU. The "DLQ depth" and "HIGH spike" alerts are already built into the app (`dead-letter-monitor-flow` and `high-band-spike-monitor-flow`).
9. If the cloud step fails, run locally in Studio - allowed but scored lower.

---

## PART Q - Documentation and demo

**High Level Design:** the file `docs/HLD.md` is ready; read it, adjust names, and add a picture (draw the Part 0.2 diagram in draw.io).

**3-minute demo script**
1. Architecture slide (HLD): API-led, two queues, DB, throttled email, DLQ.
2. Happy path: Postman 202 -> logs -> DB rows -> MailHog email.
3. Reliability: `docker stop mysql` -> send events (still 202) -> `docker start mysql` -> rows appear. "Nothing is lost: re-queue then DLQ with reason."
4. Safety: send a full card number -> 400, logs clean.
5. Flooding: burst -> one email + one aggregate.
6. MUnit green with coverage; API Manager Active + policies (if deployed).
7. Be upfront: "VM queues because Anypoint MQ was unavailable; production would use a broker."

## PART R - Basic negative scenarios (from the brief) - how to trigger and what you must see
Use the sample JSON from the brief as the "good" body; change only what is stated. `curl-tests.sh` automates 1, 2, 9 and 10.

| # | Scenario (brief) | How to trigger | Expected result | Implemented in |
|---|---|---|---|---|
| 1 | Invalid / incomplete event | (a) send `{bad json`; (b) remove `eventId`; (c) remove `transactionId`, `amount`, `currency` or `cardToken` one at a time; (d) `"amount": -5`; (e) `"currency": "XYZ"` | **400**, JSON with `errorCode: INVALID_PAYMENT_EVENT`, `correlationId`, and `fields` = JSON paths only. Nothing queued, nothing in DB | RAML + `payment-event.json`, `APIKIT:BAD_REQUEST` handler in `global-config.xml` |
| 2 | Duplicate event | send the same body twice | 2nd call **409** `DUPLICATE_EVENT`; still 1 DB row, 1 email | `dedupe-event-sf` (Object Store `failIfPresent`) |
| 3 | Queue unavailable / publish failure | stop consumers: rename `scoring-consumer.xml` to `.off`; set `vm.maxOutstanding: "1"` in config; restart; send 3 different events | once queues are full: **503** `SERVICE_UNAVAILABLE` ("NOT accepted"), log `DEPENDENCY_UNAVAILABLE eventId=...`; publish was retried `retry.queue.max` times; the dedupe key is released so the caller can retry | `queue-publish-sf` (`until-successful`), 503 handler |
| 4 | Priority misrouting | same setup as 3 with HIGH events: 1st fills the high queue, the 2nd is HIGH again | log `ROUTING ... queue=payments-high-q`, then `PRIORITY_FALLBACK ... diverted to standard queue`; consumer logs `PRIORITY_MISROUTE` if it ever sees HIGH on the standard queue. HIGH is never silent | `publish-scored-event-sf`, `process-scored-event-sf` |
| 5 | Scoring rule failure | MUnit `scoring-failure-fails-safe-to-review` (bad timestamp) | decision **REVIEW**, rule `SYS_SCORING_FAILURE`; never APPROVE | `RiskRules::finalize`, `score-event-sf` |
| 6 | Customer not found | send `"customerId": "CUST-99999"` | log `PROFILE_NOT_FOUND ... enrichment incomplete`; event still saved using default profile | `enrich-customer-profile-sf` |
| 7 | DB connectivity / timeout | `docker stop mysql`; send 3 events; wait ~30 s; `docker start mysql` | requests still get 202; logs `TRANSIENT_FAILURE_REQUEUE`; after restart rows appear; no loss | `consumerErrorHandler`, MUnit `db-down-requeues...` |
| 8 | Email delivery failure | `docker stop mailhog`; send a HIGH event | log `ALERT_EMAIL_FAILED`; `alert_status = PENDING`; `docker start mailhog` -> reconciler sends it within ~1-2 min, status `SENT` | `alert-process-sf`, `pending-alert-reconciler-flow` |
| 9 | Alert flooding | set `alert.throttle.windowSeconds: "20"`, `alert.flushFrequencyMs: "5000"`; run burst of 8 on one card (curl script test 8) | ONE immediate email, then ONE aggregate email listing every other transaction; never 8 emails | throttle store, `send-aggregate-sf`, MUnit throttle suite |
| 10 | Sensitive data exposure | (a) send full 16-digit number in `maskedCardNumber`; (b) search logs: `grep -E "[0-9]{13,}" logs/*.log`; (c) check DB `masked_card`; (d) read the email | (a) **400**, value not echoed; (b) no matches; (c) `XXXXXXXXXXXX4417`; (d) masked only | schema pattern, `Masking.dwl`, `log4j2.xml` |
| 11 | Unexpected processing error | `GET /api/does-not-exist` (404 body) and, for a true 500, temporarily set a property to a bad value (e.g. `risk.amountLimit: "abc"`) and send an event | consistent JSON `{errorCode, message, correlationId}`; **no** stack trace or internals; same id in logs and `X-Correlation-ID` header | `globalErrorHandler` |
| 12 | Additional (ours) | (a) `POST /test/poison`; (b) stop MySQL while a dead letter is written; (c) enqueue-alert fails | (a) log `DEAD_LETTER` then row in `dead_letter_events` with reason + masked payload; (b) `DLQ_WRITE_FAILED` then re-queued, never discarded; (c) `ALERT_ENQUEUE_FAILED`, reconciler recovers | `consumerErrorHandler`, `dead-letter.xml`, `scoring-consumer.xml` |

## PART S - Acceptance criteria checklist (tick each one for your demo)

| # | Acceptance criterion | How to verify | Pass when |
|---|---|---|---|
| 1 | Valid event accepted, scored, queued; response has score, band, correlation ID | POST the sample body | **202** with `riskScore: 82`, `riskBand: HIGH`, `status: QUEUED_FOR_REVIEW`, `correlationId` present |
| 2 | Amount + geography breach => HIGH and priority queue | sample body (amount 248500 > limit, SG vs IN) ; check log | log `ROUTING ... band=HIGH queue=payments-high-q`; MUnit `sample-event-scores-82-HIGH` green |
| 3 | Every scored event in `fraud_events`, one row per triggered rule in `fraud_rule_hits` | `SELECT * FROM fraud_events; SELECT * FROM fraud_rule_hits;` | 1 event row; 3 hit rows (R01_AMOUNT, R02_GEO_MISMATCH, R04_MERCHANT_CAT); also send a LOW event, it appears with 0 hits |
| 4 | HIGH event => alert email within agreed time listing all triggered rules | open http://localhost:8025 | email within seconds, subject `HIGH RISK Payment – TXN-99120455 – Score 82`, three rule bullets |
| 5 | Repeated suspicious transactions on one card => single aggregated alert | burst test (PART R #9) | 1 immediate + 1 aggregate email listing every transaction |
| 6 | No unmasked card number anywhere (logs, DB, API responses, emails) | PART R #10 | none found; masked as `XXXXXXXXXXXX4417` |
| 7 | With DB stopped, messages are redelivered and processed once it returns | PART R #7 | rows appear after `docker start mysql`; logs show `TRANSIENT_FAILURE_REQUEUE` then `PERSISTED` |
| 8 | MUnit proves each rule and each band boundary independently with mocks | Run MUnit suite (PART N5) | all green, coverage report generated |
| 9 | Deployed on Anypoint runtime with autodiscovery and security policies | PART P | API Manager status **Active**, policies listed (Client ID Enforcement, Rate Limiting, JSON Threat Protection) |

## Troubleshooting
| Symptom | Fix |
|---|---|
| Red "unknown namespace/element" in XML | a module is missing: Mule Palette -> Add Modules (HTTP, APIkit, Object Store, Database, Email, VM, Secure Properties, API Gateway) |
| `Could not resolve placeholder 'secure.key'` or decrypt error | VM argument `-M-Dsecure.key=mySecretKey` missing, or the value was encrypted with a different key/algorithm (use Blowfish + CBC both times) |
| `No suitable driver` / `Driver class not found` | mysql-connector-j dependency or `sharedLibraries` missing; Maven -> Update Project |
| `Access denied for user 'fraud_app'` | encrypted DB password does not match `ChangeMe!123`, or schema.sql was not run |
| `Table ... doesn't exist` | schema.sql not run in database `frauddb` |
| App fails: autodiscovery / `api.id` | rename `autodiscovery.xml` to `.off` for local runs |
| `Unknown error type ...` on start | an error `type="..."` is not valid in your connector version; delete that name from the list, keep the rest (`ANY` always works) |
| `vm:listener ... transactionalAction` attribute rejected | delete that attribute from the listener (and from `dead-letter.xml`); if the DB step then complains about a transaction, set the DB `try` in `persistence.xml` to `transactionalAction="ALWAYS_BEGIN"` (already so) |
| Port 8081 in use | change `http.port` |
| 500 on every call | read the log line `UNEXPECTED_ERROR type=...` |
| Email not arriving | MailHog running? `smtp.host=localhost`, `smtp.port=1025`; if the SMTP server rejects auth, remove `user`/`password` from `<email:smtp-connection>` in `global-config.xml` |
| No alert for HIGH event | check `alert_status`; `PENDING` older than 1 min = SMTP issue, reconciler retries |
| Messages from before restart reappear | expected: queues are persistent. To wipe, stop the app and delete the project's `.mule` folder |
