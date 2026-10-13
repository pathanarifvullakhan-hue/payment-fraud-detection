#!/usr/bin/env bash
# Usage: ./curl-tests.sh http://localhost:8081
B=${1:-http://localhost:8081}; U=$B/api/payments/events
good='{"eventId":"PAY-EVT-2026-900002","eventTimestamp":"2026-09-11T14:22:07+05:30","transaction":{"transactionId":"TXN-99120455","amount":248500.00,"currency":"INR","channel":"ONLINE","entryMode":"CARD_NOT_PRESENT"},"card":{"cardToken":"TKN-4d9a77e1c0","maskedCardNumber":"XXXXXXXXXXXX4417","cardType":"CREDIT","issuingCountry":"IN"},"customer":{"customerId":"CUST-77219","name":"Meena Raghavan","email":"meena.raghavan@example.com","riskProfile":"STANDARD"},"merchant":{"merchantId":"MER-5581","merchantName":"Global Gadgets Online","merchantCategory":"ELECTRONICS","merchantCountry":"SG"},"device":{"ipAddress":"203.0.113.77","deviceId":"DEV-ax91kk","geoCountry":"SG"}}'
H='Content-Type: application/json'
echo "1) happy path -> 202 HIGH";           curl -s -i -X POST $U -H "$H" -d "$good"; echo
echo "2) duplicate -> 409";                 curl -s -i -X POST $U -H "$H" -d "$good"; echo
echo "3) negative amount -> 400";           curl -s -X POST $U -H "$H" -d "${good//248500.00/-5}"; echo
echo "4) bad currency -> 400";              curl -s -X POST $U -H "$H" -d "${good//\"INR\"/\"XYZ\"}"; echo
echo "5) malformed JSON -> 400";            curl -s -X POST $U -H "$H" -d '{bad json'; echo
echo "6) FULL PAN in masked field -> 400, value must NOT be echoed"; curl -s -X POST $U -H "$H" -d "${good//XXXXXXXXXXXX4417/4111111111114417}"; echo
echo "7) missing eventId -> 400";           curl -s -X POST $U -H "$H" -d "${good//\"eventId\":\"PAY-EVT-2026-900002\",/}"; echo
echo "8) burst on one card (alert flooding: expect 1 immediate + 1 aggregate mail)"
for i in $(seq 1 8); do curl -s -o /dev/null -w "%{http_code} " -X POST $U -H "$H" -d "${good//PAY-EVT-2026-900002/B2-$i}"; done; echo
