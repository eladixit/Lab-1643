-- Fraud Detection Query(INTEGER timestamps)
-- Detects impossible travel patterns and writes to results topic
-- 
-- This query runs continuously and processes all transactions in real-time
-- 
-- Detection Logic:
-- - Same card used in two locations within 2 hours
-- - Distance > 300 km
-- - Required travel speed > 800 km/h (impossible)

INSERT INTO `bank.fraud.transactions` (
  key,
  alertMessage,
  cardHash,
  detectedAt,
  distanceKm,
  firstLocation,
  firstTransactionId,
  secondLocation,
  secondTransactionId,
  speedKmh,
  timeMinutes
)
SELECT
  CAST(a.cardHash AS BYTES) AS key,
  CONCAT(
    'FRAUD ALERT: Card used in ',
    a.merchantCity, ', ', a.merchantCountry,
    ' and then ',
    b.merchantCity, ', ', b.merchantCountry,
    ' within ',
    CAST(TIMESTAMPDIFF(SECOND, a.event_ts, b.event_ts) / 60.0 AS VARCHAR),
    ' minutes. Impossible travel detected.'
  ) AS alertMessage,
  a.cardHash,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS detectedAt,
  CAST(
    111 * SQRT(
      POWER(b.merchantLat - a.merchantLat, 2) +
      POWER(b.merchantLon - a.merchantLon, 2)
    ) AS DOUBLE
  ) AS distanceKm,
  CONCAT(a.merchantCity, ', ', a.merchantCountry) AS firstLocation,
  a.transactionId AS firstTransactionId,
  CONCAT(b.merchantCity, ', ', b.merchantCountry) AS secondLocation,
  b.transactionId AS secondTransactionId,
  CAST(
    (
      111 * SQRT(
        POWER(b.merchantLat - a.merchantLat, 2) +
        POWER(b.merchantLon - a.merchantLon, 2)
      )
    ) / (TIMESTAMPDIFF(SECOND, a.event_ts, b.event_ts) / 3600.0) AS DOUBLE
  ) AS speedKmh,
  CAST(TIMESTAMPDIFF(SECOND, a.event_ts, b.event_ts) / 60.0 AS DOUBLE) AS timeMinutes
FROM `bank.transactions` a
JOIN `bank.transactions` b
  ON a.cardHash = b.cardHash
 AND b.event_ts > a.event_ts
 AND b.event_ts <= a.event_ts + INTERVAL '2' HOUR
WHERE a.authResult = 'APPROVED'
  AND b.authResult = 'APPROVED'
  AND a.channel = 'POS'
  AND b.channel = 'POS'
  AND a.cardPresent = TRUE
  AND b.cardPresent = TRUE
  AND TIMESTAMPDIFF(SECOND, a.event_ts, b.event_ts) > 0
  -- Fraud detection thresholds
  AND (
    111 * SQRT(
      POWER(b.merchantLat - a.merchantLat, 2) +
      POWER(b.merchantLon - a.merchantLon, 2)
    )
  ) > 300  -- Distance threshold: 300 km
  AND (
    (
      111 * SQRT(
        POWER(b.merchantLat - a.merchantLat, 2) +
        POWER(b.merchantLon - a.merchantLon, 2)
      )
    ) / (TIMESTAMPDIFF(SECOND, a.event_ts, b.event_ts) / 3600.0)
  ) > 800;  -- Speed threshold: 800 km/h

-- This query will:
-- 1. Run continuously (24/7)
-- 2. Process all new transactions automatically
-- 3. Detect impossible travel patterns in real-time
-- 4. Write fraud alerts to bank.fraud.transactions
--
-- To stop: Go to Flink → Running statements → Stop this statement
-- To monitor: Check Flink metrics and logs in Confluent Cloud UI

-- Made with Bob