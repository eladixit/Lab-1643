INSERT INTO `bank.fraud.transactions`
SELECT
  b.secondTransactionId AS transactionId,
  a.cardHash,
  CONCAT(
    'FRAUD ALERT: Card used in ',
    a.merchantCity, ', ', a.merchantCountry,
    ' and then ',
    b.merchantCity, ', ', b.merchantCountry,
    '. Impossible travel detected.'
  ) AS alertMessage,
  CONCAT(a.merchantCity, ', ', a.merchantCountry) AS firstLocation,
  CONCAT(b.merchantCity, ', ', b.merchantCountry) AS secondLocation,
  111 * SQRT(POWER(b.merchantLat - a.merchantLat, 2) + POWER(b.merchantLon - a.merchantLon, 2)) AS distanceKm,
  (b.eventTime - a.eventTime) / 60000.0 AS timeMinutes,
  (111 * SQRT(POWER(b.merchantLat - a.merchantLat, 2) + POWER(b.merchantLon - a.merchantLon, 2))) / ((b.eventTime - a.eventTime) / 3600000.0) AS speedKmh,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS detectedAt,
  a.transactionId AS firstTransactionId,
  b.secondTransactionId
FROM (
  SELECT transactionId, cardHash, authResult, channel, cardPresent,
         eventTime, merchantCity, merchantCountry, merchantLat, merchantLon
  FROM `bank.transactions`
  WHERE authResult = 'APPROVED'
    AND channel = 'POS'
    AND cardPresent = 'true'
) a
JOIN (
  SELECT cardHash, authResult, channel, cardPresent,
         eventTime, merchantCity, merchantCountry, merchantLat, merchantLon,
         transactionId AS secondTransactionId
  FROM `bank.transactions`
  WHERE authResult = 'APPROVED'
    AND channel = 'POS'
    AND cardPresent = 'true'
) b
  ON a.cardHash = b.cardHash
 AND b.eventTime > a.eventTime
 AND b.eventTime <= a.eventTime + 7200000
WHERE (
    111 * SQRT(
      POWER(b.merchantLat - a.merchantLat, 2) +
      POWER(b.merchantLon - a.merchantLon, 2)
    )
  ) > 300
  AND (
    (
      111 * SQRT(
        POWER(b.merchantLat - a.merchantLat, 2) +
        POWER(b.merchantLon - a.merchantLon, 2)
      )
    ) / ((b.eventTime - a.eventTime) / 3600000.0)
  ) > 800
