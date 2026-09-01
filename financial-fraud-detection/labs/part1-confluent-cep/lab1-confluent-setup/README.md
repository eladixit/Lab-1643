# Lab 1: Confluent Cloud Setup

## Overview

In Lab 1, you will set up a real-time fraud detection pipeline using Confluent Cloud. This lab walks you through creating a Kafka cluster, configuring topics with schemas, and deploying a Flink SQL query that detects impossible travel patterns in card transactions.

This lab provides a complete walkthrough that results in:

- A Confluent Cloud environment with a running Kafka cluster
- Input and output topics with registered JSON schemas
- A Flink compute pool running a fraud detection query
- API keys for secure access to Kafka and Schema Registry

---

## Learning Objectives

By completing this lab, you will learn how to:

- Create and configure Confluent Cloud environments and Kafka clusters
- Design and register JSON schemas for streaming data
- Set up Flink compute pools for stream processing
- Write Flink SQL queries to detect fraud patterns using self-joins and time windows
- Generate and manage API keys for Kafka and Schema Registry

---

## Prerequisites

Before starting Lab 1, ensure you have:

- A **Confluent Cloud account** - [Sign up for free trial](https://www.confluent.io/confluent-cloud/tryfree/)
- **VS Code** code editor installed
- Basic understanding of SQL (SELECT, JOIN, WHERE clauses)
- Familiarity with JSON data structures

---

## Architecture Overview

This lab extends the Part 1 fraud detection flow with an AI-powered fraud review step:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Confluent Cloud Platform                   │
│                                                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  INPUT: Card Transaction Stream                            │ │
│  │  Topic: bank.transactions                                  │ │
│  │  • Real-time card swipes from POS terminals                │ │
│  │  • ATM withdrawals                                         │ │
│  │  • Online purchases                                        │ │
│  └──────────────────────┬─────────────────────────────────────┘ │
│                         │                                       │
│                         ▼                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  PROCESSING: Flink SQL Fraud Detection Engine              │ │
│  │  • Analyzes transaction patterns in real-time              │ │
│  │  • Detects impossible travel (same card, distant locations)│ │
│  │  • Calculates distance and speed between transactions      │ │
│  │  • Applies business rules (300km, 800km/h thresholds)      │ │
│  └──────────────────────┬─────────────────────────────────────┘ │
│                         │                                       │
│                         ▼                                       │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  OUTPUT: Fraud Alert Stream                                │ │
│  │  Topic: bank.fraud.transactions                            │ │
│  │  • High-confidence fraud alerts                            │ │
│  │  • Detailed context for investigation                      │ │
│  │  • Ready for downstream action systems                     │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📝 Lab Steps

## Step 1: Environment Setup

### 1.1 Create Confluent Cloud Environment

**What you'll do:** Set up a logical workspace for your fraud detection project.

1. Sign in to [Confluent Cloud](https://confluent.cloud)
2. Click **"Environments"** in the left sidebar
3. Click **"Add environment"**
4. Name: `fraud-detection-prod`
5. Click **"Create"**

✅ **Checkpoint:** You should see your new environment `fraud-detection-prod` in the environments list.

---

### 1.2 Create Kafka Cluster

**What you'll do:** Provision a Kafka cluster to host your streaming data.

1. Inside the `fraud-detection-prod` environment, click **"Create cluster"**
2. Select **"Basic"** cluster type (sufficient for learning)
3. Choose your preferred cloud provider and region
4. Name: `fraud-detection-cluster`
5. Click **"Launch cluster"**

⏱️ **Wait Time:** Cluster creation takes 5-10 minutes.

✅ **Checkpoint:** Cluster status shows **"Running"** with a green indicator.

---

### 1.3 Create API Keys

**What you'll do:** Generate authentication credentials for your applications.

##### Kafka API Key

1. Go to your **"Hamburger"** menu on top right corner  → click on **"API keys"**

   ![API keys](./images/api_key.png)

2. Click **"Create key"**

   ![Select Resource Kafka Group for API Key](./images/add_apikey.png)

3. Select **"My Account"** for api key and click **"Next"**

   ![Select Resource Kafka Group for API Key](./images/select_account_for_apikey.png)

4. Select resource scope for API key → **"Kafka Cluster"**

5. Specify Kafka **"Cluster"** and **"Environment"** click **"Next**"

   ![Select Resource Kafka Group for API Key](./images/select_resource_kafka_group_for_apikey.png)

6. Add a name and description to help identify this API key in the future.
   Name & Description: `fraud-detection-app`

7. Click **"Create Api Key"**

   ![Add Name and Description for Kafka Key](./images/add_name_discription_for_kafka_key.png)

8. **⚠️ IMPORTANT:** Download and save the key and secret (you can't retrieve them later)

##### Schema Registry API Key

1. Go to your **"Hamburger"** menu on top right corner → click on **"API keys"**

2. Click **"Create key"**

3. Select **"My Account"** for api key and click **"Next"**

4. Select resource scope for API key → **"Schema Registry"**

5. Specify **"Environment"** click **"Next"**

   ![Select Resource SR Group for API Key](./images/select_resource_sr_group_for_apikey.png)

6. Add a name and description to help identify this API key in the future.
   Name & Description: `fraud-detection-schema`

7. Click **"Create Api Key"**

   ![Add Name and Description for SR Key](./images/add_name_and_discription_for_sr_key.png)

8. **⚠️ IMPORTANT:** Download and save the key and secret (you can't retrieve them later)

---

### 1.4 Get Connection Endpoints

**What you'll do:** Retrieve the connection URLs needed for your applications.

##### Get Kafka Bootstrap Server

1. Navigate to your **Environment**

2. Go to your **Cluster**

3. Copy the link under **"Bootstrap server"**

   Example: `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`

   ![get_kafka_bootstrap](./images/get_kafka_bootstrap.png)


##### Get Schema Registry URL

1. Navigate to your **Environment**

2. Click on **"Schema Registry"**

3. Copy the link under **"Endpoints"**

   Example: `https://psrc-xxxxx.us-east-1.aws.confluent.cloud`

   ![get_sr_url](./images/get_sr_url.png)

✅ **Checkpoint:** You have saved 6 values:
- Kafka API Key
- Kafka API Secret
- Schema Registry API Key
- Schema Registry API Secret
- Kafka Bootstrap Server URL
- Schema Registry URL

> 💾 **Best Practice:** Store these credentials in a password manager or secure vault. Never commit them to version control.

---

## Step 2: Topic and Schema Setup

### 2.1 Create Transaction Topic and Register Transaction Schema

**What you'll do:** Create a topic to receive card transaction events.

1. Go inside **Environment**

   ![Environment](./images/environment.png)

2. Select  **Cluster**
   
   ![Select Cluster](./images/select_cluster.png)

3. Click on **Topics**
   
   ![Click on Topics](./images/click_on_topics.png)

4. Click on  **"Add Topic"**
   
   ![Add Topic](./images/add_input_topic_4.png)

5. **Topic name:** `bank.transactions`

6. **Partitions:** `1` (sufficient for learning)

7. Click **"Create with defaults"**

   ![Name Topic](./images/name_input_topic.png)

**Topic Configuration:**
- Retention: 7 days (default)
- Cleanup policy: delete (default)


Now let's Create a Data Contract


8. Select **"Add for topic message values"** 

9. Click **"Add Data Contract"**

   ![Add Topic](./images/add_data_contract.png)

10. **Subject:** `bank.transactions-value`

   ![Name Data Contract](./images/name_data_contract_transcation.png)

11. **Schema type:** `JSON`

   ![Select JSON Schema](./images/select_json_schema_11.png)

12. **Schema:** Copy from [`card-present-transaction.schema.json`](../../../confluent-setup/schemas/card-present-transaction.schema.json)

> ⚠️ **Critical:** `eventTime` must be `"type": "integer"` (not string) for Flink compatibility.

13. Click **"Create"**

✅ **Checkpoint:** 

- Topic `bank.transactions` appears in your topics list.

- Schema `bank.transactions-value` shows version 1.


---

### 2.2 Create Output Topic and Register Fraud Alert Schema

**What you'll do:** Create a topic to store fraud alerts.

1. Click **"Create topic"**

2. **Topic name:** `bank.fraud.transactions`

3. **Partitions:** `1`

4. Click **"Create with defaults"**

   ![Add Output Topic](./images/name_output_topic.png)

Now let's Create a Data Contract

5. Select **"Add for topic message values"** 

6. Click **"Add Data Contract"**


7. **Subject:** `bank.fraud.transactions-value`

   ![Name Data Contract](./images/name_data_contract_for_fraud_alert_schema.png)

8. **Schema type:** `JSON`


9. **Schema:** Copy from [`fraud-alert-input.schema.json`](../../../confluent-setup/schemas/fraud-alert-input.schema.json)

10. Click **"Create"**

✅ **Checkpoint:** 

Topic `bank.fraud.transactions` appears in your topics list.

Schema `bank.fraud.transactions-value` shows version 1.

---

### 2.3 Create Agent Response Topic

**What you'll do:** Create a topic to store fraud alerts.

1. Click **"Create topic"**

2. **Topic name:** `agent.fraud.response`

3. **Partitions:** `1`

4. Click **"Create with defaults"**

   ![Add Agent Topic](./images/name_agent_topic.png)

Now let's Create a Data Contract

5. Select **"Add for topic message values"** 

6. Click **"Add Data Contract"**


7. **Subject:** `agent.fraud.response-value`

   ![Name Data Contract](./images/name_data_contract_for_agent_response.png)

8. **Schema type:** `JSON`


9. **Schema:** Copy from [`fraud-alert-agent.schema.json`](../../../confluent-setup/schemas/fraud-alert-agent.schema.json)

10. Click **"Create"**


✅ **Checkpoint:** 

Topic `agent.fraud.response` appears in your topics list.

Schema `agent.fraud.response-value` shows version 1.


---

## Step 3: Flink SQL Setup

### 3.1 Create Flink Compute Pool

**What you'll do:** Provision computing resources for stream processing.

**Note : Confluent will automatically provision the computing resource, but the default is going to be 50 CFUs, for the lab purposes we recommend the following setups to make sure you don't incur high compute cost.

1. In your environment, 

2. Click **"Flink"** 

![click_flink](./images/click_flink.png)

3. Go to **"Compute pools"** tab

![click_compute_pool](./images/click_compute_pool.png)

4. Click **"Create compute pool"**

5. **Name:** `fraud-detection-pool`

6. **Region:** Same as your Kafka cluster

7. **Max CFUs:** `5` (sufficient for learning)

8. Click **"Create"**

⏱️ **Wait Time:** Pool creation takes 3-5 minutes.

✅ **Checkpoint:** Compute pool status shows **"Running"**.

---

### 3.2 Open Flink SQL Workspace

**What you'll do:** Access the SQL editor for writing queries.

1. In your environment, 

2. Click **"Flink"** 

3. Go to **"Compute pools"** tab

4. Select your compute pool: `fraud-detection-pool`

5. Click **"Open SQL workspace"**

You should see:
- SQL editor in the center
- Catalog of topics on the left
- Query history on the right

---

### 3.3 Verify Topic Table

**What you'll do:** Confirm Flink can see your transaction topic.

Run this query in the SQL workspace:

```sql
DESCRIBE `bank.transactions`;
```

**Expected Output:**
```
Column Name       Data Type
---------------------------------
key               VARBINARY
accountId         VARCHAR
amount            DOUBLE
authResult        VARCHAR
cardHash          VARCHAR
cardPresent       BOOLEAN
channel           VARCHAR
currency          VARCHAR
eventTime         BIGINT          ← INTEGER from schema
merchantCity      VARCHAR
merchantCountry   VARCHAR
merchantLat       DOUBLE
merchantLon       DOUBLE
merchantName      VARCHAR
transactionId     VARCHAR
```

> 💡 **Note:** `eventTime` appears as `BIGINT` (Flink's integer type). This is correct!

✅ **Checkpoint:** Table structure matches expected output.

---

### 3.4 Add Event Time Column

**What you'll do:** Convert the timestamp to a format Flink can use for time-based operations.

Run this query:

```sql
ALTER TABLE `bank.transactions`
ADD `event_ts` AS TO_TIMESTAMP_LTZ(`eventTime`, 3);
```

**What this does:**
- Creates a computed column `event_ts`
- Converts `eventTime` (milliseconds) to TIMESTAMP
- `3` means milliseconds precision

**Verify:**
```sql
DESCRIBE `bank.transactions`;
```

You should now see `event_ts` column with type `TIMESTAMP_LTZ(3)`.

✅ **Checkpoint:** `event_ts` column exists in table description.

---

### 3.5 Add Watermark

**What you'll do:** Configure how Flink handles late-arriving events.

**Why watermarks are important:**
Watermarks tell Flink when it's safe to process time-based operations (like our 2-hour time window for fraud detection). They handle the reality that events don't always arrive in perfect time order - network delays, system issues, or clock differences can cause events to arrive late. Without watermarks, Flink wouldn't know when to trigger time-based calculations.

**How it works in our fraud detection:**
- We set a 2-minute watermark tolerance
- If a transaction arrives within 2 minutes of its event time, it's processed normally
- Transactions arriving more than 2 minutes late are dropped
- This ensures our fraud detection runs in near real-time while handling minor delays

Run this query:

```sql
ALTER TABLE `bank.transactions`
MODIFY WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '2' MINUTE;
```

**What this does:**
- Defines watermark on `event_ts` column
- Allows events up to 2 minutes late
- Events more than 2 minutes late are dropped
- Enables time-based joins and windows to work correctly

**Verify:**
```sql
SHOW CREATE TABLE `bank.transactions`;
```

You should see the watermark definition in the output.

✅ **Checkpoint:** Watermark appears in table definition.

---

### 3.6 Create Fraud Detection Query

**What you'll do:** Implement the core fraud detection logic as a streaming query.

**The Algorithm:**
1. **Self-join** on `cardHash` (find pairs of transactions for same card)
2. **Time window:** Second transaction within 2 hours of first
3. **Calculate distance:** Using Haversine approximation (111 km per degree)
4. **Calculate speed:** Distance ÷ Time
5. **Filter fraud:** Distance > 300 km AND Speed > 800 km/h

Copy and run this query from [`fraud_detection.sql`](../../../confluent-setup/sql/fraud_detection.sql).

Make sure the SQL workspace is set to **Streaming** mode before you run the statement:
1. In the Flink SQL workspace, locate the query execution mode control
2. Select **Streaming**
3. Then run the query

```sql
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
```

**Understanding Key Parts:**

**Self-Join:**
```sql
FROM `bank.transactions` a
JOIN `bank.transactions` b
  ON a.cardHash = b.cardHash
```
Finds pairs of transactions for the same card.

**Time Window:**
```sql
AND b.event_ts > a.event_ts
AND b.event_ts <= a.event_ts + INTERVAL '2' HOUR
```
Second transaction must be after first, within 2 hours.

**Distance Calculation:**
```sql
111 * SQRT(
  POWER(b.merchantLat - a.merchantLat, 2) +
  POWER(b.merchantLon - a.merchantLon, 2)
)
```
Haversine approximation: ~111 km per degree of latitude/longitude.

**Speed Calculation:**
```sql
distance / (time_in_seconds / 3600.0)
```
Converts to km/h.

**Fraud Thresholds:**
```sql
distance > 300  -- More than 300 km apart
speed > 800     -- Faster than 800 km/h (impossible for humans)
```

Click **"Run"** to start the streaming query.

✅ **Checkpoint:** Query status shows **"Running"** in Flink statements list.

---

## Step 4: Verification

### 4.1 Check Flink Statement Status

1. Go to **"Flink"** → **"Running statements"**
2. Find your fraud detection query
3. Status should be **"Running"**
4. Check metrics:
   - Records in: 0 (no data sent yet)
   - Records out: 0 (no fraud detected yet)

✅ **Checkpoint:** Statement is running with no errors.

---

### 4.2 Verify Topics in UI

1. Go to **"Topics"**
2. Verify these topics exist:
   - `bank.transactions`
   - `bank.fraud.transactions`
   - `agent.fraud.response`
3. Click on each topic and check:
   - Partitions: 1
   - Messages: 0 (no data yet)
   - Schema: Registered

✅ **Checkpoint:** All topics show correct configuration.

---

### 4.3 Verify Schemas

1. Go to **"Schema Registry"** → **"Schemas"**
2. Verify these schemas exist:
   - `bank.transactions-value` (version 1)
   - `bank.fraud.transactions-value` (version 1)
   - `agent.fraud.response-value`
   (version 1)
3. Click on each schema and verify:
   - Type: JSON
   - Compatibility: BACKWARD (default)
   - No validation errors

✅ **Checkpoint:** All schemas are registered and valid.

---

## 🎉 Lab Complete!

### What You've Built

You now have a **production-ready fraud detection system**:

✅ **Data Ingestion Layer**
- Topic: `bank.transactions`
- Schema: Validates all incoming transactions
- Ready to receive events from POS terminals and ATMs

✅ **Processing Layer**
- Flink SQL query running 24/7
- Detects impossible travel patterns in real-time
- Processes thousands of transactions per second

✅ **Alert Layer**
- Topic: `bank.fraud.transactions`
- Schema: Validates fraud alerts
- Ready for downstream consumers

✅ **Agent Response Layer**
- Topic: `agent.fraud.response`
- Schema: Validates agent responses
- Ready to receive AI-powered fraud analysis results

### What Happens Next

When transaction events arrive:
1. **Validation:** Schema Registry validates format
2. **Ingestion:** Events land in `bank.transactions` topic
3. **Processing:** Flink SQL continuously analyzes patterns
4. **Detection:** Impossible travel patterns trigger alerts
5. **Output:** Fraud alerts written to `bank.fraud.transactions`
6. **Action:** Downstream systems consume alerts and take action
6. **AI Analysis:** WatsonX Orchestrate agents consume fraud alerts for intelligent review
7. **Response:** Agent decisions written to `agent.fraud.response`
8. **Action:** Downstream systems consume agent responses and take action

---

## 🚀 What Happens After This Lab?

### Lab 2: Testing Fraud Scenarios
- Send realistic transaction data
- Trigger various fraud patterns
- Analyze detection accuracy
- Tune thresholds for optimal performance

### Lab 3: WatsonX Orchestrate Integration
- Connect AI agents to fraud alerts
- Automate investigation workflows
- Generate customer notifications
- Create case management tickets

[Go to Lab 2 →](../lab2-testing-fraud-scenarios/README.md)

---

## 🔗 Resources

- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Flink SQL Reference](https://docs.confluent.io/cloud/current/flink/reference/overview.html)
- [Schema Registry Guide](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Complete Setup Guide](../../../confluent-setup/COMPLETE_SETUP_GUIDE.md)

---

