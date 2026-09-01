# Real-Time Fraud Detection with Confluent Cloud and watsonx Orchestrate

## Overview

This bootcamp demonstrates how to build an intelligent, real-time fraud detection system by combining **Confluent Cloud's stream processing capabilities** with **IBM watsonx Orchestrate's AI-powered decision-making**. You'll learn to detect impossible travel patterns in card transactions and enrich fraud alerts with AI-driven risk analysis.

### What You'll Build

By the end of this bootcamp, you will have created:

- **Real-time fraud detection pipeline** using Confluent Cloud and Flink SQL
- **Impossible travel detection** that identifies when a card is used in two distant locations within an impossibly short timeframe
- **AI-powered fraud analysis** using watsonx Orchestrate agents with custom knowledge bases
- **End-to-end integration** from event detection to intelligent decision-making

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PART 1: Event Processing                    │
│                                                                     │
│  Card Transactions  →  Flink SQL Detection  →  Fraud Alerts         │
│  (Kafka Topic)         (Impossible Travel)      (Kafka Topic)       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PART 2: AI-Powered Analysis                    │
│                                                                     │
│  Python Consumer  →  watsonx Agent  →  Enriched Response            │
│  (Reads Alerts)      (Risk Analysis)    (Published to Kafka)        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Learning Objectives

### Technical Skills

By completing this bootcamp, you will learn:

**Stream Processing & Event-Driven Architecture:**
- Configure Confluent Cloud environments and Kafka clusters
- Design and register JSON schemas for streaming data
- Write Flink SQL queries for real-time pattern detection
- Implement self-joins and time windows for fraud detection
- Manage Kafka producers and consumers with Python

**AI Integration & Orchestration:**
- Create and configure watsonx Orchestrate agents
- Build knowledge bases for AI-powered decision-making
- Integrate AI agents with event-driven systems
- Validate and publish structured AI responses
- Handle authentication and API integration

**DevOps & Best Practices:**
- Use modern Python package management with uv
- Manage environment variables and secrets securely
- Implement schema validation for data quality
- Monitor and troubleshoot distributed systems

### Business Skills

**Fraud Detection Fundamentals:**
- Understand impossible travel fraud patterns
- Apply business rules and thresholds
- Balance false positives vs false negatives
- Interpret fraud detection metrics

**AI-Augmented Decision Making:**
- Leverage AI for risk assessment
- Provide explainable fraud decisions
- Integrate human expertise through knowledge bases
- Scale fraud investigation workflows

---

## Prerequisites

Before starting this bootcamp, ensure you have:

### Required Accounts
- **Confluent Cloud account** - [Sign up for free trial](https://www.confluent.io/confluent-cloud/tryfree/)
- **watsonx Orchestrate instance** - [Reserve via TechZone](#reserving-watsonx-orchestrate-on-techzone)

### Technical Requirements
- **uv package manager** - [Installation instructions](#installing-uv)
  - *Modern Python package manager for dependency management*
- **Git** - For cloning the repository
  - *To download the bootcamp code and resources*
- **Code editor** (VS Code, PyCharm, etc.) - Optional but recommended
  - *For viewing and editing configuration files*
  - *Not required if using IBM BOB for Part 2 agent setup*

### Optional for Part 2 (Automated Agent Setup)
- **IBM BOB** - For automated watsonx Orchestrate agent creation
  - *Alternative to manual UI-based agent setup*
  - *Provides faster, reproducible agent configuration*
  - *See [Part 2 BOB Setup Guide](labs/part2-watsonx-orchestrate/setup-with-bob.md)*

### Knowledge Prerequisites
- Basic understanding of SQL (SELECT, JOIN, WHERE clauses)
  - *Required for writing and understanding Flink SQL queries*
- Familiarity with JSON data structures
  - *For working with schemas and message formats*
- Basic Python knowledge (helpful but not required)
  - *For understanding the consumer/producer applications*
- Understanding of REST APIs (helpful for Part 2)
  - *For integrating with watsonx Orchestrate*

---

## Reserving watsonx Orchestrate on TechZone

To complete Part 2 of this bootcamp, you'll need access to a watsonx Orchestrate instance. Follow these steps to reserve one via IBM TechZone:

1. **Go to TechZone:** Visit [watsonx Orchestrate Essentials](https://techzone.ibm.com/my/reservations/create/6931cf6125f231c47e59cb7f)

2. **Request an environment:**
   - Click on **"Request an environment"**

3. **Fill in the reservation details:**
   - **Purpose:** Select **"Education"**
   - **Purpose description:** Enter `for wxo-confluent bootcamp`
   - **Preferred region:** Choose your preferred region

4. **Accept terms and submit:**
   - Check **"I agree to IBM Technology Zone's Terms & Conditions"**
   - Click **"Submit"**

5. **Wait for provisioning:**
   - Your reservation will be processed
   - You'll receive an email with access details once ready
   - This typically takes 15-30 minutes

> [!NOTE]
> Make sure to reserve your watsonx Orchestrate instance before starting Part 2 of the bootcamp.

---

## Installing uv

### macOS/Linux

Use curl to download and execute the installation script:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If your system doesn't have curl, use wget:
```bash
wget -qO- https://astral.sh/uv/install.sh | sh
```

### Windows

Use PowerShell to download and execute the installation script:
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

> [!NOTE]
> Changing the execution policy allows running a script from the internet.

For more installation options, see the [official uv documentation](https://docs.astral.sh/uv/getting-started/installation/).

---

## Getting Started

### Clone the Repository

```bash
git clone https://github.ibm.com/AmericasTopTeam/wxo-confluent-americas.git
cd wxo-confluent-americas
```

### Open the Folder in IBM BOB (Optional)

If you plan to complete Part 2 using IBM BOB, open the cloned repository folder in IBM BOB.

### Install Dependencies

```bash
cd financial-fraud-detection
uv sync --locked
```

This will:
- Create a virtual environment
- Install all required dependencies from `pyproject.toml`
- Use the locked versions from `uv.lock` for reproducibility

---

## Bootcamp Structure

This bootcamp is divided into two main parts, each containing hands-on labs:

### Part 1: Confluent Cloud Event Processing

Build a real-time fraud detection pipeline using Confluent Cloud and Flink SQL.

#### [Lab 1: Confluent Cloud Setup](labs/part1-confluent-cep/lab1-confluent-setup/README.md)

Set up your Confluent Cloud environment and deploy a Flink SQL fraud detection query.

**What you'll learn:**
- Create Confluent Cloud environments and Kafka clusters
- Design and register JSON schemas
- Set up Flink compute pools
- Write Flink SQL for impossible travel detection
- Generate API keys for secure access

**What you'll build:**
- Input topic: `bank.transactions`
- Output topic: `bank.fraud.transactions`
- Flink SQL query detecting impossible travel patterns
- Schema Registry with validated data contracts

#### [Lab 2: Testing Fraud Detection Scenarios](labs/part1-confluent-cep/lab2-testing-fraud-scenarios/README.md)

Test your fraud detection pipeline with realistic transaction data.

**What you'll learn:**
- Configure Python applications with Confluent Cloud
- Send test transactions with proper timestamps
- Consume and interpret fraud alerts
- Analyze detection accuracy

**What you'll build:**
- Python producer sending test transactions
- Python consumer displaying fraud alerts
- Verification of fraud detection logic
- Understanding of system behavior

---

### Part 2: watsonx Orchestrate AI Integration

Enhance fraud alerts with AI-powered risk analysis using watsonx Orchestrate.

#### [Lab: watsonx Orchestrate Agent Integration](labs/part2-watsonx-orchestrate/README.md)

Integrate an AI agent that analyzes fraud alerts and provides intelligent recommendations.

**What you'll learn:**
- Create and configure watsonx Orchestrate agents
- Build fraud investigation knowledge bases
- Extract and configure integration credentials
- Validate AI responses against schemas
- Publish enriched responses to Kafka

**What you'll build:**
- `CC Fraud Alert Processor` agent
- Fraud knowledge base with decision rules
- Python consumer with agent integration
- Agent response topic: `agent.fraud.responses`
- End-to-end AI-augmented fraud workflow

---

## Project Structure

```
wxo-confluent-americas/
├── README.md                          # This file
├── pyproject.toml                     # Python dependencies
├── uv.lock                            # Locked dependency versions
│
├── confluent-setup/                   # Confluent Cloud configuration
│   ├── data/                          # Test transaction data
│   │   └── test_mixed_transactions.csv
│   ├── schemas/                       # JSON schemas
│   │   ├── card-present-transaction.schema.json
│   │   ├── fraud-alert-agent.schema.json
│   │   └── fraud-alert-input.schema.json
│   └── sql/                           # Flink SQL queries
│       └── fraud_detection.sql
│
├── consumer-app/                      # Python applications
│   ├── .env.example                   # Environment variables template
│   ├── consume_fraud_alerts.py        # Basic fraud alert consumer
│   ├── consume_fraud_alerts_with_agent.py  # Agent-integrated consumer
│   ├── produce_test_with_current_time.py   # Test data producer
│   ├── orchestrate_client.py          # watsonx Orchestrate API client
│   ├── agent_payload_builder.py       # Transform alerts for agent
│   ├── agent_response_producer.py     # Publish agent responses
│   └── response_validator.py          # Validate agent responses
│
└── labs/                              # Lab instructions
    ├── part1-confluent-cep/
    │   ├── lab1-confluent-setup/
    │   └── lab2-testing-fraud-scenarios/
    └── part2-watsonx-orchestrate/
```

---

## Key Concepts

### Impossible Travel Detection

The fraud detection system identifies when a card is used in two locations that are too far apart to travel between in the given time:

**Detection Logic:**
```sql
-- Calculate distance between transactions
distance > 300 km

-- Calculate travel speed
speed > 800 km/h (faster than commercial aircraft)

-- Time window
Within 2 hours of each other
```

**Example Fraud Scenario:**
```
Card: CARD_DAVID_004
Transaction 1: Miami, US at 10:00 AM
Transaction 2: Tokyo, JP at 10:10 AM
Distance: 24,427 km in 10 minutes
Speed: 146,563 km/h (73x faster than supersonic jets!)
Result: FRAUD ALERT
```

### AI-Powered Risk Analysis

The watsonx Orchestrate agent enhances fraud alerts with:

- **Risk Assessment:** Confidence levels (low, medium, high)
- **Contextual Analysis:** Customer history and behavior patterns
- **Recommended Actions:** monitor, review, create_case, escalate
- **Explainable Decisions:** Reasoning for each recommendation
- **Structured Output:** Schema-validated JSON responses

---

## Support & Resources

### Documentation
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Flink SQL Reference](https://docs.confluent.io/cloud/current/flink/reference/overview.html)
- [watsonx Orchestrate Documentation](https://www.ibm.com/docs/en/watsonx/watson-orchestrate)
- [uv Documentation](https://docs.astral.sh/uv/)

### Troubleshooting
- Check lab-specific troubleshooting sections
- Review error messages carefully
- Verify environment variables and credentials
- Ensure all prerequisites are met

---
