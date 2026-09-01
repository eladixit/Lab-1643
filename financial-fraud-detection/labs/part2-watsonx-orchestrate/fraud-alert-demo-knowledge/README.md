# Fraud Alert Demo Knowledge

This folder contains a compact, demo-friendly knowledge set for a watsonx Orchestrate fraud alert processing agent.

## Why this folder exists

The goal is to give the agent just enough reference knowledge to:

- interpret a fraud alert
- compare it with compact customer history
- explain fraud confidence in simple language
- recommend the next action

This is intentionally small and explainable for a demo.

## Required manual uploads for Part 2

In Part 2, students should manually upload the following files to the `CC Fraud Alert Processor` agent:

- [`agent-guardrails.pdf`](./agent-guardrails.pdf)
- [`fraud-playbook.pdf`](./fraud-playbook.pdf)
- [`decision-rules.pdf`](./decision-rules.pdf)
- [`customer-history-baselines.csv`](./customer-history-baselines.csv)

These are the required manual knowledge uploads for the lab walkthrough.

## Files in this folder

- `fraud-playbook.md`  
  Lightweight fraud concepts and demo scenarios the agent should understand.

- `fraud-playbook.pdf`  
  PDF version of the fraud playbook used as a manual upload in the lab.

- `decision-rules.md`  
  Explainable rules for turning an alert plus history into low, medium, or high confidence.

- `decision-rules.pdf`  
  PDF version of the decision rules used as a manual upload in the lab.

- `agent-guardrails.md`  
  Safety and response boundaries for the demo agent.

- `agent-guardrails.pdf`  
  PDF version of the guardrails used as a manual upload in the lab.

- `customer-history-baselines.csv`  
  Customer baseline data uploaded manually during the lab.

- `customer-history-summary.json`  
  Compact per-card baseline summaries derived from [`test_mixed_transactions.csv`](../../../confluent-setup/data/test_mixed_transactions.csv).

- `customer-history-examples.json`  
  Additional example customer history records for reference and prompt design.

## Design principles

This knowledge set is:

- compact rather than exhaustive
- explainable rather than model-heavy
- focused on impossible-travel demo scenarios
- suitable for retrieval or direct reference by a watsonx Orchestrate agent

## How customer history is represented

The raw transactions remain in [`test_mixed_transactions.csv`](../../../confluent-setup/data/test_mixed_transactions.csv).

The JSON files in this folder are useful reference material for understanding the demo history patterns, but the required manual upload for the lab walkthrough is [`customer-history-baselines.csv`](./customer-history-baselines.csv).

This keeps the demo clean while still allowing the agent to explain why an alert looks suspicious.

## Suggested usage in the demo

Use these files as:

- knowledge documents for the agent
- reference material for prompt design
- support material when explaining how the agent derives confidence from customer history

For the guided lab flow, follow the required upload list in [`../README.md`](../README.md).