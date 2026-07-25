# Credit Risk Data Platform

## Table of Contents
1. [Business Domain](#1-business-domain)
2. [High-Level System Deployment Diagram](#2-high-level-system-deployment-diagram)
3. [Repository Structure](#3-repository-structure)
4. [Quick Start: Running Locally](#4-quick-start-running-locally)
5. [Detailed Documentation](#5-detailed-documentation)

---

## 1. Business Domain
The core business domain of this project is **Credit Risk Management** within the financial services sector. 

In modern banking and financial institutions, accurately monitoring and predicting credit events is crucial for maintaining capital adequacy and regulatory compliance (such as Basel II/III frameworks). This platform simulates a real-time data ingestion pipeline that captures credit-related activities (e.g., loan origination, repayment delays, default signals). 

By streaming these events through a robust message broker, the system provides the foundational data infrastructure required by quantitative risk teams to calculate key risk indicators like Probability of Default (PD), Loss Given Default (LGD), and Exposure at Default (EAD).

## 2. High-Level System Deployment Diagram

*(Note for developer: Insert the diagram image here. Ensure each component is a deployable unit, arrows show data flow direction with data labels, and flows are numbered/color-coded per the rubric.)*

![System Deployment Diagram](./docs/images/architecture-diagram.png)

## 3. Repository Structure
Below is the structure of this repository and the purpose of each key component:

```text
credit-risk-data-platform/
│
├── docs/                           # Contains detailed documentation and reports
│   ├── IaC_Docker.md               # Infrastructure as Code and Docker optimization report
│   └── images/                     # Stores image assets (diagrams, terminal screenshots)
│
├── generators/                     # Custom application code
│   └── credit_events.py            # Main Python script generating simulated credit data
│
├── requirements.txt                # Dependencies for main processing framework (if any)
├── requirements-gen.txt            # Isolated dependencies for the Data Generator (pandas, faker, kafka)
│
├── docker-compose.yml              # Defines and orchestrates multi-container infrastructure (Kafka, Zookeeper, Generator)
├── Dockerfile                      # Optimized Multi-stage Dockerfile for the Data Generator
├── Dockerfile.non-optimized        # Baseline Dockerfile for benchmark comparisons
├── .dockerignore                   # Specifies files to exclude from the Docker build context
└── README.md                       # Main entry point and project overview
```

## 4. Quick Start: Running Locally

### Prerequisites
*   Docker and Docker Compose installed.

### Step 1: Environment Setup
Clone the repository and set up your environment variables. Never commit the actual `.env` file to version control.
```bash
# Copy the example environment file
cp .env.example .env

# (Optional) Edit the .env file with your specific configurations
# nano .env
```

### Step 2: Build the Data Generator Image
```bash
docker build -f Dockerfile -t credit-risk-platform:optimized .
```

### Step 3: Start the Infrastructure
```bash
docker-compose up -d
```

### Step 4: Verify Services
```bash
docker ps
docker logs -f platform-data-generator
```

### Step 5: Shut Down
```bash
docker-compose down
```

## 5. Detailed Documentation
For an in-depth explanation of the containerization strategy and the multi-stage build footprint benchmark, refer to the [IaC & Docker Optimization Report](./docs/IaC_Docker.md).