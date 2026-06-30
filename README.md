# Big Data Streaming Pipeline

> **Note**
>
> This repository contains a two-person team project developed as part of the Big Data Technologies course in the MSc Informatics program at FH Wiener Neustadt.

## Project Overview

This project implements an end-to-end cloud-native streaming pipeline for near real-time data processing using Apache Spark Structured Streaming, Confluent Kafka, Google BigQuery, Docker and Terraform.

Using the MovieLens dataset as an event source, the system continuously ingests, processes and aggregates streaming events. The processed results are stored in Google BigQuery and made available through a REST API.

The project focuses on integrating modern big data technologies into a scalable, event-driven architecture rather than implementing isolated components.


## Objectives

The main objectives of this project were to:

- Design an end-to-end streaming data pipeline
- Process events in near real time using Apache Spark Structured Streaming
- Build an event-driven architecture with Apache Kafka
- Deploy cloud infrastructure using Terraform
- Orchestrate local services with Docker Compose
- Store analytical results in Google BigQuery
- Provide processed data through a REST API
- Apply cloud-native design principles and Infrastructure as Code (IaC)


## Architecture

The pipeline follows an event-driven architecture.

MovieLens rating data is continuously read by a Python producer and published as events to Apache Kafka. Apache Spark Structured Streaming consumes these events, performs near real-time aggregations and writes the processed results back to Kafka.

A Fivetran connector automatically transfers the aggregated events to Google BigQuery, where they can be queried efficiently for analytical purposes.

A Flask REST API exposes the processed data through HTTP endpoints, allowing analytical results to be consumed by external applications.

Each component has a clearly defined responsibility and communicates exclusively through events, resulting in a loosely coupled, scalable and cloud-native architecture.


## Technology Stack

- Python
- Apache Spark Structured Streaming
- Confluent Kafka
- Google BigQuery
- Google Cloud Platform (GCP)
- Google IAM
- Docker
- Docker Compose
- Terraform
- Flask
- REST API
- Fivetran


## Development Environment

The streaming components were developed and tested using Docker Compose. During development, an Azure Linux Virtual Machine was used to execute the PySpark workloads required for the project.


## Key Features

- End-to-end streaming data pipeline
- Near real-time event processing
- Event streaming with Apache Kafka
- Event-driven architecture
- Cloud-native system design
- Infrastructure as Code (Terraform)
- Containerized local development with Docker Compose
- Automated data loading into BigQuery
- REST API for serving processed results
- Loosely coupled and scalable architecture


## Results

The completed solution demonstrates how modern cloud-native technologies can be integrated into a scalable streaming architecture.

The pipeline continuously ingests, processes and aggregates streaming events before automatically delivering analytical data to Google BigQuery and exposing it through a REST API.


## Future Improvements

Possible future enhancements include:

- Kubernetes deployment
- CI/CD pipeline automation
- Monitoring and observability
- Streaming dashboards
- Enhanced data quality monitoring
