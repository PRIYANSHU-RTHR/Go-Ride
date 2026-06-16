# Go-Ride

## Overview

Go-Ride is a distributed ride-sharing backend platform built using Go, Docker, and Kubernetes. The system follows a microservices architecture designed to manage ride requests, driver operations, trip scheduling, and payment processing in a scalable and resilient manner.

The platform demonstrates distributed systems principles such as service isolation, asynchronous communication, observability, and horizontal scalability. Each service is independently deployable and communicates through APIs and message queues, making the system suitable for high-throughput environments.

## Features

* Microservices-based architecture
* API Gateway for centralized request routing
* Driver management and availability tracking
* Trip creation and lifecycle management
* Payment processing workflows
* Asynchronous event-driven communication using RabbitMQ
* Distributed tracing with Jaeger
* Containerized deployment with Docker
* Kubernetes orchestration and scaling
* Production-ready deployment support

## System Architecture

The platform consists of the following core services:

### API Gateway

Acts as the entry point for client requests and routes traffic to the appropriate backend services.

### Driver Service

Manages driver registration, availability updates, and driver-related operations.

### Trip Service

Handles ride requests, trip scheduling, driver assignment, and trip state transitions.

### Payment Service

Processes ride payments and manages transaction workflows.

### RabbitMQ

Provides asynchronous messaging between services for reliable event-driven communication.

### Jaeger

Enables distributed tracing and observability across the system.

## Architecture Highlights

* Event-driven microservices communication
* Horizontal scalability through Kubernetes
* Fault isolation between services
* Distributed tracing and monitoring
* Cloud-native deployment workflows
* Production-focused infrastructure design

## Tech Stack

| Category          | Technologies                |
| ----------------- | --------------------------- |
| Language          | Go                          |
| Containerization  | Docker                      |
| Orchestration     | Kubernetes                  |
| Messaging         | RabbitMQ                    |
| Observability     | Jaeger                      |
| APIs              | REST, gRPC                  |
| Cloud Platform    | Google Cloud Platform (GCP) |
| Local Development | Tilt, Minikube              |

## Running Locally

```bash
tilt up
```

## Monitoring

```bash
kubectl get pods
```

or

```bash
minikube dashboard
```

## Deployment

Deployment manifests are provided for Kubernetes environments, including support for:

* Google Kubernetes Engine (GKE)
* Artifact Registry image hosting
* Secret and configuration management
* Service discovery and networking
* HTTPS ingress configuration
* Scalable production deployments


