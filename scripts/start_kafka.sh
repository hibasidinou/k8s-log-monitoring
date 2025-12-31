#!/bin/bash

# Script to start Kafka using Docker Compose
# Usage: ./scripts/start_kafka.sh

set -e

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "  Starting Kafka with Docker Compose"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

# Navigate to docker directory
cd docker

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Error: docker-compose.yml not found in docker directory"
    exit 1
fi

echo "📦 Starting Kafka and Zookeeper..."
docker-compose up -d

echo ""
echo "⏳ Waiting for Kafka to be ready..."
sleep 5

# Check if Kafka is running
if docker-compose ps | grep -q "kafka.*Up"; then
    echo "✅ Kafka is running!"
    echo ""
    echo "Kafka broker: localhost:9092"
    echo ""
    echo "To view logs:"
    echo "  cd docker && docker-compose logs -f kafka"
    echo ""
    echo "To stop Kafka:"
    echo "  cd docker && docker-compose down"
    echo ""
    echo "To check status:"
    echo "  cd docker && docker-compose ps"
else
    echo "❌ Error: Kafka failed to start"
    echo "Check logs with: cd docker && docker-compose logs"
    exit 1
fi



