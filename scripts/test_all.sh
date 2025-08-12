#!/bin/bash
set -e

echo "🚀 Starting Email Cleaner Test Suite"
echo "======================================"

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to wait for services to be ready
wait_for_services() {
    echo "⏳ Waiting for Spark services to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8080 > /dev/null; then
            echo "✅ Spark Master is ready"
            break
        fi
        echo "   Attempt $attempt/$max_attempts - waiting for Spark Master..."
        sleep 2
        ((attempt++))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo "❌ Spark services failed to start within timeout"
        exit 1
    fi
    
    # Give a bit more time for worker to connect
    sleep 5
}

echo "📋 Step 1: Checking Docker..."
check_docker

echo "📋 Step 2: Building UDF..."
uv run datacompose build email

echo "📋 Step 3: Running unit tests..."
echo "Running tests that don't require external services..."
uv run pytest -m unit -v --tb=short

echo "📋 Step 4: Starting Docker services..."
docker-compose up -d

echo "📋 Step 5: Waiting for services..."
wait_for_services

echo "📋 Step 6: Running Spark integration tests..."
echo "Running tests that require Spark cluster..."
uv run pytest -m spark -v --tb=short

echo "📋 Step 7: Running all tests together..."
echo "Final validation with complete test suite..."
uv run pytest -v --tb=short

echo "📋 Step 8: Cleaning up..."
echo "Stopping Docker services..."
docker-compose down

echo ""
echo "🎉 All tests completed successfully!"
echo "======================================"
echo "Summary:"
echo "✅ UDF generation"
echo "✅ Unit tests (pandas functionality)"
echo "✅ Spark integration tests (cluster connectivity)"
echo "✅ Complete test suite"
echo ""
echo "Your email cleaning UDF is ready for production! 🚀"