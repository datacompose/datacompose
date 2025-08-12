#!/bin/bash

# Docker development helper script for Datacompose
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    echo -e "${2}${1}${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_color "Docker is not running. Please start Docker first." "$RED"
        exit 1
    fi
}

# Function to show help
show_help() {
    echo "Datacompose Docker Development Helper"
    echo ""
    echo "Usage: ./docker-dev.sh [command]"
    echo ""
    echo "Commands:"
    echo "  up          Start all services (Spark, Postgres, MinIO, and Datacompose dev container)"
    echo "  down        Stop all services"
    echo "  restart     Restart all services"
    echo "  logs        Show logs from all services"
    echo "  shell       Enter the Datacompose development container"
    echo "  test        Run tests inside the container"
    echo "  spark-ui    Open Spark Master UI in browser"
    echo "  minio-ui    Open MinIO Console in browser"
    echo "  pgadmin     Start pgAdmin (if not running)"
    echo "  clean       Stop services and remove volumes"
    echo "  build       Rebuild the development container"
    echo "  status      Show status of all services"
    echo "  help        Show this help message"
}

# Main command handling
case "$1" in
    up)
        check_docker
        print_color "Starting Datacompose development environment..." "$GREEN"
        docker-compose up -d
        print_color "Waiting for services to be ready..." "$YELLOW"
        sleep 5
        docker-compose ps
        print_color "\nDevelopment environment is ready!" "$GREEN"
        print_color "VS Code: Open the folder in VS Code and select 'Reopen in Container'" "$YELLOW"
        print_color "Spark UI: http://localhost:8080" "$YELLOW"
        print_color "MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)" "$YELLOW"
        ;;
    
    down)
        check_docker
        print_color "Stopping Datacompose development environment..." "$YELLOW"
        docker-compose down
        print_color "Environment stopped." "$GREEN"
        ;;
    
    restart)
        check_docker
        print_color "Restarting Datacompose development environment..." "$YELLOW"
        docker-compose restart
        print_color "Environment restarted." "$GREEN"
        ;;
    
    logs)
        check_docker
        docker-compose logs -f
        ;;
    
    shell)
        check_docker
        print_color "Entering Datacompose development container..." "$GREEN"
        docker-compose exec datacompose-dev bash
        ;;
    
    test)
        check_docker
        print_color "Running tests in container..." "$GREEN"
        docker-compose exec datacompose-dev pytest tests/
        ;;
    
    spark-ui)
        if command -v open > /dev/null 2>&1; then
            open http://localhost:8080
        elif command -v xdg-open > /dev/null 2>&1; then
            xdg-open http://localhost:8080
        else
            print_color "Spark Master UI: http://localhost:8080" "$GREEN"
        fi
        ;;
    
    minio-ui)
        if command -v open > /dev/null 2>&1; then
            open http://localhost:9001
        elif command -v xdg-open > /dev/null 2>&1; then
            xdg-open http://localhost:9001
        else
            print_color "MinIO Console: http://localhost:9001" "$GREEN"
            print_color "Login: minioadmin / minioadmin123" "$YELLOW"
        fi
        ;;
    
    pgadmin)
        check_docker
        print_color "Starting pgAdmin..." "$GREEN"
        docker-compose --profile tools up -d pgadmin
        print_color "pgAdmin: http://localhost:5050 (admin@example.com/admin123)" "$YELLOW"
        ;;
    
    
    clean)
        check_docker
        print_color "Stopping services and removing volumes..." "$YELLOW"
        docker-compose down -v
        print_color "Clean complete." "$GREEN"
        ;;
    
    build)
        check_docker
        print_color "Rebuilding development container..." "$YELLOW"
        docker-compose build --no-cache datacompose-dev
        print_color "Build complete." "$GREEN"
        ;;
    
    status)
        check_docker
        print_color "Service Status:" "$GREEN"
        docker-compose ps
        ;;
    
    help|--help|-h|"")
        show_help
        ;;
    
    *)
        print_color "Unknown command: $1" "$RED"
        echo ""
        show_help
        exit 1
        ;;
esac