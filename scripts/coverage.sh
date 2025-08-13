#!/bin/bash
# Generate and display test coverage report

echo "Running tests with coverage..."
pytest tests/ --cov=datacompose --cov-report=term --cov-report=html --cov-report=json

# Extract coverage percentage
COVERAGE=$(python -c "import json; print(json.load(open('coverage.json'))['totals']['percent_covered'])")
COVERAGE_INT=${COVERAGE%.*}

# Determine badge color
if [ $COVERAGE_INT -ge 90 ]; then
    COLOR="brightgreen"
elif [ $COVERAGE_INT -ge 80 ]; then
    COLOR="green"
elif [ $COVERAGE_INT -ge 70 ]; then
    COLOR="yellowgreen"
elif [ $COVERAGE_INT -ge 60 ]; then
    COLOR="yellow"
elif [ $COVERAGE_INT -ge 50 ]; then
    COLOR="orange"
else
    COLOR="red"
fi

echo ""
echo "========================================="
echo "Total Coverage: ${COVERAGE}%"
echo "Badge Color: $COLOR"
echo "========================================="
echo ""
echo "HTML report generated at: htmlcov/index.html"
echo "To view: open htmlcov/index.html"

# Update README badge if requested
if [ "$1" == "--update-badge" ]; then
    echo "Updating coverage badge in README.md..."
    sed -i "s/coverage-[0-9]*%25/coverage-${COVERAGE_INT}%25/g" README.md
    sed -i "s/badge\/coverage-[0-9]*%25-[a-z]*/badge\/coverage-${COVERAGE_INT}%25-${COLOR}/g" README.md
    echo "Badge updated!"
fi