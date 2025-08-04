#!/bin/bash
# Coverage Update Script for Quant Async
# Runs tests, generates coverage report, and updates badge in README

set -e

echo "🧪 Running coverage analysis..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Run tests with coverage (excluding integration tests by default)
echo -e "${YELLOW}📊 Running unit tests with coverage...${NC}"
uv run pytest tests/ -m "not integration" \
    --cov=src/quant_async \
    --cov-report=term \
    --cov-report=json \
    --cov-report=html \
    -v

# Generate HTML coverage report
echo -e "${YELLOW}📈 Coverage HTML report generated in htmlcov/index.html${NC}"

# Update coverage badge in README
echo -e "${YELLOW}🎯 Updating coverage badge...${NC}"
python scripts/generate_coverage_badge.py

# Show coverage summary
if [ -f coverage.json ]; then
    COVERAGE=$(python -c "import json; data=json.load(open('coverage.json')); print(f\"{data['totals']['percent_covered']:.1f}\")")
    echo ""
    echo -e "${GREEN}✅ Coverage analysis complete!${NC}"
    echo -e "📊 Overall coverage: ${COVERAGE}%"
    echo -e "📁 HTML report: htmlcov/index.html"
    echo -e "🎯 Badge updated in README.md"
    
    # Provide coverage feedback
    if (( $(echo "$COVERAGE >= 80" | bc -l) )); then
        echo -e "${GREEN}🎉 Great coverage! Keep it up!${NC}"
    elif (( $(echo "$COVERAGE >= 60" | bc -l) )); then
        echo -e "${YELLOW}⚠️ Good coverage, but there's room for improvement${NC}"
    else
        echo -e "${RED}⚠️ Coverage is below 60%. Consider adding more tests.${NC}"
    fi
else
    echo -e "${RED}❌ Coverage report not generated${NC}"
    exit 1
fi

echo ""
echo "Next steps:"
echo "1. Open htmlcov/index.html to see detailed coverage report"
echo "2. Add tests for uncovered code to improve coverage"
echo "3. Commit the updated README.md with the new coverage badge"