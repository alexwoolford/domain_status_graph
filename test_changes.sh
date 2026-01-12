#!/bin/bash
# End-to-end test script for recent changes
# Tests: LayeredEntityValidator removal, TieredDecisionSystem migration, documentation cleanup

# Note: We don't use set -e because Test 1 expects a command to fail

echo "🧪 Testing Recent Changes - End-to-End"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0

test_pass() {
    echo -e "${GREEN}✅ PASS:${NC} $1"
    ((PASSED++))
}

test_fail() {
    echo -e "${RED}❌ FAIL:${NC} $1"
    ((FAILED++))
}

test_info() {
    echo -e "${YELLOW}ℹ️  INFO:${NC} $1"
}

# Test 1: Verify LayeredEntityValidator is removed
echo "Test 1: Verify LayeredEntityValidator is removed"
if python -c "from public_company_graph.entity_resolution import LayeredEntityValidator" 2>/dev/null; then
    test_fail "LayeredEntityValidator still exists"
else
    test_pass "LayeredEntityValidator removed"
fi
set -e  # Re-enable exit on error after this test
echo ""

# Test 2: Verify TieredDecisionSystem can be imported
echo "Test 2: Verify TieredDecisionSystem can be imported"
if python -c "from public_company_graph.entity_resolution import TieredDecisionSystem, Decision, DecisionTier" 2>/dev/null; then
    test_pass "TieredDecisionSystem imports successfully"
else
    test_fail "TieredDecisionSystem import failed"
    exit 1
fi
echo ""

# Test 3: Verify migrated evaluation scripts can import
echo "Test 3: Verify migrated evaluation scripts can import"
for script in er_analyze_errors.py er_evaluate_split.py evaluate_layered_validator.py; do
    if python -c "import sys; sys.path.insert(0, 'scripts'); from ${script%.py} import *" 2>/dev/null; then
        test_pass "$script imports successfully"
    else
        test_fail "$script import failed"
    fi
done
echo ""

# Test 4: Verify no broken imports in production code
echo "Test 4: Verify no broken imports in production code"
if python -c "
from public_company_graph.parsing.business_relationship_extraction import extract_all_relationships
from public_company_graph.entity_resolution.tiered_decision import TieredDecisionSystem
from public_company_graph.entity_resolution.embedding_scorer import EmbeddingSimilarityScorer
print('All production imports successful')
" 2>/dev/null; then
    test_pass "Production code imports successfully"
else
    test_fail "Production code import failed"
    exit 1
fi
echo ""

# Test 5: Run test suite (critical tests only)
echo "Test 5: Run critical test suite"
if python -m pytest tests/entity_resolution/test_tiered_decision.py tests/parsing/test_edge_cleanup.py tests/integration/test_idempotency.py::TestLoaderErrorHandling::test_load_domains_handles_missing_required_field -v --tb=short 2>/dev/null; then
    test_pass "Critical tests pass"
else
    test_fail "Some critical tests failed"
fi
echo ""

# Test 6: Verify evaluation scripts can initialize (dry run)
echo "Test 6: Verify evaluation scripts can initialize (dry run)"
test_info "Checking er_analyze_errors.py..."
if python scripts/er_analyze_errors.py --help >/dev/null 2>&1; then
    test_pass "er_analyze_errors.py can run"
else
    test_fail "er_analyze_errors.py failed"
fi

test_info "Checking er_evaluate_split.py..."
if python scripts/er_evaluate_split.py --help >/dev/null 2>&1; then
    test_pass "er_evaluate_split.py can run"
else
    test_fail "er_evaluate_split.py failed"
fi

test_info "Checking evaluate_layered_validator.py..."
if python scripts/evaluate_layered_validator.py --help >/dev/null 2>&1; then
    test_pass "evaluate_layered_validator.py can run"
else
    test_fail "evaluate_layered_validator.py failed"
fi
echo ""

# Test 7: Verify extraction script can import TieredDecisionSystem
echo "Test 7: Verify extraction script can import TieredDecisionSystem"
if python -c "
import sys
sys.path.insert(0, 'scripts')
from extract_with_llm_verification import *
from public_company_graph.entity_resolution.tiered_decision import TieredDecisionSystem
print('Extraction script imports successfully')
" 2>/dev/null; then
    test_pass "Extraction script imports successfully"
else
    test_fail "Extraction script import failed"
fi
echo ""

# Test 8: Verify documentation files exist
echo "Test 8: Verify essential documentation exists"
ESSENTIAL_DOCS=(
    "docs/graph_schema.md"
    "docs/money_queries.md"
    "docs/architecture.md"
    "docs/step_by_step_guide.md"
    "docs/10k_parsing.md"
    "docs/research_enhancements.md"
    "docs/EDGE_QUALITY_SYSTEM.md"
)
for doc in "${ESSENTIAL_DOCS[@]}"; do
    if [ -f "$doc" ]; then
        test_pass "$doc exists"
    else
        test_fail "$doc missing"
    fi
done
echo ""

# Test 9: Verify removed files are gone
echo "Test 9: Verify removed files are gone"
REMOVED_FILES=(
    "public_company_graph/entity_resolution/layered_validator.py"
    "tests/entity_resolution/test_layered_validator.py"
    "docs/CONSOLIDATION_COMPLETE.md"
    "docs/SIMPLIFICATION_PROGRESS.md"
    "docs/EXPERT_CODE_REVIEW.md"
)
for file in "${REMOVED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        test_pass "$file removed"
    else
        test_fail "$file still exists"
    fi
done
echo ""

# Test 10: Quick smoke test - TieredDecisionSystem initialization
echo "Test 10: Quick smoke test - TieredDecisionSystem initialization"
if python -c "
from public_company_graph.entity_resolution.tiered_decision import TieredDecisionSystem, Decision
from public_company_graph.entity_resolution.candidates import Candidate

# Create a simple decision system
system = TieredDecisionSystem(use_tier1=True, use_tier2=True, use_tier3=False, use_tier4=False)

# Create a test candidate
candidate = Candidate(
    text='Microsoft',
    sentence='We compete with Microsoft in cloud services.',
    start_pos=0,
    end_pos=9,
    source_pattern='test'
)

# Make a decision
decision = system.decide(
    candidate=candidate,
    context='We compete with Microsoft in cloud services.',
    relationship_type='HAS_COMPETITOR',
    company_name='Microsoft Corporation',
    embedding_similarity=None
)

assert decision is not None, 'Decision should not be None'
assert decision.decision in [Decision.ACCEPT, Decision.REJECT, Decision.CANDIDATE], 'Invalid decision'
print('TieredDecisionSystem smoke test passed')
" 2>/dev/null; then
    test_pass "TieredDecisionSystem smoke test"
else
    test_fail "TieredDecisionSystem smoke test failed"
fi
echo ""

# Summary
echo "======================================"
echo "Test Summary"
echo "======================================"
echo -e "${GREEN}Passed:${NC} $PASSED"
echo -e "${RED}Failed:${NC} $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed! Ready to commit.${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed. Please fix before committing.${NC}"
    exit 1
fi
