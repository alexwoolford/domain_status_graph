# Testing Guide - Recent Changes

This guide helps you test the recent changes before committing:
- Removal of `LayeredEntityValidator`
- Migration of evaluation scripts to `TieredDecisionSystem`
- Documentation cleanup

## Quick Test (Recommended)

Run the automated test script:

```bash
./test_changes.sh
```

This will run 10 comprehensive tests covering:
1. ✅ LayeredEntityValidator removal verification
2. ✅ TieredDecisionSystem imports
3. ✅ Migrated evaluation scripts
4. ✅ Production code imports
5. ✅ Critical test suite
6. ✅ Evaluation script initialization
7. ✅ Extraction script imports
8. ✅ Essential documentation exists
9. ✅ Removed files are gone
10. ✅ TieredDecisionSystem smoke test

## Manual Testing

If you prefer to test manually:

### 1. Test Imports

```bash
# Should fail (LayeredEntityValidator removed)
python -c "from public_company_graph.entity_resolution import LayeredEntityValidator" && echo "FAIL: Still exists" || echo "PASS: Removed"

# Should succeed (TieredDecisionSystem available)
python -c "from public_company_graph.entity_resolution import TieredDecisionSystem, Decision, DecisionTier" && echo "PASS: Imports work"
```

### 2. Test Evaluation Scripts

```bash
# Test each migrated script can at least show help
python scripts/er_analyze_errors.py --help
python scripts/er_evaluate_split.py --help
python scripts/evaluate_layered_validator.py --help
```

### 3. Run Critical Tests

```bash
# Run tests for TieredDecisionSystem and edge cleanup
python -m pytest tests/entity_resolution/test_tiered_decision.py -v
python -m pytest tests/parsing/test_edge_cleanup.py -v
python -m pytest tests/integration/test_idempotency.py::TestLoaderErrorHandling::test_load_domains_handles_missing_required_field -v
```

### 4. Full Test Suite (Optional)

```bash
# Run all tests (may take a few minutes)
python -m pytest tests/ -x --tb=short -q
```

### 5. Smoke Test - TieredDecisionSystem

```bash
python -c "
from public_company_graph.entity_resolution.tiered_decision import TieredDecisionSystem, Decision
from public_company_graph.entity_resolution.candidates import Candidate

system = TieredDecisionSystem(use_tier1=True, use_tier2=True, use_tier3=False, use_tier4=False)
candidate = Candidate(text='Microsoft', sentence='We compete with Microsoft.', start_pos=0, end_pos=9, source_pattern='test')
decision = system.decide(candidate=candidate, context='We compete with Microsoft.', relationship_type='HAS_COMPETITOR', company_name='Microsoft Corporation', embedding_similarity=None)
print(f'Decision: {decision.decision.value}, Tier: {decision.tier.value}')
"
```

## What to Check

### ✅ Success Criteria

- [ ] All imports work (no `ImportError` or `ModuleNotFoundError`)
- [ ] Evaluation scripts can at least show help (no syntax errors)
- [ ] Critical tests pass
- [ ] No references to `LayeredEntityValidator` in code (except historical docs)
- [ ] Essential documentation files exist
- [ ] Removed files are actually gone

### ⚠️ If Tests Fail

1. **Import errors**: Check that `LayeredEntityValidator` references are removed
2. **Evaluation script errors**: Verify they use `TieredDecisionSystem` correctly
3. **Test failures**: Check that test expectations match new code
4. **Missing files**: Verify files weren't accidentally deleted

## Pre-Commit Checklist

Before committing, ensure:

- [ ] `./test_changes.sh` passes (or manual tests pass)
- [ ] No broken imports
- [ ] Critical tests pass
- [ ] Documentation is accurate
- [ ] No references to removed code (except in historical context)

## Next Steps After Testing

If all tests pass:

1. **Review changes**: `git diff` to see what changed
2. **Commit**: `git add . && git commit -m "Remove LayeredEntityValidator, migrate to TieredDecisionSystem, cleanup docs"`
3. **Push**: `git push`

If tests fail:

1. **Fix issues**: Address any import errors or test failures
2. **Re-test**: Run `./test_changes.sh` again
3. **Repeat**: Until all tests pass
