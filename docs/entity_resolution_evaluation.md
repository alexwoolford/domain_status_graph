# Entity Resolution AI Audit

This document describes the process for evaluating entity resolution quality using AI-assisted labeling.

## Important Caveat

**This produces AI-labeled evaluation data, not human-verified ground truth.**

The labels reflect GPT-5.2-Pro's judgment, which is highly accurate but not infallible. For production validation, use the `spot-check` command to export samples for human review.

## AI Audit Dataset

**Location:** `data/er_ai_audit.csv`

### Current Statistics (January 2026)

| Metric | Count | % |
|--------|-------|---|
| ✓ Correct | 275 | 53.6% |
| ✗ Incorrect | 196 | 38.2% |
| ? Ambiguous | 42 | 8.2% |
| **AI-Estimated Precision** | | **58.4%** |

### Common Error Patterns Identified by AI

1. **Generic words matching tickers/names**
   - "Target" (goal) → Target Corp (TGT)
   - "MA" (Marketing Authorization) → Mastercard (MA)

2. **Exchange/venue mentions**
   - "Nasdaq" (exchange listing) → Nasdaq Inc (NDAQ)

3. **Wrong relationship type**
   - Partner labeled as competitor
   - Supplier labeled as customer

4. **Biographical references**
   - "Mr. Smith serves as director of Microsoft" (career mention, not business relationship)

5. **Corporate structure**
   - Subsidiary/affiliate relationships mislabeled as supplier/customer

## Unified CLI Tool

All operations use: `scripts/er_ai_audit.py`

### Workflow

```bash
# Step 1: Extract random samples from Neo4j
python scripts/er_ai_audit.py sample --count 200 --append

# Step 2: Label samples with AI (parallel, ~10x faster)
python scripts/er_ai_audit.py label --concurrency 10

# Step 3: Evaluate current filters against AI-labeled data
python scripts/er_ai_audit.py evaluate

# Step 4: Export samples for human spot-check verification
python scripts/er_ai_audit.py spot-check --count 50

# View dataset statistics
python scripts/er_ai_audit.py stats
```

### Commands

| Command | Description |
|---------|-------------|
| `sample` | Extract random relationships from Neo4j graph |
| `label` | AI-label samples using GPT-5.2-Pro (parallel) |
| `evaluate` | Test filters/verifiers against AI labels |
| `spot-check` | Export samples for human verification |
| `stats` | Show dataset statistics |

## Human Verification (Spot-Check)

For production quality assurance, export a sample for human review:

```bash
python scripts/er_ai_audit.py spot-check --count 50 --focus mixed
```

This creates `data/er_spot_check.csv` with columns:
- Original data (source, target, context, etc.)
- `ai_label` - The AI's judgment
- `ai_reasoning` - The AI's explanation
- `human_label` - **Empty, for you to fill**
- `human_notes` - **Empty, for your notes**

### Instructions

1. Open `data/er_spot_check.csv` in a spreadsheet
2. For each row, read the `context` and review the `ai_label`
3. Fill `human_label` with: `correct`, `incorrect`, or `unsure`
4. Add any notes in `human_notes`
5. Calculate agreement rate: % where `human_label == ai_label`

**If agreement is >90%**, the AI labels are trustworthy for evaluation purposes.

## Layered Validation Approach

The current entity resolution uses a layered validation approach:

1. **Embedding similarity** (catches semantic mismatches)
2. **Biographical filter** (catches career/director mentions)
3. **Relationship verifier** (catches wrong relationship types)
4. **Exchange filter** (catches stock exchange references)

### Evaluation Results

On held-out test set (never used for tuning):

| Approach | Precision | Improvement |
|----------|-----------|-------------|
| Baseline (no validation) | 50% | - |
| Layered validation | 66.7% | +16.7pp |

## Files

| File | Description |
|------|-------------|
| `data/er_ai_audit.csv` | Main AI-labeled dataset |
| `data/er_spot_check.csv` | Human verification export |
| `scripts/er_ai_audit.py` | Unified CLI tool |
| `scripts/evaluate_layered_validator.py` | Tiered decision system evaluation (uses TieredDecisionSystem) |
