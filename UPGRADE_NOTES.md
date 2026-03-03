# UPGRADE NOTES (v0.2.0)

This release introduces hardening for analytical heuristics, a cleaner API-based upload mechanism, and deterministic policy matrix handling in the decision engine.

## New Features & Improvements

### 1. Hardened Analytical Heuristics (Spike/Reversal)
- **Objective**: Robust detection of "spikes" and "reversals" in GST and Bank data to identify potential circular trading.
- **Implementation**: 
  - Data is resampled to calendar months.
  - Spike detection uses robust outliers (MAD/IQR) and relative change thresholds.
  - Reversal detection identifies offset follow-ups within a configurable window.
  - A `circular_trading_risk` score (0-100) is computed and included in `signals.json`.
- **Config**: New `signals:` block in `base.yaml`.
  - `spike.z_threshold`: Sensitivity for outliers.
  - `reversal.window_k`: Look-ahead window for offsets.

### 2. API-Based Uploads
- **Objective**: Decouple Streamlit from direct filesystem access and centralize input management.
- **Endpoints**:
  - `POST /jobs/{job_id}/uploads`: Accepts `gst_returns`, `bank_transactions`, and multiple `pdfs`.
  - `GET /jobs/{job_id}/inputs`: Lists uploaded files with checksums.
- **Streamlit**: Now uses these endpoints for "Local Uploads" mode. Direct writes to `outputs/jobs/` from the UI are removed.

### 3. Deepened Disambiguation & Policy Matrix
- **Objective**: Improve entity matching in research and add deterministic overrides for decisions.
- **Research**:
  - Entity profiles created at `research/entities/profile.json`.
  - Improved fuzzy matching using Jaccard token similarity.
  - `entity_confidence` score (0.0-1.0) and `legal_hits` count added to findings.
- **Decision Engine**:
  - New `policy_matrix` in `base.yaml`.
  - Deterministic rules can trigger `REFER` based on risk signals (e.g., low confidence, high circular trading risk, or legal hits).

## Config Migration

Add the following to your `config/base.yaml`:

```yaml
signals:
  spike:
    method: "mad"
    z_threshold: 3.0
    rel_threshold: 0.6
    min_points: 6
    rolling_window: 6
  reversal:
    window_k: 2
    offset_ratio_min: 0.7
  weights:
    spike: 10
    reversal: 25
    cap: 100

decision:
  policy_matrix:
    - when:
        entity_confidence_lt: 0.6
        adverse_count_gte: 1
      action: "REFER"
      driver: "Low entity confidence with adverse media"
    - when:
        circular_trading_risk_gte: 60
      action: "REFER"
      driver: "Spike-reversal pattern suggests circular trading"
    - when:
        legal_hits_gt: 0
      action: "REFER"
      driver: "Open legal proceedings detected"
```

## Backward Compatibility
- Existing jobs will still run. 
- If `policy_matrix` or `signals` config is missing, the system defaults to legacy behavior.
- `signals.json` schema is extended but existing fields (mismatch) remain.
