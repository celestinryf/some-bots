# Sprint 2 Manual Validation: Email Example Format

Date: 2026-03-19

Scope:
- Close out Sprint 2 item: "Manual validation: compare output to the email example format."
- Compare currently generated recommendation output fields against the example digest format in `docs/production-plan.md` (Phase 4.1, lines 376-395).
- This is a structure/field validation, not delivery validation (email delivery is Sprint 3).

## Source Of Truth

- Example format block: `docs/production-plan.md` lines 376-395.
- Sprint 2 checklist item: `docs/production-plan.md` line 712.

## Validation Results

### Recommendation Row Mapping

1. `MIAMI — HIGH 71–72°F`
- Available now: city + market type + bracket low/high are present in DB-backed runtime records.
- Evidence:
  - `KalshiMarket.city_id`, `market_type`, `bracket_low`, `bracket_high` in `shared/db/models.py`.

2. `Kalshi: 54%`
- Available now: `recommendation.kalshi_probability`.
- Evidence:
  - Assigned in recommendation cycle: `services/prediction-engine/src/main.py` lines 657-658.

3. `Model: 12% → BUY NO (gap: -42%) Risk: 3/10`
- Available now: `direction`, `model_probability`, `gap`, `risk_score`.
- Evidence:
  - Assigned in recommendation cycle: `services/prediction-engine/src/main.py` lines 656-661.

4. `Models: NWS ... | VC ... | PW ... | OWM ... | Spread: 3°`
- Available now:
  - Per-source temps in prediction distribution `source_temps`.
  - Spread in risk factors via `forecast_spread`.
- Evidence:
  - Source temps read from distribution: `services/prediction-engine/src/main.py` lines 454-461.
  - Spread computed in risk payload: `services/prediction-engine/src/main.py` lines 473-477.
  - Risk factors persisted on recommendation: `services/prediction-engine/src/main.py` lines 662-664.

5. Entry-price lock for recommendation time
- Available now and persisted for paper trade linkage.
- Evidence:
  - Candidate entry price captured and passed into fixed paper trade creation:
    - candidate creation uses ask price: `services/prediction-engine/src/main.py` lines 394-403
    - paper trade creation with entry price: `services/prediction-engine/src/main.py` lines 665-669.

### Digest-Level Lines

1. Header line (`Kalshi Bot · Mar 09 · 11:55 PM`) and `X markets shown`
- Partially available now:
  - Notification runtime currently scans and logs recommendation count and user count.
  - Final formatted digest rendering is still Sprint 3 scope.
- Evidence:
  - `notification_digest_scanned` includes `recommendation_count`, `user_count`:
    `services/notification-service/src/main.py` lines 62-66.

2. `Paper Trading Stats ...` and `Portfolio ...`
- Not fully implemented as formatted digest output yet (Sprint 3 scope).
- Sprint 2 already creates fixed paper trades automatically per recommendation.

## Conclusion

Sprint 2 recommendation output contains all core fields needed to render the per-market lines in the example format:
- city/market/bracket
- Kalshi probability
- model probability
- direction
- gap
- risk
- per-source temperatures
- spread

Email rendering/sending and final digest text composition remain Sprint 3 by plan design.

Status: Sprint 2 manual format-comparison validation complete.
