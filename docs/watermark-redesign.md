# Watermark recovery redesign (proposed, not implemented)

Current extraction advances orders_pipeline before downstream success and uses wall-clock paths. The design below is a separate follow-up, not a claim about current behavior.

1. Pass Airflow's logical batch date to extraction, raw validation, landing and every Bronze/Silver job. Store FX inputs under that batch too; retries reuse those inputs rather than fetching new rates.
2. Create a batch ledger keyed by pipeline and batch date with lower/upper watermarks, input manifest, status and row counts. Before extracting, acquire a per-pipeline lock and persist fixed bounds. Use a consistent source snapshot and a compound cursor such as (updated_at, order_id); order_date alone misses old-order updates. Add an orders updated_at column and maintain it on writes, or use CDC. A lookback/overlap plus dedup is needed for late commits.
3. Extract within the saved bounds and freeze all six input files and FX data. Record a manifest only after every file is durable. On retry, reuse the manifest and bounds, including empty batches. Do not advance the committed cursor yet.
4. Run transformations, load accepted snapshots into an isolated Snowflake candidate schema, and use dbt build there. Preserve batch-partitioned rejects and reconcile accepted + rejected counts with inputs.
5. After successful validation, promote the candidate through a controlled consumer schema/view pointer. Persist a promotion receipt, then compare-and-swap the committed cursor from the expected lower bound to the upper bound in PostgreSQL. Keep max_active_runs=1 as an additional guard.
6. There is no distributed transaction across Snowflake, S3 and PostgreSQL: a crash after promotion but before cursor commit must be reconciled using the receipt. Promotion and finalization must be repeatable. Never reset the watermark blindly, and do not let a historical replay move the current cursor backwards.

Acceptance checks: crash before/after input manifest, dbt failure, crash after promotion, same-date retry, concurrent trigger, empty batch, late update, and historical replay. Each must preserve counts, reuse fixed inputs and advance the cursor only after validated publication.
