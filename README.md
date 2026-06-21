# Finance Fraud Detection SQL Pipeline

This project builds a complete PostgreSQL data pipeline around a credit card transaction dataset, using a Bronze → Silver → Gold layered architecture to take three raw, related CSV files and turn them into a clean, validated, query-ready relational database.

The source data — customer profiles, card details, and transaction history — comes from the [Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets) on Kaggle. What makes this dataset a good fit for this kind of project is that it's already relational: customers, cards, and transactions are connected through real, verifiable keys, rather than arriving as a single flat table. That structure makes it possible to practice genuine normalization — building separate, properly keyed tables instead of forcing everything into one — and to validate every relationship with actual SQL checks rather than assuming the source data is trustworthy.

The pipeline moves through three distinct stages. **Bronze** loads the raw CSVs into Postgres exactly as they are, with no cleaning and no enforced constraints — its only job is to faithfully mirror the source, flaws included. **Silver** takes that raw data and applies real transformation logic: stripping currency symbols, breaking apart malformed date strings, converting text flags into proper booleans, fixing a column that violated basic normalization rules, and — only once uniqueness and referential integrity were explicitly confirmed — adding real primary and foreign key constraints. **Gold** sits on top of the validated silver tables as a set of analytical views, each one built to answer a specific question about spending behavior, merchant patterns, and fraud-relevant anomalies, using aggregation, window functions, CTEs, and time-series logic.

Throughout the build, the goal was to treat every non-obvious decision as a decision — testing assumptions before acting on them, documenting the reasoning behind each typing and cleaning choice, and explicitly flagging the handful of cases where no clean technical answer existed and a judgment call had to be made instead.

---

## 🛠️ Tools & Stack

- **Database:** PostgreSQL
- **Client:** pgAdmin
- **Source data:** Kaggle — [`computingvictor/transactions-fraud-datasets`](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)
- **Version control:** GitHub

The dataset arrives as three separate CSV files — customer profiles, card details, and transaction history — connected through shared identifiers (`client_id`, `card_id`) rather than as a single combined export. That structure is what made it possible to build out a proper multi-table schema instead of a single flat table, and it's reflected in how the project is organized: each layer of the pipeline (bronze, silver, gold) lives in its own Postgres schema, with dedicated setup and load scripts per layer rather than one monolithic script handling everything.

---

## 🏗️ Schema Design — How the Layers Connect

### What changes at each stage

**Bronze** is an unaltered, column-for-column mirror of the three source CSVs. No keys are enforced and no values are reformatted — every column defaults to `TEXT` unless the raw value is already valid input for a native type with no changes required. This is a deliberate constraint, not an oversight: a `PRIMARY KEY` or `FOREIGN KEY` actively rejects rows that violate it, and bronze's job is to accept the source exactly as delivered, including any flaws it contains. For example, `credit_limit` arrives as `"$24295"` — a string, not a number — and is loaded into bronze exactly as written:

```sql
CREATE TABLE bronze.cards_data (
    ...
    credit_limit TEXT,
    ...
);
```

**Silver** is where every transformation bronze deliberately deferred actually happens, and where constraints are introduced for the first time — but only after each one was validated against the real data rather than assumed. Currency fields are stripped of their `$` prefix and cast to `NUMERIC`:

```sql
CAST(TRIM(REPLACE(credit_limit, '$', '')) AS NUMERIC) AS credit_limit
```

Malformed date strings (`MM/YYYY`, with no day component) are split into separate integer columns rather than forced into a `DATE` that would require fabricating a day value:

```sql
CAST(SPLIT_PART(expires, '/', 1) AS INT) AS expire_month,
CAST(SPLIT_PART(expires, '/', 2) AS INT) AS expire_year
```

Text-based boolean flags are converted to genuine `BOOLEAN` values:

```sql
CASE WHEN has_chip = 'YES' THEN TRUE ELSE FALSE END AS has_chip
```

And only once uniqueness and referential integrity were confirmed with dedicated validation queries did `PRIMARY KEY` and `FOREIGN KEY` constraints get added to the table definitions themselves:

```sql
CREATE TABLE silver.cards_data (
    id          TEXT PRIMARY KEY,
    client_id   TEXT NOT NULL REFERENCES silver.users_data(id),
    ...
);
```

**Gold** sits on top of the validated silver tables as a set of views — not tables — since a view stores no data of its own and re-runs its underlying query every time it's accessed. That means refreshing silver via `CALL silver.load_silver_tables()` automatically updates every gold view's results, with no separate gold-layer rebuild step needed.

### How the silver tables relate to each other

The three source files map naturally onto a dimension/fact pattern, and that mapping was confirmed with actual SQL checks — uniqueness tests and orphan checks — rather than taken on faith:

```
silver.users_data  (id PK)
        │
        ├──< silver.cards_data  (id PK, client_id FK → users_data.id)
        │
        └──< silver.transactions_data
                  (id PK,
                   client_id FK → users_data.id,
                   card_id   FK → cards_data.id)
                          │
                          └──< silver.transaction_errors
                                    (transaction_id FK → transactions_data.id)
```

`users_data` sits at the top — nothing points further back than that. `cards_data` belongs to a customer. `transactions_data` is the fact table, carrying two foreign keys at once. `transaction_errors` exists because the original `errors` column held multiple comma-separated error codes in a single cell, which isn't valid under First Normal Form — that table is the fix.

Bronze carries none of these constraints, for the same reason stated above: rejecting malformed data would mean bronze stopped doing its job of mirroring the source. Every constraint shows up for the first time in silver, and only after the matching validation query came back clean.

---

## 📋 Table Organization

### Bronze

| Table | What it holds | Key columns |
|---|---|---|
| `bronze.users_data` | Unmodified customer demographics | `id` |
| `bronze.cards_data` | Unmodified card details | `id`, `client_id` |
| `bronze.transactions_data` | Unmodified transaction history | `id`, `client_id`, `card_id` |

Every column defaults to `TEXT`. A column only gets a native type if the raw text is already a valid value for that type with nothing to strip or reformat — which is why card numbers, CVVs, zip codes, and merchant category codes all stay text: they read like numbers, but they're identifiers, and casting them risks quietly losing a leading zero.

### Silver

| Table | What it holds | Primary key | Foreign keys |
|---|---|---|---|
| `silver.users_data` | Cleaned customer records | `id` | none |
| `silver.cards_data` | Cleaned card records | `id` | `client_id` → `users_data.id` |
| `silver.transactions_data` | Cleaned transaction records | `id` | `client_id` → `users_data.id`, `card_id` → `cards_data.id` |
| `silver.transaction_errors` | One row per error code per transaction | none | `transaction_id` → `transactions_data.id` |

### Gold

| View | Question it answers | Technique |
|---|---|---|
| `gold.mcc_spend_summary` | Total and average transaction amount per merchant category | Aggregation |
| `gold.top_spenders_ranked` | Highest-spending customers, ranked and percentiled | Window function over an aggregate |
| `gold.monthly_transaction_trend` | Month-over-month transaction volume and dollar amount | Time-series aggregation |
| `gold.customer_running_total` | Each customer's cumulative spend over time | Window function (running total) |
| `gold.merchant_state_error_rate` | Error rate by merchant state vs. the overall rate | CTE + conditional aggregation |
| `gold.card_brand_type_avg_amount` | Card brand/type combinations with the highest average amount | Join + aggregation |
| `gold.customer_activity_span` | Gap between each customer's first and most recent transaction | Date arithmetic + aggregation |
| `gold.customer_outlier_transactions` | Customers with a transaction over 5x their own average | CTE + self-relative filtering |

---

## 🥉 Bronze Layer

**What came in:** `users_data.csv`, `cards_data.csv`, `transactions_data.csv`, already linked by real keys rather than something that had to be inferred.

**Setting up the schemas:** all three schemas get dropped and recreated together at the start of the build (`DROP SCHEMA IF EXISTS ... CASCADE`, then `CREATE SCHEMA`). This is meant to be run repeatedly during development without leaving behind partial state from a prior attempt — it works because the source files are a fixed snapshot, not something that grows over time. This same approach would be the wrong call in a system where new data arrives continuously; that kind of pipeline needs an append-only design instead, with audit columns like `source_file`/`ingested_at` and an `INSERT ... ON CONFLICT DO NOTHING` to avoid reloading the same row twice.

**The typing rule:** default to `TEXT`; only use a native type when the source value needs zero changes to be valid in that type. That single rule is why `card_number`, `cvv`, `zip`, `merchant_id`, and `mcc` are all `TEXT` even though every character in them is a digit — none of them represent a quantity, so there's nothing to gain from a numeric type and a real risk of losing a leading zero if cast.

**Getting the data in:** plain `COPY ... DELIMITER ',' CSV HEADER`, no cleanup applied during the load itself.

**Checking it landed correctly:** confirmed row counts were non-zero across all three tables, confirmed `amount` still carried its `$` prefix unchanged, and confirmed `date` parsed into a real `TIMESTAMP` — though it later turned out every single value had `:00` seconds, more on that below.

📄 [`/scripts/bronze_layer/bronze_setup.sql`](./scripts/bronze_layer/bronze_setup.sql)

---

## 🔍 Data Quality Testing — Bridging Bronze and Silver

Before writing a single transformation, every column across all three bronze tables got tested on its own: nulls, stray whitespace, format checks, duplicate values, plausible ranges, and — for the foreign key columns — whether every reference actually pointed at something real in the parent table.

What turned up:

- **Keys checked out completely.** No duplicate or missing `id` in any table, and no `client_id`/`card_id` pointing at a row that doesn't exist. This is the reason real `PRIMARY KEY`/`FOREIGN KEY` constraints were possible in silver at all.
- **CVV lengths weren't consistent.** Some records have a 1- or 2-digit CVV, which doesn't match the real-world standard of 3 digits (most networks) or 4 (Amex). Since there's no way to know what digit was actually missing, this got flagged as a decision for project leadership rather than silently patched with an assumed zero. The call: leave it alone, write down the limitation.
- **`errors` broke First Normal Form.** One cell could contain several error codes separated by commas (`"Bad CVV,Insufficient Balance"`). Fixed by pulling that data into its own `silver.transaction_errors` table, one row per code, with a simple `had_errors` flag left on the transactions table for quick filtering.
- **`date` only has minute-level resolution.** Every timestamp ends in `:00` seconds with no exceptions — the column is typed `TIMESTAMP`, but the underlying data was never recorded down to the second. Noted as a limitation of the source, not something to repair.
- **`zip` had a leftover `.0`** from being stored as a float somewhere upstream. Removed with `SPLIT_PART`, but the result stays `TEXT` rather than being cast to a number, for the same leading-zero reason as everything else.

📄 [`cards_data_quality_tests.sql`](./scripts/testing/cards_data_quality_tests.sql) · [`transactions_data_quality_tests.sql`](./scripts/testing/transactions_data_quality_tests.sql) · [`users_data_quality_tests.sql`](./scripts/testing/users_data_quality_tests.sql)

---

## 🥈 Silver Layer

This is where every fix that bronze deliberately skipped actually happens:

- **Currency columns** (`credit_limit`, `amount`, `per_capita_income`, `yearly_income`, `total_debt`) — the `$` is stripped and the result cast to `NUMERIC`. `FLOAT` was ruled out (rounding drift), and a fixed-scale `DECIMAL` was ruled out too, since no consistent scale was confirmed across every row. `amount` keeps its sign — a negative value means a refund or reversal, which matters for fraud analysis and shouldn't be flattened away.
- **`MM/YYYY` date strings** (`expires`, `acct_open_date`) — broken into separate `*_month` and `*_year` integer columns instead of forced into a `DATE`, since a real date would need a day value that the source never provided.
- **Text flags** (`has_chip`, `card_on_dark_web`) — converted to actual `BOOLEAN` values with a `CASE` expression.
- **Inconsistent casing** (`merchant_city`) — run through `INITCAP()` across the entire column, which also happens to fix the one all-caps `'ONLINE'` value as a side effect of the broader cleanup.
- **The `errors` split** — extracted with `STRING_TO_ARRAY` + `UNNEST` into `silver.transaction_errors`.
- **Constraints, finally.** `PRIMARY KEY` on every `id`, and the `FOREIGN KEY`s that mirror the dimension/fact relationships — added only after the validation pass confirmed they wouldn't reject real data.

A single stored procedure, `silver.load_silver_tables()`, handles all four tables in one call, using a full truncate-and-reload — there's no incremental data here, so rebuilding from scratch every time is simpler than tracking partial updates. Because the tables reference each other, the truncation step uses `CASCADE`, so clearing a parent table automatically clears whatever depends on it in the right order, without the procedure needing a manual update every time a new dependent table gets added.

```sql
CALL silver.load_silver_tables();
```

📄 [`/scripts/silver_layer/silver_setup.sql`](./scripts/silver_layer/silver_setup.sql)

---

## 🥇 Gold Layer

The gold layer answers eight analytical questions against the validated silver tables — straight aggregation, window functions, CTEs, conditional aggregation, and time-series logic — framed around the dataset's fraud-detection premise: spend behavior, merchant patterns, error rates, and outlier detection.

Each one is built as a view rather than a table, so it always reflects the current state of silver with no separate refresh step. A few of the more notable design decisions:

- **`gold.top_spenders_ranked`** aggregates transactions down to one row per customer *before* ranking — ranking individual transactions directly would answer a different question than "which customers spend the most."
- **`gold.monthly_transaction_trend`** groups on the full truncated date (month *and* year together), not just the month number, since the dataset spans multiple years and a month-number-only grouping would merge every January across every year into one row.
- **`gold.merchant_state_error_rate`** computes the overall error rate once in a CTE, then safely cross-joins it against the full table — safe specifically because the CTE always produces exactly one row, so the join doesn't multiply the row count.
- **`gold.customer_outlier_transactions`** uses a self-relative threshold (5x a customer's *own* average) rather than a fixed dollar amount, so the same logic works fairly across customers with very different normal spending levels — directly tied to the dataset's fraud-detection theme.

A few decisions remain open rather than fully resolved: whether "total spend" across several views should reflect net flow (refunds included) or gross charges, and how the running-total view should treat multiple transactions from the same customer that land in the same minute.

📄 [`/scripts/gold_layer/gold_views.sql`](./scripts/gold_layer/gold_views.sql)

---

## ✅ Summary — End-to-End Pipeline

```sql
-- 1. Bronze: schemas + raw load
\i scripts/bronze_layer/bronze_setup.sql

-- 2. Silver: table structure + full refresh
\i scripts/silver_layer/silver_setup.sql
CALL silver.load_silver_tables();

-- 3. Gold: analytical views
\i scripts/gold_layer/gold_views.sql
```

Once all three layers are built, refreshing the entire analytical layer after any bronze data change requires only one call:

```sql
CALL silver.load_silver_tables();
```

Every gold view reads from the refreshed silver tables automatically, since views recompute their query on every access rather than storing a static snapshot — there's nothing to manually rebuild downstream of silver.

This project moved through the same discipline at every stage: confirm before transforming, transform before constraining, and document the calls that didn't have a clean technical answer rather than letting them slide by unnoticed. The dataset's reliable keys made genuine normalization possible — a customer dimension, a card dimension, a transaction fact table, and a properly split-out error table — validated with actual queries at each step rather than assumed from the dataset's description. The result is a small but complete pipeline: raw CSVs in, eight fraud-relevant analytical views out, with every transformation and every judgment call along the way traceable back to a specific test and a specific decision.

---

Scripts referenced throughout this document live under [`/scripts`](./scripts): `/scripts/bronze_layer`, `/scripts/silver_layer`, `/scripts/testing`, `/scripts/gold_layer`.
