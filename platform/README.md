# platform — producer project

The **producer** in the retail Mesh. Owns the raw sources and transforms them
into governed, documented, tested models. Publishes four **public, contracted**
interface models that the `marketing` and `finance` consumer projects build on.

## What's here

| Layer | Path | Materialization | Notes |
|-------|------|-----------------|-------|
| Sources | `models/staging/retail/_retail__sources.yml` | — | Fivetran → Unity Catalog; source freshness on `_fivetran_synced` |
| Staging | `models/staging/retail/` | view | rename, cast, drop soft-deletes |
| Intermediate | `models/intermediate/` | ephemeral | explode line items, enrich + dedup customers |
| Marts | `models/marts/` | table (`fct_sales`: incremental merge) | public interface + internal marts |
| Snapshot | `snapshots/` | SCD2 (check strategy) | `customers_snapshot` |
| Semantic | `semantic_models/` | — | MetricFlow metrics |
| Unit tests | `models/intermediate/_int__unit_tests.yml` | — | explode + SCD dedup logic |

## Public interface (consumed across the Mesh)

`access: public`, `contract.enforced: true`, group `retail`, with Unity Catalog
PK/FK constraints:

- `dim_customers` — PK `customer_business_key`
- `fct_sales` — PK `sales_order_line_id`, FK `customer_id` → `dim_customers`
- `fct_orders` — PK `order_id`
- `dim_loyalty_segments` — PK `loyalty_segment_id`

`fct_support_tickets` is `protected` (internal to this project).

## Run order

```bash
dbt deps                          # install dbt_utils
dbt build                         # staging → intermediate → marts (+ snapshot, tests)
dbt test --select test_type:unit  # unit tests only (no warehouse data needed)
dbt source freshness              # check Fivetran load recency
dbt docs generate                 # catalog with persisted Unity Catalog comments
```

Set the raw data location to match your Fivetran destination (defaults are
`Databricks_LondonSALab_2026` / `hicham_babahmed_retail` in `dbt_project.yml`):

```bash
dbt build --vars '{raw_catalog: Databricks_LondonSALab_2026, raw_schema: hicham_babahmed_retail}'
```

## Deploying for the Mesh

Consumers resolve `ref('platform', …)` from this project's **production
environment** publication artifact in the dbt platform. Deploy `platform`
(run a production job) before the consumers can build. See the top-level
[`README.md`](../README.md), Module 5.
