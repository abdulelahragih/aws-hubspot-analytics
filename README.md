# HubSpot Analytics Data Platform (CDK + Lambda + S3 + Glue + Athena)

This project ingests HubSpot data into an AWS data lake and exposes it for analytics and dashboards.

## Architecture summary

- Lambda (Python, container image)
  - Handlers: `deals`, `activities`, `owners`, `contacts`, `companies` (single image, multiple functions via `TASK` env)
  - Fetches HubSpot data (deals, activities across emails/calls/meetings/tasks/notes/communications, owners, contacts, companies)
  - Normalizes and writes partitioned Parquet (Snappy) to S3 under `curated/{table}/dt=YYYY-MM-DD/`
  - Secrets Manager stores HubSpot token; the Lambda caches tokens per container with TTL
  - Built-in backoff and rate-limit handling for HubSpot Search API (5 RPS, 200/page, <3k body, <=10k results)

- S3 (Data Lake)
  - Curated Parquet tables per entity: `deals`, `activities`, `owners`, `contacts`, `companies`
  - Partitioned by `dt` for efficient reads

- Glue (Data Catalog + Crawlers)
  - Crawlers register schemas/partitions for curated tables in the Glue Data Catalog

- Athena (SQL layer)
  - Queries over curated Parquet tables; views to support KPIs, weekly activity, cohorts, and chart-friendly datasets
  - Feeds QuickSight dashboards

## Relation to Google Apps Script (.gs)

- `activities.gs`: Full activity report (matrix by owner/type) with optional detailed sheets.
- `activity-kpis.gs`: Chart-friendly Activity Volume per Rep + Weekly per Owner (last 3 weeks).
- `kpis.gs`: Deal KPIs (monthly/weekly metrics, averages Op→Prep, Prep→Sent, Sent→Won, sales cycle, cohorts).
- `fetchDealData.gs`: Exports “Raw Deal Data” to Sheets from HubSpot CRM v3.
- `visualize.gs`: Visualization helpers (charts setup).

AWS reproduces these by ingesting raw objects and modeling curated Parquet tables; Athena views implement equivalent aggregations for QuickSight.

## Differences between key Apps Script functions

- `runHubSpotActivityReport` (in `activities.gs`)
  - Prompts for date range and target sheet name
  - Fetches owners, engagements (enhanced), contacts created/worked, deals created
  - Aggregates to a full matrix by owner and activity type; can write detailed sheets per type

- `runActivityVolumeReport` (in `activity-kpis.gs`)
  - Prompts for date range; auto-names the output sheet
  - Reuses activity fetchers and generates a chart-friendly table optimized for stacked column charts
  - Includes a Weekly Activity per Owner variant (last 3 weeks)

## Weekly report

- GAS computes last 3 week windows and aggregates per owner/type/week.
- In AWS we can compute the same via Athena views on `activities` (group by week_start, owner, activity_type) or a small Lambda aggregator.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template
