# Repository Guidelines

## Project Structure & Module Organization
- `lib/`: TypeScript CDK stack (`hubspot-analytics-stack.ts`).
- `lambda/pythonsrc/`: Python Lambda code (entry `app.py`, task handlers in `functions/`).
- `sql/`: Athena view DDL for analytics (activity volume, KPIs, cohorts).
- `test/`: Jest tests for CDK constructs (`*.test.ts`).
- `bin/`: CDK app entry (`hubspot-analytics` CLI).
- `docs/`, `examples/`, `google-scripts/`: Reference material and prior GAS logic.

## Build, Test, and Development Commands
- `npm run build`: Compile TypeScript (outputs to `lib/` and `bin/`).
- `npm run watch`: Recompile on change.
- `npm test`: Run Jest tests in `test/`.
- `npm run format:check` | `npm run format`: Prettier check/fix.
- `npx cdk synth` | `npx cdk diff` | `npx cdk deploy`: CDK workflow. Set env via `.env` (copy `.env.example`).

## Coding Style & Naming Conventions
- TypeScript: 2 spaces, single quotes, semicolons, width 80 (see `.prettierrc`).
- TS naming: classes `PascalCase`, functions/vars `camelCase`, files kebab-case (e.g., `hubspot-analytics-stack.ts`).
- Python: `snake_case` modules and functions.
- Prefer small, focused modules; keep CDK constructs in `lib/` and Lambda logic under `lambda/pythonsrc/`.

## Testing Guidelines
- Framework: Jest with `ts-jest` (`jest.config.js`).
- Location: `test/`; name files `*.test.ts`.
- Run locally: `npm test`.
- Add tests when modifying CDK resources (e.g., validate outputs or props).

## Commit & Pull Request Guidelines
- Commits: Follow Conventional Commits (`feat:`, `fix:`, `chore:`, etc.), as in git history.
- PRs: Include purpose, linked issues, and a short summary of changes.
  - For infra changes: paste `npx cdk diff` output.
  - For SQL/Athena changes: note affected views and example query/expected result.
  - For Lambda changes: specify `TASK` handlers affected and S3 paths written (e.g., `curated/{table}/dt=YYYY-MM-DD/`).

## Security & Configuration Tips
- Never commit secrets. HubSpot token is read from AWS Secrets Manager (`HubspotToken`).
- Configure environment in `.env` (`CDK_ENVIRONMENT`, `SNS_EMAIL_RECIPIENTS`, optional `START_DATE`).
- AWS credentials and region come from your environment or `.env` when synthesizing/deploying.
- Buckets, crawlers, and schedulers are created by CDK; avoid hardcoding ARNs/regions in code.

