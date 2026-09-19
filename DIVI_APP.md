# Divi application layer

The original ETL continues to ingest personal data into BigQuery. This layer is
an independent multi-user application: it does not read the ETL tables until a
future, user-scoped data-linking flow exists.

## Local frontend

```bash
cd divi_web
cp .env.example .env.local
npm install
npm run dev
```

Without `VITE_API_URL`, the UI exposes the complete demo flow so visual work can
continue offline. With an API URL and a Cognito access token in
`localStorage.divi_access_token`, actions call the authenticated API.

## Deploy prerequisites

Set these Terraform variables through CI secrets, never in source:

- `divi_frontend_origin`. The CI workflow derives
  `divi_frontend_bucket_name` as `divi-expenses-frontend-<AWS account ID>` so
  Terraform stays non-interactive and the S3 bucket name remains globally
  unique.
- Google OAuth client ID/secret (optional; email authentication works without it)
- Mercado Pago application token, webhook secret, Premium plan ID and OAuth
  client ID/secret/redirect URI.

`divi_app_infra.tf` provisions the Cognito pool, DynamoDB store, HTTP API,
Secrets Manager secret, Lambda and private S3 + CloudFront SPA distribution.
Build the API image with `app_api/app_api.dockerfile`, tag it `app_api-latest`
in the existing `etl-expenses` ECR repository, then upload `divi_web/dist` to
the Terraform-created frontend bucket and invalidate CloudFront.

## Payment configuration

Create the ARS 3,000/month Premium `preapproval_plan` in Mercado Pago with a
60-day free trial, and assign its ID to `divi_mp_premium_plan_id`. Each pool
organizer must complete Mercado Pago OAuth before opening a pool; contributions
are created with that organizer's token, so Divi never holds the money.
