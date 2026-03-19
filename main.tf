terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-2"
}

data "aws_caller_identity" "current" {}

# Leer el secreto de AWS Secrets Manager
data "aws_secretsmanager_secret_version" "gcp_ua_creds" {
  secret_id = "gcp_api_credentials"
}

data "aws_secretsmanager_secret_version" "gcp_sa_creds" {
  secret_id = "gcp_sa_api_credentials"
}

variable "GCP_PROJECT_ID" {
  description = "GCP Project ID"
  type        = string
  sensitive   = true
}

provider "google" {
  credentials = data.aws_secretsmanager_secret_version.gcp_sa_creds.secret_string
  project = "${var.GCP_PROJECT_ID}"
  region  = "us-central1"
}

########### 0. Definicion de Variables ###########

# Definimos las variables que van a utilizar algunos recursos para referenciar a las ARN
variable "AWS_ACCOUNT_ID" {
  description = "AWS Account ID"
  type        = string
  sensitive   = true
}

variable "AWS_REGION" {
  description = "AWS REGION"
  type        = string
  sensitive   = true
}

variable "EMAIL" {
  description = "email"
  type        = string
  sensitive   = true
}

variable "TELEGRAM_BOT_TOKEN" {
  description = "TELEGRAM_BOT_TOKEN"
  type        = string
  sensitive   = true
}

variable "OPENAI_API_KEY" {
  description = "OpenAI API Key"
  type        = string
  sensitive   = true
}

variable "CIFRADO_SECRET_MP" {
  description = "CIFRADO_SECRET_MP"
  type        = string
  sensitive   = true
}

variable "SNS_TOPIC" {
  description = "SNS_TOPIC"
  type        = string
  sensitive   = true
}

variable "TABSCANNER_API_KEY" {
  description = "TabScanner API Key for receipt OCR fallback"
  type        = string
  sensitive   = true
  default     = ""
}


variable "glue_database_name" {
  type    = string
  default = "etl_database"
}

variable "glue_crawler_name_market_tickets" {
  type    = string
  default = "market-tickets-crawler"
}

variable "glue_crawler_name_mp_reports" {
  type    = string
  default = "mp-reports-crawler"
}

variable "glue_crawler_name_bank_payments" {
  type    = string
  default = "bank-payments-crawler"
}

variable "glue_crawler_name_mp_transfers" {
  type    = string
  default = "mp-transfers-crawler"
}

variable "dynamodb_table_name" {
  type    = string
  default = "schema_cache"
}

########### 1. Buckets de S3 ###########
# 1.1 Bucket para PDF de Gmail
resource "aws_s3_bucket" "market_tickets" {
  bucket = "market-tickets"
  force_destroy = true
}

# 1.2 Bucket para Reportes de Mercado Pago
resource "aws_s3_bucket" "mp_reports" {
  bucket = "mercadopago-reports"
  force_destroy = true
}

# 1.3 Bucket para Gastos con tarjetas del banco
resource "aws_s3_bucket" "bank_payments" {
  bucket        = "bank-payments"
  force_destroy = true
}

# 1.4 Bucket para Tickets de Telegram (fotos de recibos)
resource "aws_s3_bucket" "telegram_receipts" {
  bucket        = "telegram-receipts"
  force_destroy = true
}

# 1.5 Bucket para Transferencias de Mercado Pago
resource "aws_s3_bucket" "mp_transfers" {
  bucket        = "mercadopago-transfers"
  force_destroy = true
}

########### 2. Repositorio ECR para las imágenes Lambda ###########
resource "aws_ecr_repository" "lambda_images" {
  name                 = "etl-expenses"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

########### 4. API Gateway ###########
# API Gateway principal
resource "aws_api_gateway_rest_api" "main_api" {
  name        = "main-api"
  description = "API Gateway único para Telegram Bot y mails de gastos del banco y de supermercado"
}

# Recurso /telegram_bot
resource "aws_api_gateway_resource" "telegram_bot_resource" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  parent_id   = aws_api_gateway_rest_api.main_api.root_resource_id
  path_part   = "telegram_bot"
}

# Método y Lambda para /telegram_bot
resource "aws_api_gateway_method" "telegram_bot_method" {
  rest_api_id   = aws_api_gateway_rest_api.main_api.id
  resource_id   = aws_api_gateway_resource.telegram_bot_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "telegram_bot_integration" {
  rest_api_id             = aws_api_gateway_rest_api.main_api.id
  resource_id             = aws_api_gateway_resource.telegram_bot_resource.id
  http_method             = aws_api_gateway_method.telegram_bot_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.ai_agent.invoke_arn
}

# Recurso /market_pdf
resource "aws_api_gateway_resource" "market_pdf_resource" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  parent_id   = aws_api_gateway_rest_api.main_api.root_resource_id
  path_part   = "market_pdf"
}

# Método y Lambda para /market_pdf
resource "aws_api_gateway_method" "market_pdf_method" {
  rest_api_id   = aws_api_gateway_rest_api.main_api.id
  resource_id   = aws_api_gateway_resource.market_pdf_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "market_pdf_integration" {
  rest_api_id             = aws_api_gateway_rest_api.main_api.id
  resource_id             = aws_api_gateway_resource.market_pdf_resource.id
  http_method             = aws_api_gateway_method.market_pdf_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.extract_data_gmail.invoke_arn
}

# Recurso /pdf_extractor
resource "aws_api_gateway_resource" "bank_pdf_extractor_resource" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  parent_id   = aws_api_gateway_rest_api.main_api.root_resource_id
  path_part   = "bank_pdf"
}

# Método y Lambda para /pdf_extractor
resource "aws_api_gateway_method" "bank_pdf_extractor_method" {
  rest_api_id   = aws_api_gateway_rest_api.main_api.id
  resource_id   = aws_api_gateway_resource.bank_pdf_extractor_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "bank_pdf_extractor_integration" {
  rest_api_id             = aws_api_gateway_rest_api.main_api.id
  resource_id             = aws_api_gateway_resource.bank_pdf_extractor_resource.id
  http_method             = aws_api_gateway_method.bank_pdf_extractor_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.extract_data_gmail.invoke_arn
}

# Recurso /mp_webhook
resource "aws_api_gateway_resource" "mp_webhook_resource" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  parent_id   = aws_api_gateway_rest_api.main_api.root_resource_id
  path_part   = "mp_webhook"
}

# Método y Lambda para /mp_webhook
resource "aws_api_gateway_method" "mp_webhook_method" {
  rest_api_id   = aws_api_gateway_rest_api.main_api.id
  resource_id   = aws_api_gateway_resource.mp_webhook_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "mp_webhook_integration" {
  rest_api_id             = aws_api_gateway_rest_api.main_api.id
  resource_id             = aws_api_gateway_resource.mp_webhook_resource.id
  http_method             = aws_api_gateway_method.mp_webhook_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.webhook_mp_report.invoke_arn
}

# Deployment y stage
resource "aws_api_gateway_deployment" "main_api_deployment" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id

  depends_on = [
    aws_api_gateway_integration.telegram_bot_integration,
    aws_api_gateway_integration.market_pdf_integration,
    aws_api_gateway_integration.bank_pdf_extractor_integration,
    aws_api_gateway_integration.mp_webhook_integration
  ]
}

resource "aws_api_gateway_stage" "main_api_stage" {
  deployment_id = aws_api_gateway_deployment.main_api_deployment.id
  rest_api_id   = aws_api_gateway_rest_api.main_api.id
  stage_name    = "prod"
}

resource "aws_lambda_permission" "allow_api_gateway_ai_agent" {
  statement_id  = "AllowAPIGatewayInvokeAiAgent"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ai_agent.function_name
  principal     = "apigateway.amazonaws.com"

  # Usamos el ARN del api_gateway unificado
  source_arn = "${aws_api_gateway_rest_api.main_api.execution_arn}/*/POST/telegram_bot"

  depends_on = [
    aws_api_gateway_rest_api.main_api,
    aws_lambda_function.ai_agent
  ]

  lifecycle {
    create_before_destroy = true
    ignore_changes = [source_arn]
  }
}

resource "aws_lambda_permission" "allow_api_gateway_market_mail_data_extractor" {
  statement_id  = "AllowAPIGatewayInvokeMarketMailDataExtractor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_data_gmail.function_name
  principal     = "apigateway.amazonaws.com"

  # source_arn: API -> POST /market_pdf
  source_arn = "${aws_api_gateway_rest_api.main_api.execution_arn}/*/POST/market_pdf"

  depends_on = [
    aws_api_gateway_rest_api.main_api,
    aws_lambda_function.extract_data_gmail
  ]

  lifecycle {
    create_before_destroy = true
    ignore_changes = [source_arn]
  }
}

resource "aws_lambda_permission" "allow_api_gateway_bank_mail_data_extractor" {
  statement_id  = "AllowAPIGatewayInvokeBankMailDataExtractor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_data_gmail.function_name
  principal     = "apigateway.amazonaws.com"

  # source_arn: API -> POST /bank_pdf
  source_arn = "${aws_api_gateway_rest_api.main_api.execution_arn}/*/POST/bank_pdf"

  depends_on = [
    aws_api_gateway_rest_api.main_api,
    aws_lambda_function.extract_data_gmail
  ]

  lifecycle {
    create_before_destroy = true
    ignore_changes = [source_arn]
  }
}

resource "aws_lambda_permission" "allow_api_gateway_mp_webhook" {
  statement_id  = "AllowAPIGatewayInvokeMercadoPagoWebhook"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.webhook_mp_report.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.main_api.execution_arn}/*/POST/mp_webhook"

  depends_on = [
    aws_api_gateway_rest_api.main_api,
    aws_lambda_function.webhook_mp_report
  ]

  lifecycle {
    create_before_destroy = true
    ignore_changes = [source_arn]
  }
}

# Output para obtener la URL del webhook de Telegram
output "telegram_webhook_url" {
  value       = "${aws_api_gateway_stage.main_api_stage.invoke_url}/telegram_bot"
  description = "URL del webhook para configurar en Telegram"
}

# Output para obtener la URL del webhook de Gmail
output "market_pdf_webhook_url" {
  value       = "${aws_api_gateway_stage.main_api_stage.invoke_url}/market_pdf"
  description = "URL del webhook para configurar en Gmail para escuchar mails recibidos de pagos del supermercado"
}

# Output para obtener la URL del webhook de Gmail
output "bank_pdf_webhook_url" {
  value       = "${aws_api_gateway_stage.main_api_stage.invoke_url}/bank_pdf"
  description = "URL del webhook para configurar en Gmail para escuchar mails recibidos de pagos del banco"
}

# Output para obtener la URL del webhook de Gmail
output "mp_webhook_url" {
  value       = "${aws_api_gateway_stage.main_api_stage.invoke_url}/mp_webhook"
  description = "URL del webhook para configurar en Gmail para escuchar mails recibidos de pagos del banco"
}

# Service Account
resource "google_service_account" "pubsub_sa" {
  account_id   = "terraform-sa"
  display_name = "terraform SA"
}

# Pub/Sub Topic
resource "google_pubsub_topic" "gmail_events" {
  name = "gmail-events"
}

# Pub/Sub Subscription
resource "google_pubsub_subscription" "gmail_subscription_bank_payments" {
  name  = "bank-payments-sub-to-api-gateway"
  topic = google_pubsub_topic.gmail_events.id

  # Configuración del expiration policy
  expiration_policy {
    ttl = "864000s"  # 10 días en segundos (máximo permitido)
  }

  # Otras configuraciones recomendadas
  ack_deadline_seconds = 10
  retain_acked_messages = false
  message_retention_duration = "604800s"  # 7 días
  
  push_config {
    push_endpoint = "${aws_api_gateway_stage.main_api_stage.invoke_url}/bank_pdf"

    oidc_token {
      service_account_email = google_service_account.pubsub_sa.email
      audience              = "${aws_api_gateway_stage.main_api_stage.invoke_url}/bank_pdf"
    }
  }
}

resource "google_pubsub_subscription" "gmail_subscription_market_tickets" {
  name  = "market-tickets-sub-to-api-gateway"
  topic = google_pubsub_topic.gmail_events.id
  
  # Configuración del expiration policy
  expiration_policy {
    ttl = "864000s"  # 10 días en segundos (máximo permitido)
  }

  # Otras configuraciones recomendadas
  ack_deadline_seconds = 10
  retain_acked_messages = false
  message_retention_duration = "604800s"  # 7 días

  push_config {
    push_endpoint = "${aws_api_gateway_stage.main_api_stage.invoke_url}/market_pdf"

    oidc_token {
      service_account_email = google_service_account.pubsub_sa.email
      audience              = "${aws_api_gateway_stage.main_api_stage.invoke_url}/market_pdf"
    }
  }
}

# IAM Binding en el tópico
resource "google_pubsub_topic_iam_member" "sa_publisher" {
  topic = google_pubsub_topic.gmail_events.name
  role  = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.pubsub_sa.email}"
}

# BigQuery permissions for Service Account
resource "google_project_iam_member" "sa_bigquery_admin" {
  project = var.GCP_PROJECT_ID
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pubsub_sa.email}"
}

# BigQuery Datasets
resource "google_bigquery_dataset" "staging" {
  dataset_id  = "STG"
  project     = var.GCP_PROJECT_ID
  location    = "US"
  description = "Staging dataset for ETL intermediate data"
  
  labels = {
    environment = "staging"
    managed_by  = "terraform"
  }
}

resource "google_bigquery_dataset" "production" {
  dataset_id  = "PRD"
  project     = var.GCP_PROJECT_ID
  location    = "US"
  description = "Production dataset for final ETL data"
  
  labels = {
    environment = "production"
    managed_by  = "terraform"
  }
}

# Lambda Function
resource "aws_lambda_function" "gmail_watcher" {
  function_name = "gmail-watcher-renewer"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:gmail_watcher-latest"

  memory_size = 512
  timeout     = 900

  environment {
    variables = {
      GCP_PROJECT_ID  = var.GCP_PROJECT_ID
      PUBSUB_TOPIC    = google_pubsub_topic.gmail_events.name
      GCP_SECRET_NAME = data.aws_secretsmanager_secret_version.gcp_sa_creds.secret_id
    }
  }
}

resource "aws_dynamodb_table" "gmail_history" {
  name           = "gmail-history-tracker"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "PK"

  attribute {
    name = "PK"
    type = "S"
  }
}

resource "aws_dynamodb_table" "schema_cache" {
  name           = var.dynamodb_table_name
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "table_name"
  attribute {
    name = "table_name"
    type = "S"
  }
  tags = {
    Name = "schema_cache"
    Env  = "prod"
  }
}

resource "aws_dynamodb_table" "telegram_processed_messages" {
  name         = "telegram_processed_messages"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "message_id"

  attribute {
    name = "message_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Name = "telegram-processed-messages"
  }
}

resource "aws_dynamodb_table" "telegram_pending_tickets" {
  name         = "telegram_pending_tickets"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "chat_id"

  attribute {
    name = "chat_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# 4. EventBridge rule (cada domingo 00:00 UTC)
resource "aws_cloudwatch_event_rule" "weekly" {
  name                = "gmail-watcher-renew-weekly"
  schedule_expression = "cron(0 0 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.weekly.name
  target_id = "gmailWatcherLambda"
  arn       = aws_lambda_function.gmail_watcher.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gmail_watcher.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weekly.arn
}

########### 4. Lambdas basadas en imágenes Docker ###########
# 4.2 Lambda para transformar PDFs de Gmail
resource "aws_lambda_function" "pdf_processor" {
  function_name = "pdf_processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:pdf_processor-latest"

  memory_size = 1024  # Más memoria para procesar PDFs
  timeout     = 900

  environment {
    variables = {
      MARKET_BUCKET = aws_s3_bucket.market_tickets.bucket
    }
  }
}

# 4.3 Lambda para extraer reportes de Mercado Pago
resource "aws_lambda_function" "mp_report_extractor" {
  function_name = "mp_report_extractor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:mp_report_extractor-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      MP_REPORTS_BUCKET_NAME = aws_s3_bucket.mp_reports.bucket
      MP_REPORT_STEP_FUNCTION_ARN = "arn:aws:states:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:stateMachine:mp-report-etl-flow"
      CIFRADO_SECRET_MP = var.CIFRADO_SECRET_MP
    }
  }
}

# 4.4 Lambda para transformar reportes de Mercado Pago
resource "aws_lambda_function" "mp_report_processor" {
  function_name = "mp_report_processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:mp_report_processor-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      MP_REPORTS_BUCKET_NAME = aws_s3_bucket.mp_reports.bucket
    }
  }
}

# 4.5 Lambda para extraer los gastos del banco a traves de avisos en Gmail
resource "aws_lambda_function" "extract_data_gmail" {
  function_name = "extract_data_gmail"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:extract_data_gmail-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      MARKET_BUCKET_NAME = aws_s3_bucket.market_tickets.bucket
      BANK_BUCKET_NAME   = aws_s3_bucket.bank_payments.bucket
      MP_TRANSFER_BUCKET_NAME = aws_s3_bucket.mp_transfers.bucket
      BANK_STEP_FUNCTION_ARN = aws_sfn_state_machine.bank_payments_etl_flow.arn
      MARKET_STEP_FUNCTION_ARN = aws_sfn_state_machine.pdf_etl_flow.arn
      MP_TRANSFER_STEP_FUNCTION_ARN =  aws_sfn_state_machine.mp_transfers_etl_flow.arn
      GCP_PROJECT_ID     = var.GCP_PROJECT_ID
      BQ_DATASET_PROD    = "PRD"
    }
  }
}

# 4.6 Lambda para procesar los gastos del banco
resource "aws_lambda_function" "bank_payments_processor" {
  function_name = "bank_payments_processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:bank_payments_processor-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      BANK_BUCKET = aws_s3_bucket.bank_payments.bucket
    }
  }
}

# 4.7 Lambda para cargar los dos ETLs a BigQuery (reportes de Mercado Pago y pdfs de Gmail)
resource "aws_lambda_function" "load_report_and_pdf" {
  function_name = "load_report_and_pdf"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:load_report_and_pdf-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      GCP_PROJECT_ID     = var.GCP_PROJECT_ID
      BQ_DATASET_STAGING = "STG"
      BQ_DATASET_PROD    = "PRD"
      BQ_LOCATION        = "US"
      MP_REPORTS_BUCKET  = aws_s3_bucket.mp_reports.bucket
      MP_REPORTS_BUCKET_NAME = aws_s3_bucket.mp_reports.bucket
    }
  }
}

# 4.8 Lambda Dispatcher que extrae los datos del body del POST request del webhook de reportes de MP y dispara el step function de MP
resource "aws_lambda_function" "webhook_mp_report" {
  function_name = "webhook_mp_report"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:webhook_mp_report-latest"

  memory_size = 512
  timeout     = 30

  environment {
    variables = {
      MP_REPORT_STEP_FUNCTION_ARN = aws_sfn_state_machine.mp_report_etl_flow.arn
      CIFRADO_SECRET_MP = var.CIFRADO_SECRET_MP
    }
  }
}

# 4.9 Lambda Compensation flow que limpia archivos temporales y el envia marca de que el proceso fallo por mail
resource "aws_lambda_function" "compensation_flow" {
  function_name = "compensation_flow"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:compensation_flow-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      ACCOUNT_ID = var.AWS_ACCOUNT_ID
      REGION_ID = var.AWS_REGION
      SNS_TOPIC = var.SNS_TOPIC
    }
  }
}

# 4.10 Lambda para procesar el agente de IA y resolver las consultas sobre los datos en BigQuery
resource "aws_lambda_function" "ai_agent" {
  function_name = "ai_agent"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:ai_agent-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      GCP_PROJECT_ID              = var.GCP_PROJECT_ID
      BQ_DATASET_PROD             = "PRD"
      BQ_LOCATION                 = "US"
      TELEGRAM_BOT_TOKEN          = var.TELEGRAM_BOT_TOKEN
      OPENAI_API_KEY              = var.OPENAI_API_KEY
      DDB_TABLE                   = var.dynamodb_table_name
      CACHE_TTL_SECONDS           = "604800"  # 7 días
      S3_BUCKET_TICKETS           = aws_s3_bucket.telegram_receipts.bucket
      S3_PREFIX_TICKETS           = "receipts/"
      RECEIPT_ETL_STATE_MACHINE   = aws_sfn_state_machine.telegram_receipt_etl_flow.arn
    }
  }
}

# 4.11 Lambda para extraer datos de tickets con OCR (OpenAI Vision + TabScanner fallback)
resource "aws_lambda_function" "process_telegram_img" {
  function_name = "process_telegram_img"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:process_telegram_img-latest"
  
  memory_size = 1024
  timeout     = 300

  environment {
    variables = {
      GCP_PROJECT_ID = var.GCP_PROJECT_ID,
      OPENAI_API_KEY     = var.OPENAI_API_KEY,
      TABSCANNER_API_KEY = var.TABSCANNER_API_KEY,
      S3_BUCKET_TICKETS  = aws_s3_bucket.telegram_receipts.bucket
    }
  }
}

# 4.12 
resource "aws_lambda_function" "load_receipt_to_bq" {
  function_name = "load_receipt_to_bq"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:load_receipt_to_bq-latest"
  
  memory_size = 1024
  timeout     = 300

  environment {
    variables = {
      GCP_PROJECT_ID = var.GCP_PROJECT_ID
    }
  }
}

# 4.13 Lambda para procesar las transferencias de mercado pago
resource "aws_lambda_function" "mp_transfers_processor" {
  function_name = "mp_transfers_processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:mp_transfers_processor-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      MP_TRANSFER_BUCKET_NAME = aws_s3_bucket.mp_transfers.bucket
    }
  }
}

###########  5. Permisos IAM Roles ###########
# IAM role para Lambda execution
resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# IAM role para Step Functions
resource "aws_iam_role" "step_function_role" {
  name = "step_function_role"

  assume_role_policy = jsonencode({
  Version = "2012-10-17",
  Statement = [
    {
      Effect = "Allow",
      Principal = {
        Service = ["events.amazonaws.com", "states.amazonaws.com"]
      },
      Action = "sts:AssumeRole"
    }
  ]
  })
}

resource "aws_iam_role" "glue_service_role" {
  name = "glue_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role" "api_gateway_role" {
  name = "api_gateway_step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
      }
    ]
  })
}
###########  6. Permisos IAM Policies ###########
resource "aws_iam_policy" "lambda_kms_policy" {
  name = "lambda-kms-access"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ],
        Resource = "arn:aws:kms:us-east-2:039434644707:key/5009b119-f50c-413c-9873-0e216eb14005"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_kms_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_kms_policy.arn
}

# Policy para acceder a los secrets de Secret Manager con Lambda
resource "aws_iam_role_policy" "secrets_token_access" {
  name = "lambda_token_google_secrets"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:UpdateSecret"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:secret:gcp_api_credentials-*",
          "arn:aws:secretsmanager:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:secret:gcp_sa_api_credentials-*"
        ]
      }
    ]
  })
}

# Políticas para Lambda

resource "aws_iam_policy" "lambda_ecr_access" {
  name = "lambda_ecr_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ],
        Effect   = "Allow",
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_s3_access" {
  name = "lambda_s3_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ],
        Effect = "Allow",
        Resource = [
          "${aws_s3_bucket.market_tickets.arn}/*",
          aws_s3_bucket.market_tickets.arn,
          aws_s3_bucket.mp_reports.arn,
          "${aws_s3_bucket.mp_reports.arn}/*",
          "${aws_s3_bucket.bank_payments.arn}/*",
          aws_s3_bucket.bank_payments.arn,
          "${aws_s3_bucket.mp_transfers.arn}/*",
          aws_s3_bucket.mp_transfers.arn,
          "${aws_s3_bucket.telegram_receipts.arn}/*",
          aws_s3_bucket.telegram_receipts.arn
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_dynamo_policy" {
  name        = "lambda-dynamo-gmail"
  description = "Permite a la lambda leer/escribir historyId en DynamoDB"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          "arn:aws:dynamodb:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:table/gmail-history-tracker",
          "arn:aws:dynamodb:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:table/schema_cache",
          "arn:aws:dynamodb:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:table/telegram_processed_messages",
          "arn:aws:dynamodb:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:table/telegram_pending_tickets",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_dynamo_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_dynamo_policy.arn
}


# Policy para permitir ejecutar Step Functions
resource "aws_iam_role_policy" "api_gateway_step_function_policy" {
  name = "api_gateway_step_function_policy"
  role = aws_iam_role.api_gateway_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = "states:StartExecution",
        Resource = [
          aws_sfn_state_machine.bank_payments_etl_flow.arn,
          aws_sfn_state_machine.pdf_etl_flow.arn
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Attachments de las políticas al rol
resource "aws_iam_role_policy_attachment" "lambda_ecr" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_ecr_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_s3_access.arn
}

# Policy para eliminar imagenes que no estan dentro de los tags de las funciones Lambda de los dos jobs
resource "aws_ecr_lifecycle_policy" "delete_unwanted_images" {
  repository = aws_ecr_repository.lambda_images.name

  policy = jsonencode({
    rules = [
      # Regla 1: Primeros 10 tags
      {
        rulePriority = 1
        description  = "Eliminar imágenes de los primeros 10 tags"
        selection = {
          tagStatus = "tagged"
          tagPrefixList = [
            "extract_data_gmail-latest",
            "pdf_processor-latest",
            "mp_report_extractor-latest",
            "mp_report_processor-latest",
            "bank_payments_processor-latest",
            "load_report_and_pdf-latest",
            "webhook_mp_report-latest",
            "compensation_flow-latest",
            "process_telegram_img-latest",
            "load_receipt_to_bq-latest"
          ]
          countType   = "imageCountMoreThan"
          countNumber = 1
        }
        action = { type = "expire" }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_exec_copy_policy" {
  name = "LambdaExecCopyPolicy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.market_tickets.arn}/*",
          aws_s3_bucket.market_tickets.arn,
          "${aws_s3_bucket.mp_reports.arn}/*",
          aws_s3_bucket.mp_reports.arn,
          "${aws_s3_bucket.bank_payments.arn}/*",
          aws_s3_bucket.bank_payments.arn,
          "${aws_s3_bucket.mp_transfers.arn}/*",
          aws_s3_bucket.mp_transfers.arn,
          "${aws_s3_bucket.telegram_receipts.arn}/*",
          aws_s3_bucket.telegram_receipts.arn
        ]
      }
    ]
  })
}

# Policy que permite a Glue poder acceder a S3 y SSM
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue_s3_access"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:GetCatalogImportStatus"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath"
        ]
        Resource = "arn:aws:ssm:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:parameter/mercado_pago/token"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.market_tickets.arn,
          "${aws_s3_bucket.market_tickets.arn}/*",
          aws_s3_bucket.mp_reports.arn,
          "${aws_s3_bucket.mp_reports.arn}/*",
          aws_s3_bucket.bank_payments.arn,
          "${aws_s3_bucket.bank_payments.arn}/*",
          "${aws_s3_bucket.mp_transfers.arn}/*",
          aws_s3_bucket.mp_transfers.arn,
          aws_s3_bucket.telegram_receipts.arn,
          "${aws_s3_bucket.telegram_receipts.arn}/*"
        ]
      }
    ]
  })
}

# Policy para bloquear cualquier acceso al bucket de S3 de PDFs de Gmail que no sea por HTTPS (Secure Transport).
resource "aws_s3_bucket_policy" "market_tickets_policy" {
  bucket = aws_s3_bucket.market_tickets.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          aws_s3_bucket.market_tickets.arn,
          "${aws_s3_bucket.market_tickets.arn}/*"
        ],
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

resource "aws_s3_bucket_policy" "mp_transfers_policy" {
  bucket = aws_s3_bucket.mp_transfers.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          aws_s3_bucket.mp_transfers.arn,
          "${aws_s3_bucket.mp_transfers.arn}/*"
        ],
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Policy para bloquear cualquier acceso al bucket de S3 de reportes de Mercado Pago que no sea por HTTPS (Secure Transport).
resource "aws_s3_bucket_policy" "mp_reports_policy" {
  bucket = aws_s3_bucket.mp_reports.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          aws_s3_bucket.mp_reports.arn,
          "${aws_s3_bucket.mp_reports.arn}/*"
        ],
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Policy para bloquear cualquier acceso al bucket de S3 de gastos del banco que no sea por HTTPS (Secure Transport).
resource "aws_s3_bucket_policy" "bank_payments_policy" {
  bucket = aws_s3_bucket.bank_payments.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          aws_s3_bucket.bank_payments.arn,
          "${aws_s3_bucket.bank_payments.arn}/*"
        ],
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Policy para bloquear cualquier acceso al bucket de S3 de tickets de Telegram que no sea por HTTPS.
resource "aws_s3_bucket_policy" "telegram_receipts_policy" {
  bucket = aws_s3_bucket.telegram_receipts.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Deny",
        Principal = "*",
        Action    = "s3:*",
        Resource = [
          aws_s3_bucket.telegram_receipts.arn,
          "${aws_s3_bucket.telegram_receipts.arn}/*"
        ],
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLambda_FullAccess"
}

# Policy para la Step Function para ejecutar funciones Lambda
resource "aws_iam_policy" "step_function_lambda_policy" {
  name = "step_function_lambda_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["lambda:InvokeFunction"],
        Resource = [
          aws_lambda_function.extract_data_gmail.arn,

          aws_lambda_function.pdf_processor.arn,

          aws_lambda_function.mp_report_extractor.arn,
          aws_lambda_function.mp_report_processor.arn,

          aws_lambda_function.bank_payments_processor.arn,

          aws_lambda_function.load_report_and_pdf.arn,
          aws_lambda_function.ai_agent.arn,

          # Lambdas para ETL de tickets de Telegram
          aws_lambda_function.process_telegram_img.arn,
          aws_lambda_function.load_receipt_to_bq.arn
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "step_function_start_policy" {
  name = "step_function_start_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = "states:StartExecution",
      Resource = [
        aws_sfn_state_machine.pdf_etl_flow.arn,
        aws_sfn_state_machine.mp_report_etl_flow.arn,
        aws_sfn_state_machine.bank_payments_etl_flow.arn
      ]
    }]
  })
}

# Policy para que Lambda pueda invocar Step Functions EXPRESS sincrónicamente
resource "aws_iam_policy" "lambda_step_function_sync_policy" {
  name = "lambda_step_function_sync_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "states:StartSyncExecution",
          "states:StartExecution",
          "states:DescribeExecution"
        ],
        Resource = [
          aws_sfn_state_machine.telegram_receipt_etl_flow.arn,
          aws_sfn_state_machine.bank_payments_etl_flow.arn,
          aws_sfn_state_machine.pdf_etl_flow.arn,
          aws_sfn_state_machine.mp_report_etl_flow.arn,
          "${aws_sfn_state_machine.telegram_receipt_etl_flow.arn}:*",
          "${aws_sfn_state_machine.bank_payments_etl_flow.arn}:*",
          "${aws_sfn_state_machine.pdf_etl_flow.arn}:*",
          "${aws_sfn_state_machine.mp_report_etl_flow.arn}:*",
          "arn:aws:states:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:execution:${aws_sfn_state_machine.telegram_receipt_etl_flow.name}:*",
          "arn:aws:states:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:execution:${aws_sfn_state_machine.bank_payments_etl_flow.name}:*",
          "arn:aws:states:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:execution:${aws_sfn_state_machine.pdf_etl_flow.name}:*",
          "arn:aws:states:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:execution:${aws_sfn_state_machine.mp_report_etl_flow.name}:*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_step_function_sync_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_step_function_sync_policy.arn
}

resource "aws_iam_policy" "step_function_glue_policy" {
  name = "step_function_glue_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["glue:StartCrawler"],
      Resource = [
        aws_glue_crawler.market_tickets_crawler.arn,
        aws_glue_crawler.mp_reports_crawler.arn,
        aws_glue_crawler.bank_payments_crawler.arn
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_glue_policy" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_glue_policy.arn
}


resource "aws_iam_role_policy_attachment" "attach_start_policy" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_start_policy.arn
}

# Policy para ejecutar logueos de errores de jobs de las Step Functions
resource "aws_iam_role_policy" "step_function_logging" {
  name = "step-function-logging-policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attachment de policies
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach_lambda_policy" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_lambda_policy.arn
}

resource "aws_iam_role_policy_attachment" "compensation_lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "compensation_lambda_sns_publish" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSNSFullAccess"
}

########### 7. Triggers y Eventos ###########
# 7.4 Creacion de grupo de logging de los ETLs
resource "aws_cloudwatch_log_group" "etl_logs" {
  name              = "/aws/vendedlogs/states/etl-logs"
  retention_in_days = 14
}

########### 8. Step Function para orquestar Lambdas ###########

# 8.1 Creacion del job de PDFs en Step Function
resource "aws_sfn_state_machine" "pdf_etl_flow" {
  name     = "pdf-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  definition = jsonencode({
    StartAt = "Check If Should Process",
    States = {
      "Check If Should Process" = {
        Type = "Choice",
        Choices = [
          {
            Variable      = "$.body.process",
            BooleanEquals = true,
            Next          = "Transform Gmail PDFs"
          }
        ],
        Default = "SkipProcessing"
      },

      "SkipProcessing" = {
        Type = "Succeed"
      },

      # Step 1: Transform
      "Transform Gmail PDFs" = {
        Type       = "Task",
        Resource   = aws_lambda_function.pdf_processor.arn,
        Parameters = {
          "key.$" = "$.body.key"
        },
        Next  = "Load Gmail PDFs",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 2: Load
      "Load Gmail PDFs" = {
        Type       = "Task",
        Resource   = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.body.etl_flow",
          "bucket.$"      = "$.body.bucket",
          "key.$"         = "$.body.key"
        },
        End = true,
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 3: Compensation Flow (en caso de error)
      "CompensationFlow" = {
        Type     = "Task",
        Resource = "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        End      = true
      }
    }
  })
}

resource "aws_sfn_state_machine" "mp_report_etl_flow" {
  name     = "mp-report-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  definition = jsonencode({
    StartAt = "Extract MP Reports",
    States = {
      # Step 1: Extract
      "Extract MP Reports" = {
        Type       = "Task",
        Resource   = aws_lambda_function.mp_report_extractor.arn,
        Parameters = {
          "file_name.$" = "$.file_name",
          "file_url.$"  = "$.file_url",
          "file_type.$" = "$.file_type"
        },
        Next  = "Transform MP Reports",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 2: Transform
      "Transform MP Reports" = {
        Type       = "Task",
        Resource   = aws_lambda_function.mp_report_processor.arn,
        Parameters = {
          "key.$" = "$.key"
        },
        Next  = "Load MP Reports",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 3: Load
      "Load MP Reports" = {
        Type       = "Task",
        Resource   = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.etl_flow",
          "bucket.$"      = "$.bucket",
          "key.$"         = "$.key",
          "report_date.$" = "$.report_date",
          "report_id.$"   = "$.report_id"
        },
        End = true,
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step compensatorio
      "CompensationFlow" = {
        Type     = "Task",
        Resource = "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        End      = true
      }
    }
  })
}


resource "aws_sfn_state_machine" "bank_payments_etl_flow" {
  name     = "bank-payments-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  definition = jsonencode({
    StartAt = "Check If Should Process",
    States = {
      # Step 1: Choice
      "Check If Should Process" = {
        Type = "Choice",
        Choices = [
          {
            Variable      = "$.body.process",
            BooleanEquals = true,
            Next          = "Transform Gmail Bank Payments"
          }
        ],
        Default = "SkipProcessing"
      },

      "SkipProcessing" = {
        Type = "Succeed"
      },

      # Step 2: Transform
      "Transform Gmail Bank Payments" = {
        Type       = "Task",
        Resource   = aws_lambda_function.bank_payments_processor.arn,
        Parameters = {
          "key.$" = "$.body.key"
        },
        Next  = "Load Gmail Bank Payments",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 3: Load
      "Load Gmail Bank Payments" = {
        Type       = "Task",
        Resource   = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.body.etl_flow",
          "bucket.$"      = "$.body.bucket",
          "key.$"         = "$.body.key"
        },
        End = true,
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step compensatorio
      "CompensationFlow" = {
        Type     = "Task",
        Resource = "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        End      = true
      }
    }
  })
}

resource "aws_sfn_state_machine" "mp_transfers_etl_flow" {
  name     = "mp-transfers-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  definition = jsonencode({
    StartAt = "Check If Should Process",
    States = {
      # Step 1: Choice
      "Check If Should Process" = {
        Type = "Choice",
        Choices = [
          {
            Variable      = "$.body.process",
            BooleanEquals = true,
            Next          = "Transform Gmail MP Transfers"
          }
        ],
        Default = "SkipProcessing"
      },

      "SkipProcessing" = {
        Type = "Succeed"
      },

      # Step 2: Transform
      "Transform Gmail MP Transfers" = {
        Type       = "Task",
        Resource   = aws_lambda_function.mp_transfers_processor.arn,
        Parameters = {
          "key.$" = "$.body.key"
        },
        Next  = "Load Gmail MP Transfers",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step 3: Load
      "Load Gmail MP Transfers" = {
        Type       = "Task",
        Resource   = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.body.etl_flow",
          "bucket.$"      = "$.body.bucket",
          "key.$"         = "$.body.key"
        },
        End = true,
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            ResultPath  = "$.error-info",
            Next        = "CompensationFlow"
          }
        ]
      },

      # Step compensatorio
      "CompensationFlow" = {
        Type     = "Task",
        Resource = "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        End      = true
      }
    }
  })
}

# 8.4 Step Function EXPRESS para ETL de tickets de Telegram (síncrona)
resource "aws_sfn_state_machine" "telegram_receipt_etl_flow" {
  name     = "telegram-receipt-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn
  type     = "EXPRESS"  # Express para ejecución síncrona

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  definition = jsonencode({
    Comment = "ETL para procesar tickets de supermercado enviados por Telegram"
    StartAt = "Extract Receipt with OCR"
    States = {
      # Step 1: Extraer datos del ticket con OCR (OpenAI Vision + TabScanner fallback)
      "Extract Receipt with OCR" = {
        Type     = "Task"
        Resource = aws_lambda_function.process_telegram_img.arn
        Parameters = {
          "s3_key.$"       = "$.s3_key"
          "s3_bucket.$"    = "$.s3_bucket"
          "use_fallback.$" = "$.use_fallback"
        }
        ResultPath = "$.extraction_result"
        Next       = "Load Receipt to BigQuery"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "Handle Error"
          }
        ]
      }

      # Step 2: Cargar datos en BigQuery
      "Load Receipt to BigQuery" = {
        Type     = "Task"
        Resource = aws_lambda_function.load_receipt_to_bq.arn
        Parameters = {
          "extracted_data.$" = "$.extraction_result"
          "s3_key.$"         = "$.s3_key"
        }
        ResultPath = "$.load_result"
        Next       = "Format Success Response"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "Handle Error"
          }
        ]
      }

      # Step 3: Formatear respuesta exitosa
      "Format Success Response" = {
        Type = "Pass"
        Parameters = {
          "success"           = true
          "extracted_data.$"  = "$.extraction_result"
          "rows_inserted.$"   = "$.load_result.rows_inserted"
          "message"           = "Ticket procesado exitosamente"
        }
        End = true
      }

      # Manejo de errores
      "Handle Error" = {
        Type = "Pass"
        Parameters = {
          "success"       = false
          "error_message.$" = "$.error.Cause"
          "message"       = "Error procesando el ticket"
        }
        End = true
      }
    }
  })
}

########### 9. Glue Data Catalog ###########

resource "aws_glue_catalog_database" "etl_database" {
  name = var.glue_database_name
}

########### 10. Glue Crawlers ###########

resource "aws_glue_crawler" "market_tickets_crawler" {
  name          = var.glue_crawler_name_market_tickets
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name
  description  = "Crawler semanal que analiza la carpeta processed/ en S3"
  table_prefix  = "market_tickets_"

  s3_target {
    path = "s3://${aws_s3_bucket.market_tickets.bucket}/processed/"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    },
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schedule = "cron(0 11 ? * MON *)" # Corre todos los lunes a las 8:00 UTC-3
}

resource "aws_glue_crawler" "mp_reports_crawler" {
  name          = var.glue_crawler_name_mp_reports
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name
  description   = "Crawler semanal que analiza la carpeta processed/ en S3"
  table_prefix  = "mp_reports_"

  s3_target {
    path = "s3://${aws_s3_bucket.mp_reports.bucket}/processed/"
  }

  # Opcionalmente podés agregar configuración básica de agrupamiento
  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    },
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  classifiers = [aws_glue_classifier.csv_classifier.name]

  schedule = "cron(0 11 ? * MON *)" # Corre todos los lunes a las 8:00 UTC-3
}

resource "aws_glue_classifier" "csv_classifier" {
  name = "mercadopago_csv_classifier"

  csv_classifier {
    allow_single_column = false
    contains_header     = "PRESENT"
    delimiter           = ","
    quote_symbol        = "\""
  }
}

resource "aws_glue_crawler" "bank_payments_crawler" {
  name          = var.glue_crawler_name_bank_payments
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name
  description   = "Crawler semanal que analiza la carpeta processed/ en S3"
  table_prefix  = "bank_payments_"

  s3_target {
    path = "s3://${aws_s3_bucket.bank_payments.bucket}/processed/"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    },
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schedule = "cron(0 11 ? * MON *)" # Corre todos los lunes a las 8:00 UTC-3
}

resource "aws_glue_crawler" "mp_transfers_crawler" {
  name          = var.glue_crawler_name_mp_transfers
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name
  description   = "Crawler semanal que analiza la carpeta processed/ en S3"
  table_prefix  = "mp_transfers_"

  s3_target {
    path = "s3://${aws_s3_bucket.mp_transfers.bucket}/processed/"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    },
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schedule = "cron(0 11 ? * MON *)" # Corre todos los lunes a las 8:00 UTC-3
}


########### 11. CloudWatch Alarm ###########

# 11.1 Creacion del topico de SNS para enviar alertas
resource "aws_sns_topic" "stepfunction_alerts" {
  name = "stepfunction-alerts"
}

# 11.2 Suscripcion del topico de SNS para enviar alertas por mail
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.stepfunction_alerts.arn
  protocol  = "email"
  endpoint  = "${var.EMAIL}"
}

# 11.3 Calculo de metricas de errores de ejecucion del ETL de PDFs en Cloudwatch
resource "aws_cloudwatch_metric_alarm" "etl_step_function_pdf_failure" {
  alarm_name          = "pdfFailures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors Lambda errors"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pdf_etl_flow.arn
  }
  alarm_actions = [aws_sns_topic.stepfunction_alerts.arn]
}

# 11.4 Calculo de metricas de errores de ejecucion del ETL de reportes de MP en Cloudwatch
resource "aws_cloudwatch_metric_alarm" "etl_step_function_mp_report_failure" {
  alarm_name          = "mpFailures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors Lambda errors"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.mp_report_etl_flow.arn
  }
  alarm_actions = [aws_sns_topic.stepfunction_alerts.arn]
}

# 11.4 Calculo de metricas de errores de ejecucion del ETL de reportes de MP en Cloudwatch
resource "aws_cloudwatch_metric_alarm" "etl_step_function_bank_payments_failure" {
  alarm_name          = "bank_payment_Failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors Lambda errors"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.bank_payments_etl_flow.arn
  }
  alarm_actions = [aws_sns_topic.stepfunction_alerts.arn]
}
