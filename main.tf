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

variable "REDSHIFT_USER" {
  description = "username redshift database"
  type        = string
  sensitive   = true
}

variable "REDSHIFT_PASSWORD" {
  description = "redshift database password"
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

########### 2. Redshift Serverless ###########
# Creamos el namespace
resource "aws_redshiftserverless_namespace" "etl_namespace" {
  namespace_name = "pdf-etl-namespace"
  db_name        = "dev"
  iam_roles = [aws_iam_role.lambda_exec.arn]
}

# Creamos el workgroup
resource "aws_redshiftserverless_workgroup" "etl_workgroup" {
  workgroup_name = "pdf-etl-workgroup"
  namespace_name = aws_redshiftserverless_namespace.etl_namespace.namespace_name
  base_capacity  = 8 # RPUs
  # Configuración correcta para Data API:
  publicly_accessible = true
}

########### 3. Repositorio ECR para las imágenes Lambda ###########
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
  type                    = "AWS"
  integration_http_method = "POST"
  
  # URI para Step Functions - FORMATO ESPECIAL
  uri = "arn:aws:apigateway:${var.AWS_REGION}:states:action/StartExecution"
  
  # Credenciales del rol de API Gateway
  credentials = aws_iam_role.api_gateway_role.arn
  
  # Transformación del request
  request_templates = {
    "application/json" = <<EOF
{
  "input": "$util.escapeJavaScript($input.json('$'))",
  "stateMachineArn": "${aws_sfn_state_machine.pdf_etl_flow.arn}"
}
EOF
  }
}

resource "aws_api_gateway_method_response" "market_pdf_response" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  resource_id = aws_api_gateway_resource.market_pdf_resource.id
  http_method = aws_api_gateway_method.market_pdf_method.http_method
  status_code = "200"
  
  response_models = {
    "application/json" = "Empty"
  }
}

resource "aws_api_gateway_integration_response" "market_pdf_integration_response" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  resource_id = aws_api_gateway_resource.market_pdf_resource.id
  http_method = aws_api_gateway_method.market_pdf_method.http_method
  status_code = aws_api_gateway_method_response.market_pdf_response.status_code
  
  response_templates = {
    "application/json" = "{\"status\": \"Step Function execution started\", \"executionArn\": \"$input.path('$.executionArn')\"}"
  }
}

# resource "aws_api_gateway_integration" "market_pdf_integration" {
#   rest_api_id             = aws_api_gateway_rest_api.main_api.id
#   resource_id             = aws_api_gateway_resource.market_pdf_resource.id
#   http_method             = aws_api_gateway_method.market_pdf_method.http_method
#   integration_http_method = "POST"
#   type                    = "AWS_PROXY"
#   uri                     = aws_lambda_function.extract_data_gmail.invoke_arn
# }


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

# resource "aws_api_gateway_integration" "bank_pdf_extractor_integration" {
#   rest_api_id             = aws_api_gateway_rest_api.main_api.id
#   resource_id             = aws_api_gateway_resource.bank_pdf_extractor_resource.id
#   http_method             = aws_api_gateway_method.bank_pdf_extractor_method.http_method
#   integration_http_method = "POST"
#   type                    = "AWS_PROXY"
#   uri                     = aws_lambda_function.extract_data_gmail.invoke_arn
# }

resource "aws_api_gateway_integration" "bank_pdf_extractor_integration" {
  rest_api_id             = aws_api_gateway_rest_api.main_api.id
  resource_id             = aws_api_gateway_resource.bank_pdf_extractor_resource.id
  http_method             = aws_api_gateway_method.bank_pdf_extractor_method.http_method
  type                    = "AWS"  # ← Tipo AWS (no AWS_PROXY)
  integration_http_method = "POST"
  
  # URI para Step Functions - FORMATO ESPECIAL
  uri = "arn:aws:apigateway:${var.AWS_REGION}:states:action/StartExecution"
  
  # Credenciales del rol de API Gateway
  credentials = aws_iam_role.api_gateway_role.arn
  
  # Transformación del request
  request_templates = {
    "application/json" = <<EOF
{
  "input": "$util.escapeJavaScript($input.json('$'))",
  "stateMachineArn": "${aws_sfn_state_machine.bank_payments_etl_flow.arn}"
}
EOF
  }
}

resource "aws_api_gateway_method_response" "bank_pdf_extractor_response" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  resource_id = aws_api_gateway_resource.bank_pdf_extractor_resource.id
  http_method = aws_api_gateway_method.bank_pdf_extractor_method.http_method
  status_code = "200"
  
  response_models = {
    "application/json" = "Empty"
  }
}

resource "aws_api_gateway_integration_response" "bank_pdf_extractor_integration_response" {
  rest_api_id = aws_api_gateway_rest_api.main_api.id
  resource_id = aws_api_gateway_resource.bank_pdf_extractor_resource.id
  http_method = aws_api_gateway_method.bank_pdf_extractor_method.http_method
  status_code = aws_api_gateway_method_response.bank_pdf_extractor_response.status_code
  
  response_templates = {
    "application/json" = "{\"status\": \"Step Function execution started\", \"executionArn\": \"$input.path('$.executionArn')\"}"
  }
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
    aws_api_gateway_integration_response.market_pdf_integration_response,
    aws_api_gateway_integration_response.bank_pdf_extractor_integration_response,
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

resource "aws_lambda_permission" "allow_api_gateway_bank_mail_data_extractor" {
  statement_id  = "AllowAPIGatewayInvokeBankMailDataExtractor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_data_gmail.function_name
  principal     = "apigateway.amazonaws.com"

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
  value       = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/telegram_bot"
  description = "URL del webhook para configurar en Telegram"
}

# Output para obtener la URL del webhook de Gmail
output "market_pdf_webhook_url" {
  value       = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/market_pdf"
  description = "URL del webhook para configurar en Gmail para escuchar mails recibidos de pagos del supermercado"
}

# Output para obtener la URL del webhook de Gmail
output "bank_pdf_webhook_url" {
  value       = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/bank_pdf"
  description = "URL del webhook para configurar en Gmail para escuchar mails recibidos de pagos del banco"
}

# Output para obtener la URL del webhook de Gmail
output "mp_webhook_url" {
  value       = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/mp_webhook"
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
  
  push_config {
    push_endpoint = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/bank_pdf"
  }
}

resource "google_pubsub_subscription" "gmail_subscription_market_tickets" {
  name  = "market-tickets-sub-to-api-gateway"
  topic = google_pubsub_topic.gmail_events.id
  
  push_config {
    push_endpoint = "${aws_api_gateway_deployment.main_api_deployment.invoke_url}/market_pdf"
  }
}

# IAM Binding en el tópico
resource "google_pubsub_topic_iam_member" "sa_publisher" {
  topic = google_pubsub_topic.gmail_events.name
  role  = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.pubsub_sa.email}"
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
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
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
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      BUCKET_NAME    = aws_s3_bucket.mp_reports.bucket
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
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      BUCKET_NAME    = aws_s3_bucket.mp_reports.bucket
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
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      MARKET_BUCKET_NAME    = aws_s3_bucket.market_tickets.bucket
      BANK_BUCKET_NAME      = aws_s3_bucket.bank_payments.bucket 
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
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      BUCKET_NAME    = aws_s3_bucket.mp_reports.bucket
    }
  }
}

# 4.7 Lambda para cargar los dos ETLs a tablas productivas de Redshift (reportes de Mercado Pago y pdfs de Gmail)
resource "aws_lambda_function" "load_report_and_pdf" {
  function_name = "load_report_and_pdf"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:load_report_and_pdf-latest"

  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      BUCKET_NAME    = aws_s3_bucket.mp_reports.bucket
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
      STEP_FUNCTION_ARN = aws_sfn_state_machine.mp_report_etl_flow.arn
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

# 4.10 Lambda data load de redshift a big query para visualizar los datos
resource "aws_lambda_function" "redshift_to_bq" {
  function_name = "redshift_to_bq"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:redshift_to_bq-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos
}

# 4.11 Lambda para procesar el agente de IA y resolver las consultas sobre los datos en Redshift
resource "aws_lambda_function" "ai_agent" {
  function_name = "ai_agent"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:ai_agent-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      REDSHIFT_WORKGROUP = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      REDSHIFT_DATABASE  = "dev",
      TELEGRAM_BOT_TOKEN = var.TELEGRAM_BOT_TOKEN,
      OPENAI_API_KEY     = var.OPENAI_API_KEY
    }
  }
}


###########  5. Permisos IAM Roles ###########
# IAM role para Lambda execution
resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
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

# Separar las políticas en recursos distintos
resource "aws_iam_policy" "lambda_redshift_access" {
  name = "lambda_redshift_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "redshift-data:*",
          "redshift:GetClusterCredentials",
          "redshift:Describe*",
          "redshift-serverless:*"
        ],
        Effect   = "Allow",
        Resource = "*"
      }
    ]
  })
}

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

resource "aws_iam_policy" "lambda_bedrock_access" {
  name = "lambda_bedrock_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:ListFoundationModels",
          "bedrock:GetFoundationModel",
          "bedrock-runtime:InvokeModel",
          "bedrock-runtime:InvokeModelWithResponseStream"
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
          aws_s3_bucket.bank_payments.arn
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
          "dynamodb:UpdateItem"
        ]
        Resource = "arn:aws:dynamodb:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:table/gmail-history-tracker"
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
resource "aws_iam_role_policy_attachment" "lambda_redshift" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_redshift_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_ecr" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_ecr_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_bedrock" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_bedrock_access.arn
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
            "redshift_to_bq-latest"
          ]
          countType   = "imageCountMoreThan"
          countNumber = 1
        }
        action = { type = "expire" }
      }
    ]
  })
}

# Policy que permite a Glue poder acceder a las tablas de Redshift y S3
resource "aws_iam_role_policy" "redshift_spectrum_glue_access" {
  name = "redshift_spectrum_glue_access"
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
          "${aws_s3_bucket.bank_payments.arn}/*"
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
          aws_lambda_function.redshift_to_bq.arn,

          aws_lambda_function.ai_agent.arn
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

# StartAt = "Extract Gmail PDFs",
#     # Primer step ejecuta Extract data
#     States = {
#       "Extract Gmail PDFs" = {
#         Type     = "Task",
#         Resource = aws_lambda_function.extract_data_gmail.arn,
#         Catch: [
#           {
#             "ErrorEquals": ["States.ALL"],
#             "ResultPath": "$.error-info",
#             "Next": "CompensationFlow"
#           }
#         ],
#         Next     = "Check If Should Process"
#       },

# 8.1 Creacion del job de PDFs en Step Function
resource "aws_sfn_state_machine" "pdf_etl_flow" {
  name     = "pdf-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  # Steps secuenciales
  definition = jsonencode({
      StartAt = "Check If Should Process",
      States = {
      "Check If Should Process" = {
        Type = "Choice",
        Choices = [
          {
            "Variable": "$.body.process",
            "BooleanEquals": true,
            "Next": "Transform Gmail PDFs"
          }
        ],
        Default = "SkipProcessing"
      },
      "SkipProcessing" = {
        Type = "Succeed"
      },
      # Segundo step ejecuta Transform data
      "Transform Gmail PDFs" = {
        Type     = "Task",
        Resource = aws_lambda_function.pdf_processor.arn,
        Parameters = {
          "key.$": "$.body.key"
        },
        Next     = "Load Gmail PDFs",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      # Tercer step ejecuta Load data
      "Load Gmail PDFs" = {
        Type     = "Task",
        Resource = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.body.etl_flow"
          "bucket.$"      = "$.body.bucket"
          "key.$"         = "$.body.key"
          "report_id.$"   = "$.body.report_id"
          "report_date.$" = "$.body.report_date"
        },
        Next     = "Export Redshift data to BigQuery",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      "Export Redshift data to BigQuery" = {
        Type     = "Task",
        Resource = aws_lambda_function.redshift_to_bq.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Run Market Tickets Crawler"
      },
      # Ultimo step ejecuta Glue Crawler
      "Run Market Tickets Crawler" = {
        Type     = "Task",
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler",
        Parameters = {
          Name = aws_glue_crawler.market_tickets_crawler.name
        },
        End = true
      },
      # Step compensatorio por si falla algun step del job
      CompensationFlow: {
        "Type": "Task",
        "Resource": "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        "End": true
      }
    }
  })
}

# 8.2 Creacion del job de Reportes MP en Step Function
resource "aws_sfn_state_machine" "mp_report_etl_flow" {
  name     = "mp-report-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  # Steps secuenciales
  definition = jsonencode({
    StartAt = "Extract MP Reports",
    States = {
      # Primer step ejecuta Extract data
      "Extract MP Reports" = {
        Type     = "Task",
        Resource = aws_lambda_function.mp_report_extractor.arn,
        Parameters: {
          "file_name.$": "$.file_name",
          "file_url.$": "$.file_url",
          "file_type.$": "$.file_type"
        },
        Next     = "Transform MP Reports",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      # Segundo step ejecuta Transform data
      "Transform MP Reports" = {
        Type     = "Task",
        Resource = aws_lambda_function.mp_report_processor.arn,
        Parameters = {
          "key.$": "$.key"
        },
        Next     = "Load MP Reports",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      # Tercer step ejecuta Load data
      "Load MP Reports" = {
        Type     = "Task",
        Resource = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.etl_flow"
          "bucket.$"      = "$.bucket"
          "key.$"         = "$.key"
          "report_id.$"   = "$.report_id"
          "report_date.$" = "$.report_date"
        },
        Next     = "Export Redshift data to BigQuery",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      "Export Redshift data to BigQuery" = {
        Type     = "Task",
        Resource = aws_lambda_function.redshift_to_bq.arn,
        Parameters = {
          "table_name.$" = "$.table_name"
        }
        Next     = "Run MP Reports Crawler",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      # Ultimo step ejecuta Glue Crawler
      "Run MP Reports Crawler" = {
        Type     = "Task",
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler",
        Parameters = {
          Name = aws_glue_crawler.mp_reports_crawler.name
        },
        End = true
      },
      # Step compensatorio por si falla algun step del job
      CompensationFlow: {
        "Type": "Task",
        "Resource": "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        "End": true
      }
    }
  })
}


# StartAt = "Extract Bank Payments Gmail",
#     # Primer step ejecuta Extract data
#     States = {
#       "Extract Bank Payments Gmail" = {
#         Type     = "Task",
#         Resource = aws_lambda_function.extract_data_gmail.arn,
#         Catch: [
#           {
#             "ErrorEquals": ["States.ALL"],
#             "ResultPath": "$.error-info",
#             "Next": "CompensationFlow"
#           }
#         ],
#         Next     = "Check If Should Process"
#       },

# 8.1 Creacion del job de PDFs en Step Function
resource "aws_sfn_state_machine" "bank_payments_etl_flow" {
  name     = "bank-payments-etl-flow"
  role_arn = aws_iam_role.step_function_role.arn

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.etl_logs.arn}:*"
  }

  # Steps secuenciales
  definition = jsonencode({
      StartAt = "Check If Should Process",
      States = {
      "Check If Should Process" = {
        Type = "Choice",
        Choices = [
          {
            "Variable": "$.body.process",
            "BooleanEquals": true,
            "Next": "Transform Gmail Bank Payments"
          }
        ],
        Default = "SkipProcessing"
      },
      "SkipProcessing" = {
        Type = "Succeed"
      },
      # Segundo step ejecuta Transform data
      "Transform Gmail Bank Payments" = {
        Type     = "Task",
        Resource = aws_lambda_function.bank_payments_processor.arn,
        Parameters = {
          "key.$": "$.body.key"
        },
        Next     = "Load Gmail Bank Payments",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      # Tercer step ejecuta Load data
      "Load Gmail Bank Payments" = {
        Type     = "Task",
        Resource = aws_lambda_function.load_report_and_pdf.arn,
        Parameters = {
          "etl_flow.$"    = "$.body.etl_flow"
          "bucket.$"      = "$.body.bucket"
          "key.$"         = "$.body.key"
          "report_id.$"   = "$.body.report_id"
          "report_date.$" = "$.body.report_date"
        },
        Next     = "Export Redshift data to BigQuery",
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ]
      },
      "Export Redshift data to BigQuery" = {
        Type     = "Task",
        Resource = aws_lambda_function.redshift_to_bq.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Run Bank Payments Crawler"
      },
      # Ultimo step ejecuta Glue Crawler
      "Run Bank Payments Crawler" = {
        Type     = "Task",
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler",
        Parameters = {
          Name = aws_glue_crawler.bank_payments_crawler.name
        },
        End = true
      },
      # Step compensatorio por si falla algun step del job
      CompensationFlow: {
        "Type": "Task",
        "Resource": "arn:aws:lambda:${var.AWS_REGION}:${var.AWS_ACCOUNT_ID}:function:compensation_flow",
        "End": true
      }
    }
  })
}

########### 9. Glue Data Catalog ###########

resource "aws_glue_catalog_database" "etl_database" {
  name = "etl_database"
}

########### 10. Glue Crawlers ###########

resource "aws_glue_crawler" "market_tickets_crawler" {
  name          = "market-tickets-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name

  table_prefix  = "market_tickets_"

  s3_target {
    path = "s3://${aws_s3_bucket.market_tickets.bucket}/processed/"
  }

  schedule = "cron(0 8 * * ? *)" # Corre todos los días a las 8:00 UTC
}

resource "aws_glue_crawler" "mp_reports_crawler" {
  name          = "mp-reports-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name

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

  schedule = "cron(0 8 * * ? *)" # Corre todos los días a las 8:00 UTC
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
  name          = "bank-payments-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name

  table_prefix  = "bank_payments_"

  s3_target {
    path = "s3://${aws_s3_bucket.bank_payments.bucket}/processed/"
  }

  schedule = "cron(0 8 * * ? *)" # Corre todos los días a las 8:00 UTC
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
