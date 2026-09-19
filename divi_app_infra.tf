# Divi application layer. It is intentionally separate from main.tf: current
# Gmail/Telegram ETL endpoints and their deployment stay untouched.

variable "divi_frontend_bucket_name" {
  type = string
}

variable "divi_frontend_origin" {
  type    = string
  default = "http://localhost:5173"
}

variable "divi_google_client_id" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_google_client_secret" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_cognito_domain_prefix" {
  type    = string
  default = "divi-expenses-app"
}

variable "divi_mp_access_token" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_mp_webhook_secret" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_mp_premium_plan_id" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_mp_oauth_client_id" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_mp_oauth_client_secret" {
  type      = string
  default   = ""
  sensitive = true
}

variable "divi_mp_oauth_redirect_uri" {
  type      = string
  default   = ""
  sensitive = true
}

locals {
  divi_google_enabled      = var.divi_google_client_id != "" && var.divi_google_client_secret != ""
  divi_identity_providers = local.divi_google_enabled ? ["COGNITO", "Google"] : ["COGNITO"]
}

resource "aws_dynamodb_table" "divi_application" {
  name         = "divi-application"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = {
    Name      = "divi-application"
    Component = "transactional-app"
  }
}

resource "aws_cognito_user_pool" "divi" {
  name                     = "divi-users"
  username_attributes      = ["email"]
  auto_verified_attributes = ["email"]
  mfa_configuration        = "OFF"

  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_uppercase = true
    require_numbers   = true
    require_symbols   = false
  }

  schema {
    attribute_data_type = "String"
    name                = "name"
    required            = true
    mutable             = true

    string_attribute_constraints {
      min_length = 1
      max_length = 80
    }
  }
}

resource "aws_cognito_identity_provider" "google" {
  count         = local.divi_google_enabled ? 1 : 0
  user_pool_id  = aws_cognito_user_pool.divi.id
  provider_name = "Google"
  provider_type = "Google"

  provider_details = {
    client_id        = var.divi_google_client_id
    client_secret    = var.divi_google_client_secret
    authorize_scopes = "openid email profile"
  }

  attribute_mapping = {
    email    = "email"
    name     = "name"
    username = "sub"
  }
}

resource "aws_cognito_user_pool_client" "divi_web" {
  name                                 = "divi-web"
  user_pool_id                         = aws_cognito_user_pool.divi.id
  generate_secret                      = false
  supported_identity_providers         = local.divi_identity_providers
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["openid", "email", "profile"]
  callback_urls                        = ["${var.divi_frontend_origin}/auth/callback"]
  logout_urls                          = ["${var.divi_frontend_origin}/"]
  explicit_auth_flows                  = ["ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
  prevent_user_existence_errors        = "ENABLED"

  depends_on = [aws_cognito_identity_provider.google]
}

resource "aws_cognito_user_pool_domain" "divi" {
  domain       = var.divi_cognito_domain_prefix
  user_pool_id = aws_cognito_user_pool.divi.id
}

resource "aws_secretsmanager_secret" "divi_mercadopago" {
  name        = "divi/mercadopago"
  description = "Mercado Pago credentials for Divi application payments"
}

resource "aws_secretsmanager_secret_version" "divi_mercadopago" {
  secret_id = aws_secretsmanager_secret.divi_mercadopago.id
  secret_string = jsonencode({
    access_token        = var.divi_mp_access_token
    webhook_secret      = var.divi_mp_webhook_secret
    premium_plan_id     = var.divi_mp_premium_plan_id
    oauth_client_id     = var.divi_mp_oauth_client_id
    oauth_client_secret = var.divi_mp_oauth_client_secret
    oauth_redirect_uri  = var.divi_mp_oauth_redirect_uri
  })
}

resource "aws_iam_role_policy" "divi_application" {
  name = "divi-application-store-and-payments"
  role = aws_iam_role.lambda_exec.id

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
          "dynamodb:Scan",
          "dynamodb:TransactWriteItems"
        ]
        Resource = aws_dynamodb_table.divi_application.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.divi_mercadopago.arn
      }
    ]
  })
}

resource "aws_lambda_function" "divi_app_api" {
  function_name = "divi_app_api"
  description   = "Authenticated API for Divi groups, gifts, pools and Premium"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:app_api-latest"
  timeout       = 30
  memory_size   = 512

  environment {
    variables = {
      APP_TABLE           = aws_dynamodb_table.divi_application.name
      FRONTEND_ORIGIN     = var.divi_frontend_origin
      MP_SECRET_ARN       = aws_secretsmanager_secret.divi_mercadopago.arn
      MP_WEBHOOK_BASE_URL = aws_apigatewayv2_api.divi.api_endpoint
    }
  }

  depends_on = [
    aws_iam_role_policy.divi_application,
    aws_secretsmanager_secret_version.divi_mercadopago
  ]
}

resource "aws_apigatewayv2_api" "divi" {
  name          = "divi-app-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = [var.divi_frontend_origin]
    allow_methods = ["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"]
    allow_headers = ["authorization", "content-type"]
    max_age       = 86400
  }
}

resource "aws_apigatewayv2_integration" "divi" {
  api_id                 = aws_apigatewayv2_api.divi.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.divi_app_api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_authorizer" "divi" {
  api_id           = aws_apigatewayv2_api.divi.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  name             = "divi-cognito"

  jwt_configuration {
    audience = [aws_cognito_user_pool_client.divi_web.id]
    issuer   = "https://${aws_cognito_user_pool.divi.endpoint}"
  }
}

resource "aws_apigatewayv2_route" "divi_authenticated" {
  api_id             = aws_apigatewayv2_api.divi.id
  route_key          = "ANY /v1/{proxy+}"
  target             = "integrations/${aws_apigatewayv2_integration.divi.id}"
  authorization_type = "JWT"
  authorizer_id      = aws_apigatewayv2_authorizer.divi.id
}

resource "aws_apigatewayv2_route" "divi_payment_webhook" {
  api_id             = aws_apigatewayv2_api.divi.id
  route_key          = "POST /v1/webhooks/mercadopago/payments"
  target             = "integrations/${aws_apigatewayv2_integration.divi.id}"
  authorization_type = "NONE"
}

resource "aws_apigatewayv2_route" "divi_subscription_webhook" {
  api_id             = aws_apigatewayv2_api.divi.id
  route_key          = "POST /v1/webhooks/mercadopago/subscriptions"
  target             = "integrations/${aws_apigatewayv2_integration.divi.id}"
  authorization_type = "NONE"
}

resource "aws_apigatewayv2_route" "divi_oauth_callback" {
  api_id             = aws_apigatewayv2_api.divi.id
  route_key          = "GET /v1/mercadopago/oauth/callback"
  target             = "integrations/${aws_apigatewayv2_integration.divi.id}"
  authorization_type = "NONE"
}

resource "aws_lambda_permission" "divi_api" {
  statement_id  = "AllowDiviHttpApi"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.divi_app_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.divi.execution_arn}/*/*"
}

resource "aws_apigatewayv2_stage" "divi" {
  api_id      = aws_apigatewayv2_api.divi.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_s3_bucket" "divi_frontend" {
  bucket = var.divi_frontend_bucket_name

  tags = {
    Name = "divi-frontend"
  }
}

resource "aws_s3_bucket_public_access_block" "divi_frontend" {
  bucket                  = aws_s3_bucket.divi_frontend.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudfront_origin_access_control" "divi" {
  name                              = "divi-frontend-oac"
  description                       = "CloudFront-only access to Divi SPA"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_distribution" "divi" {
  enabled             = true
  default_root_object = "index.html"

  origin {
    domain_name              = aws_s3_bucket.divi_frontend.bucket_regional_domain_name
    origin_id                = "divi-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.divi.id
  }

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "divi-s3"
    viewer_protocol_policy = "redirect-to-https"
    cache_policy_id        = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  }

  custom_error_response {
    error_code         = 403
    response_code      = 200
    response_page_path = "/index.html"
  }

  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/index.html"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_s3_bucket_policy" "divi_frontend" {
  bucket = aws_s3_bucket.divi_frontend.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.divi_frontend.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.divi.arn
        }
      }
    }]
  })
}

output "divi_api_url" {
  value = aws_apigatewayv2_api.divi.api_endpoint
}

output "divi_frontend_url" {
  value = "https://${aws_cloudfront_distribution.divi.domain_name}"
}

output "divi_cognito_issuer" {
  value = "https://${aws_cognito_user_pool.divi.endpoint}"
}

output "divi_cognito_client_id" {
  value = aws_cognito_user_pool_client.divi_web.id
}
