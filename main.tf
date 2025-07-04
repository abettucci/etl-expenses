terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-2"
}

########### 0. Definicion de Variables ###########

# Definimos las variables que van a utilizar algunos recursos para referenciar a las ARN
variable "aws_account_id" {
  description = "AWS Account ID"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS REGION"
  type        = string
  sensitive   = true
}

variable "email" {
  description = "email"
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

########### 2. Redshift Serverless ###########
# Creamos el namespace
resource "aws_redshiftserverless_namespace" "etl_namespace" {
  namespace_name = "pdf-etl-namespace"
  db_name        = "dev"
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

########### 4. Lambdas basadas en imágenes Docker ###########
# 4.1 Lambda para extraer PDFs de Gmail
resource "aws_lambda_function" "pdf_extractor" {
  function_name = "pdf_extractor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:pdf_extractor-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos

  environment {
    variables = {
      WORKGROUP_NAME = aws_redshiftserverless_workgroup.etl_workgroup.workgroup_name
      BUCKET_NAME    = aws_s3_bucket.market_tickets.bucket
    }
  }
}

# 4.2 Lambda para transformar PDFs de Gmail
resource "aws_lambda_function" "pdf_processor" {
  function_name = "pdf_processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:pdf_processor-latest"

  memory_size = 2048  # Más memoria para procesar PDFs
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

# 4.5 Lambda para cargar los dos ETLs a tablas productivas de Redshift (reportes de Mercado Pago y pdfs de Gmail)
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

# 4.6 Lambda Dispatcher que extrae los datos del body del POST request del webhook de reportes de MP y dispara el step function de MP
resource "aws_lambda_function" "dispatcher" {
  function_name = "dispatcher"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:webhook_mp_report-latest"

  memory_size = 512
  timeout     = 30

  environment {
    variables = {
      STEP_FUNCTION_ARN = aws_sfn_state_machine.mp_report_etl_flow.arn
    }
  }
}

# 4.7 Lambda Compensation flow que limpia archivos temporales y el envia marca de que el proceso fallo por mail
resource "aws_lambda_function" "compensation_flow" {
  function_name = "compensation-flow"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda_images.repository_url}:compensation_flow-latest"
  
  memory_size = 1024  # Ajustar según necesidades
  timeout     = 900   # Máximo 15 minutos
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
        Resource = "arn:aws:secretsmanager:us-east-2:${var.aws_account_id}:secret:gcp_api_credentials-*"
      }
    ]
  })
}

# Policy para conectar las Lambda a: 1) las tablas de Redshift, 2) las imagenes de ECR, 3) los buckets de S3 y 4) las Step Functions.
resource "aws_iam_role_policy" "lambda_redshift_access" {
  name = "lambda_redshift_access"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "redshift-data:*",
          "redshift-data:ExecuteStatement",
          "redshift-data:GetStatementResult",
          "redshift:GetClusterCredentials",
          "redshift:Describe*",
          "redshift-serverless:*",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ],
        Effect   = "Allow",
        Resource = "*"
      },
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
          aws_s3_bucket.market_tickets.arn
        ]
      },
      {
        Effect = "Allow",
        Action = "states:StartExecution",
        Resource = aws_sfn_state_machine.mp_report_etl_flow.arn
      }
    ]
  })
}

# Policy para eliminar imagenes que no estan dentro de los tags de las funciones Lambda de los dos jobs
resource "aws_ecr_lifecycle_policy" "delete_unwanted_images" {
  repository = aws_ecr_repository.lambda_images.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Eliminar imágenes que no sean las últimas válidas"
        selection = {
          tagStatus = "tagged"
          tagPrefixList = [
            "pdf_extractor-latest",
            "pdf_processor-latest",
            "load_report_and_pdf-latest",
            "mp_report_extractor-latest",
            "mp_report_processor-latest",
            "webhook_mp_report-latest",
            "compensation_flow-latest",
            "lambda-base"
          ]
          countType   = "imageCountMoreThan"
          countNumber = 1 # Mantener solo la última versión de cada una
        }
        action = {
          type = "expire"
        }
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
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.market_tickets.arn,
          "${aws_s3_bucket.market_tickets.arn}/*",
          aws_s3_bucket.mp_reports.arn,
          "${aws_s3_bucket.mp_reports.arn}/*"
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
          aws_lambda_function.pdf_extractor.arn,
          aws_lambda_function.pdf_processor.arn,
          aws_lambda_function.mp_report_extractor.arn,
          aws_lambda_function.mp_report_processor.arn,
          aws_lambda_function.load_report_and_pdf.arn
        ]
      }
    ]
  })
}

# Policy para la Step Function para que se pueda ejecutar Glue Crawler en el ultimo step del job
resource "aws_iam_role_policy" "step_function_glue_permissions" {
  name = "step_function_glue_permissions"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:StartCrawler"
        ],
        Resource = [
          aws_glue_crawler.market_tickets_crawler.arn,
          aws_glue_crawler.mp_reports_crawler.arn
        ]
      },
      {
        Effect = "Allow",
        Action = "states:StartExecution",
        Resource = [
          aws_sfn_state_machine.pdf_etl_flow.arn,
          aws_sfn_state_machine.mp_report_etl_flow.arn
        ]
      }
    ]
  })
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
# 7.1 Cron schedule para ejecutar el ETL de los PDFs de Gmail
resource "aws_cloudwatch_event_rule" "weekly_monday_schedule" {
  name                = "etl_step_function_schedule"
  description         = "Ejecuta la Step Function cada lunes a las 7:00 AM UTC"
  schedule_expression = "cron(0 7 ? * MON *)"
}

# 7.2 Attachment de cron schedule de Cloudwatch a la Step Function de PDFs de Gmail
resource "aws_cloudwatch_event_target" "trigger_pdf_etl" {
  rule      = aws_cloudwatch_event_rule.weekly_monday_schedule.name
  target_id = "TriggerPDFETL"
  arn       = aws_sfn_state_machine.pdf_etl_flow.arn
  role_arn  = aws_iam_role.step_function_role.arn
}

# 7.3 Creacion de grupo de logging de los ETLs
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

  # Steps secuenciales
  definition = jsonencode({
    StartAt = "Extract Gmail PDFs",
    # Primer step ejecuta Extract data
    States = {
      "Extract Gmail PDFs" = {
        Type     = "Task",
        Resource = aws_lambda_function.pdf_extractor.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Transform Gmail PDFs"
      },
      # Segundo step ejecuta Transform data
      "Transform Gmail PDFs" = {
        Type     = "Task",
        Resource = aws_lambda_function.pdf_processor.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Load Gmail PDFs"
      },
      # Tercer step ejecuta Load data
      "Load Gmail PDFs" = {
        Type     = "Task",
        Resource = aws_lambda_function.load_report_and_pdf.arn,
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
        "Resource": "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:compensation-flow",
        "End": true
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
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Transform MP Reports"
      },
      # Segundo step ejecuta Transform data
      "Transform MP Reports" = {
        Type     = "Task",
        Resource = aws_lambda_function.mp_report_processor.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Load MP Reports"
      },
      # Tercer step ejecuta Load data
      "Load MP Reports" = {
        Type     = "Task",
        Resource = aws_lambda_function.load_report_and_pdf.arn,
        Catch: [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error-info",
            "Next": "CompensationFlow"
          }
        ],
        Next     = "Run MP Reports Crawler"
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
        "Resource": "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:compensation-flow",
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

  s3_target {
    path = "s3://${aws_s3_bucket.market_tickets.bucket}/raw/"
  }

  schedule = "cron(0 8 * * ? *)" # Corre todos los días a las 8:00 UTC
}

resource "aws_glue_crawler" "mp_reports_crawler" {
  name          = "mp-reports-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.etl_database.name

  s3_target {
    path = "s3://${aws_s3_bucket.mp_reports.bucket}/raw/"
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
  endpoint  = "${var.email}"
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