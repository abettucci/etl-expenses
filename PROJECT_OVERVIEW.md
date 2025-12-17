# 💸 ETL de Gastos Personales - Sistema Multi-Cloud Serverless

<div align="center">

![AWS](https://img.shields.io/badge/AWS-10_Services-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-BigQuery_+_PubSub-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)

**Pipeline ETL event-driven que centraliza y procesa automáticamente gastos personales de múltiples fuentes para análisis en tiempo real**

[Ver Arquitectura Completa](./ARCHITECTURE.md) • [Ver Diagrama](#-arquitectura-visual)

</div>

---

## 🎯 ¿Qué Hace Este Sistema?

Sistema ETL serverless que automatiza el tracking de gastos personales desde 3 fuentes:

| Fuente | Método | Frecuencia | Output |
|--------|--------|------------|--------|
| 🏦 **Banco Santander** | Gmail API + Pub/Sub | Tiempo real | JSON → CSV → BigQuery |
| 🛒 **Carrefour (Supermercado)** | PDFs por email + OCR | Tiempo real | PDF → CSV → BigQuery |
| 💳 **MercadoPago** | Webhook + Reports API | Semanal | CSV → BigQuery |

**Resultado:** Base de datos unificada en BigQuery lista para análisis, dashboards (Looker) y consultas por AI Agent (Telegram Bot).

---

## ✨ Características Destacadas

### **🚀 Serverless & Event-Driven**
- 10 Lambda Functions orquestadas con Step Functions
- 0 servidores que mantener
- Costo operativo: ~$1.50/mes

### **🤖 AI-Powered Query Bot**
- Telegram Bot con OpenAI GPT-4o-mini
- Pregunta en lenguaje natural → Genera SQL → Ejecuta en BigQuery
- Ejemplos: *"¿Cuánto gasté este mes?"*, *"Productos más comprados en Carrefour"*

### **🔄 ETL Robusto**
- 3 pipelines independientes con error handling
- Compensation flows automáticos (rollback + alertas)
- Deduplicación inteligente (evita reprocesar datos)

### **📊 Data Governance**
- AWS Glue Data Catalog para schema management
- Crawlers automáticos semanales
- Staging → Production pattern en BigQuery

### **🔐 Seguridad**
- Secrets Manager para credenciales
- Validación criptográfica de webhooks (bcrypt)
- OIDC tokens para Pub/Sub → API Gateway
- S3 con HTTPS-only policies

### **⚙️ CI/CD Completo**
- GitHub Actions con matrix builds (10 imágenes Docker)
- Terraform para IaC (importa recursos existentes)
- Deploy automático en cada push a main
- Optimización: solo pushea imágenes que cambiaron

---

## 🏗️ Stack Tecnológico

### **Cloud Services**

<table>
<tr>
<td valign="top" width="50%">

#### AWS (Compute & Storage)
- **Lambda** (10 funciones)
- **Step Functions** (orquestación ETL)
- **API Gateway** (REST endpoints)
- **S3** (data lake: raw + processed)
- **ECR** (imágenes Docker)
- **DynamoDB** (state management)
- **CloudWatch + SNS** (monitoreo)
- **EventBridge** (cron jobs)
- **Glue** (data catalog)

</td>
<td valign="top" width="50%">

#### GCP (Data & APIs)
- **BigQuery** (data warehouse)
- **Pub/Sub** (event streaming)
- **Gmail API** (acceso a emails)
- **Service Accounts** (auth)

#### External Services
- **OpenAI API** (GPT-4o-mini)
- **Telegram Bot API**
- **MercadoPago API**

</td>
</tr>
</table>

### **Tech Stack**
- **Lenguaje:** Python 3.11+
- **IaC:** Terraform
- **CI/CD:** GitHub Actions
- **Containers:** Docker (multi-stage builds)
- **Librerías clave:** pandas, pdfplumber, boto3, google-cloud-bigquery, openai, rapidfuzz

---

## 🔄 Arquitectura Visual

### **Diagrama Simplificado**

```
┌─────────────────────────────────────────────────────────────────┐
│                    FUENTES DE DATOS                              │
├─────────────────────────────────────────────────────────────────┤
│  📧 Gmail        🛒 Carrefour PDFs      💳 MercadoPago Webhook  │
└────────┬───────────────────┬────────────────────┬───────────────┘
         │                   │                    │
         ▼                   ▼                    ▼
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │ Pub/Sub │         │ Pub/Sub │         │ Webhook │
   │  Bank   │         │ Market  │         │   MP    │
   └────┬────┘         └────┬────┘         └────┬────┘
        │                   │                    │
        └───────────────────┴────────────────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │  API Gateway    │
                   │  4 endpoints    │
                   └────────┬────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
   ┌─────────┐       ┌─────────┐       ┌─────────┐
   │  Bank   │       │  PDF    │       │   MP    │
   │   ETL   │       │  ETL    │       │  ETL    │
   │ Step Fn │       │Step Fn  │       │Step Fn  │
   └────┬────┘       └────┬────┘       └────┬────┘
        │                 │                  │
        │    Extract      │     Extract      │    Extract
        ├────►Lambda      ├────►Lambda       ├────►Lambda
        │                 │                  │
        │   Transform     │    Transform     │   Transform
        ├────►Lambda      ├────►Lambda       ├────►Lambda
        │                 │                  │
        └─────────────────┴──────────────────┘
                          │
                          │ Load
                          ▼
                    ┌───────────┐
                    │ BigQuery  │
                    │ STG → PRD │
                    └─────┬─────┘
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
        ┌──────────┐            ┌──────────┐
        │  Looker  │            │ AI Agent │
        │Dashboard │            │ Telegram │
        └──────────┘            └──────────┘
```

### **Flujos Principales**

#### **1️⃣ Gastos Bancarios (Real-time)**
```
Gmail → Pub/Sub → API Gateway → extract_data_gmail
                                        ↓
                            Guarda JSON en S3 (raw/)
                                        ↓
                    Step Function: bank-payments-etl-flow
                                        ↓
                transform_data_bank_pay (parsea HTML)
                                        ↓
                            Guarda CSV en S3 (processed/)
                                        ↓
                        load_data (BigQuery: STG → PRD)
```

#### **2️⃣ Tickets Supermercado (Real-time)**
```
Gmail con PDF → Pub/Sub → API Gateway → extract_data_gmail
                                                ↓
                                    Descarga PDF desde URL
                                                ↓
                                    Guarda en S3 (raw/)
                                                ↓
                            Step Function: pdf-etl-flow
                                                ↓
                    transform_data_pdf (OCR + parsing)
                                                ↓
                                    Guarda CSV en S3 (processed/)
                                                ↓
                                load_data (BigQuery: STG → PRD)
```

#### **3️⃣ MercadoPago (Semanal)**
```
MP genera reporte → Webhook → API Gateway → webhook_mp_report
                                                    ↓
                                        Valida firma bcrypt
                                                    ↓
                                Step Function: mp-report-etl-flow
                                                    ↓
                    mp_report_extractor (descarga desde API)
                                                    ↓
                                        Guarda CSV en S3 (raw/)
                                                    ↓
                    transform_data_mp (normaliza columnas)
                                                    ↓
                                    Guarda CSV en S3 (processed/)
                                                    ↓
                                    load_data (BigQuery: STG → PRD)
```

---

## 🤖 AI Agent - Demo

**Ejemplo de Conversación:**

```
👤 Usuario: ¿Cuánto gasté en Carrefour este mes?

🤖 Bot:
📝 Generando consulta SQL...

📊 Resultados:
---
*total_gastado:* $ 45.320,50
*cantidad_compras:* 8
*ticket_promedio:* $ 5.665,06
```

```
👤 Usuario: Mostrame los 5 productos que más compro

🤖 Bot:
📊 Resultados:
---
*producto:* Leche Serenisima 1L
*veces_comprado:* 24
---
*producto:* Pan Lactal Bimbo
*veces_comprado:* 18
---
*producto:* Yogur Ser Firme
*veces_comprado:* 16
...
```

---

## 📊 Modelo de Datos

### **Tablas en BigQuery (PRD)**

#### **bank_payments** (Gastos bancarios)
```sql
id: STRING                  -- Hash MD5 único
message_id: STRING          -- ID del email en Gmail
fecha_pago: STRING          -- Formato DD/MM/YYYY
hora_pago: STRING
tarjeta: STRING             -- Tipo de tarjeta
nro_tarjeta: STRING         -- Últimos 4 dígitos
comercio: STRING
cuotas: INT64
monto: FLOAT64
divisa: STRING              -- ARS, USD
extraido_en: TIMESTAMP
```

#### **carrefour_data** (Detalle productos)
```sql
nro_ticket: INT64           -- PK
fecha: STRING
categoria: STRING
producto: STRING
cantidad: FLOAT64
peso: FLOAT64
precio_unit: FLOAT64
monto_total: FLOAT64
ean: STRING
product_id: INT64           -- FK a dim_producto
grupo_producto: STRING      -- Fuzzy matching
total_ticket_bruto: FLOAT64
total_ticket_meli: FLOAT64  -- 30% del total
```

#### **mp_data** (Transacciones MercadoPago)
```sql
REPORT_ID: STRING           -- PK
REPORT_DATE: DATE
SOURCE_ID: STRING
TRANSACTION_TYPE: STRING
TRANSACTION_AMOUNT: FLOAT64
PAYMENT_METHOD: STRING
INSTALLMENTS: INT64
SETTLEMENT_NET_AMOUNT: FLOAT64
FEE_AMOUNT: FLOAT64
STORE_NAME: STRING
... (60+ columnas más)
```

#### **dim_producto** (Catálogo de productos)
```sql
product_id: INT64           -- PK (autoincremental)
nombre_producto: STRING
ean: STRING
grupo_producto: STRING      -- Nombre normalizado (fuzzy match)
```

#### **archivos_ingestados** (Control de duplicados)
```sql
id: STRING                  -- nro_ticket o message_id
ins_dttm: TIMESTAMP
```

---

## 🔐 Seguridad Implementada

### **Autenticación y Autorización**

| Componente | Mecanismo | Descripción |
|------------|-----------|-------------|
| **Gmail → Pub/Sub** | OIDC Tokens | Pub/Sub push con JWT validado en Lambda |
| **MP Webhook** | bcrypt signature | Hash de `{transaction_id}-{secret}-{date}` |
| **BigQuery** | Service Account | Credenciales JSON en Secrets Manager |
| **Telegram Bot** | Bot Token | Token privado en env vars |
| **OpenAI** | API Key | Key privada en env vars |

### **Gestión de Secretos**

```
AWS Secrets Manager:
├── gcp_api_credentials        (OAuth2 User Account)
└── gcp_sa_api_credentials     (Service Account JSON)

AWS Parameter Store:
└── /mercado_pago/token        (Access Token)

Lambda Environment Variables:
├── TELEGRAM_BOT_TOKEN
├── OPENAI_API_KEY
├── CIFRADO_SECRET_MP
└── GCP_PROJECT_ID
```

### **IAM Best Practices**

- ✅ Least privilege principle (roles con permisos mínimos)
- ✅ No hardcoded credentials
- ✅ Secrets rotation (refresh tokens automático)
- ✅ S3 buckets con HTTPS-only policy
- ✅ VPC no necesario (serverless + secure by default)

---

## 📈 Métricas y Observabilidad

### **CloudWatch Alarms**

```yaml
Alarms configuradas:
  - pdfFailures: Errores en pdf-etl-flow
  - mpFailures: Errores en mp-report-etl-flow
  - bank_payment_Failures: Errores en bank-payments-etl-flow

Notificaciones:
  - SNS Topic: stepfunction-alerts
  - Email subscription configurado
  - Compensation flow automático en caso de error
```

### **Logs Centralizados**

```
/aws/lambda/extract_data_gmail
/aws/lambda/transform_data_pdf
/aws/lambda/transform_data_mp
/aws/lambda/transform_data_bank_pay
/aws/lambda/load_data
/aws/lambda/ai_agent
/aws/vendedlogs/states/etl-logs  (Step Functions)
```

**Retention:** 14 días (configurable)

---

## 💰 Análisis de Costos

### **Costo Mensual Estimado: ~$1.53**

| Servicio | Costo | Justificación |
|----------|-------|---------------|
| **AWS Lambda** | $0.00 | Free tier: 1M requests + 400K GB-segundo |
| **API Gateway** | $0.00 | Free tier: 1M requests |
| **S3** | $0.05 | ~500 MB de datos |
| **DynamoDB** | $0.05 | On-demand, bajo volumen |
| **ECR** | $0.10 | ~285 MB de imágenes |
| **Secrets Manager** | $0.80 | 2 secrets × $0.40 |
| **Glue Crawlers** | $0.20 | 3 crawlers × 1 min/semana |
| **Step Functions** | $0.00 | Free tier: 4K transitions |
| **CloudWatch** | $0.00 | Free tier: 5 GB logs |
| **BigQuery** | $0.00 | Free tier: 10 GB storage + 1 TB queries |
| **Pub/Sub** | $0.00 | Free tier: 10 GB/mes |
| **OpenAI API** | $0.03 | GPT-4o-mini (~50K tokens) |
| **Telegram Bot** | $0.00 | Gratis |

**Optimizaciones:**
- ✅ Comparación de hashes de imágenes Docker (evita pushes innecesarios)
- ✅ Cache de esquemas en DynamoDB (reduce queries a BigQuery)
- ✅ MERGE incremental (no full refresh)
- ✅ Lifecycle policy en ECR
- ✅ Lambda memory tuning (512-1024 MB)

---

## 🚀 CI/CD Pipeline

### **GitHub Actions Workflow**

```yaml
Trigger: push a main o fix-version-rollback-n3

Jobs:
  1. build-lambda-images (paralelo):
     - Matrix build de 10 imágenes Docker
     - Compara SHA256 local vs remote
     - Solo pushea si cambió
     - Actualiza Lambda function code

  2. terraform-deploy (secuencial):
     - Import de ~50 recursos AWS/GCP
     - Terraform apply -auto-approve
     - Configura webhook de Telegram
```

**Optimización destacada:** Matrix strategy reduce tiempo de build de ~15 min a ~3 min (builds paralelos).

---

## 📚 Casos de Uso Reales

### **1. Análisis Financiero Personal**

**Pregunta:** *¿Estoy gastando más de lo habitual este mes?*

**Query generada por AI:**
```sql
SELECT 
  FORMAT_DATE('%Y-%m', CURRENT_DATE()) as mes_actual,
  SUM(monto) as total_gastado,
  (SELECT AVG(total_mensual) 
   FROM (
     SELECT FORMAT_DATE('%Y-%m', PARSE_DATE('%d/%m/%Y', fecha_pago)) as mes,
            SUM(monto) as total_mensual
     FROM `hazel-pillar-400222.PRD.bank_payments`
     WHERE PARSE_DATE('%d/%m/%Y', fecha_pago) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
     GROUP BY 1
   )) as promedio_6_meses
FROM `hazel-pillar-400222.PRD.bank_payments`
WHERE PARSE_DATE('%d/%m/%Y', fecha_pago) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
```

### **2. Optimización de Compras**

**Pregunta:** *¿Qué productos compro que tienen descuento más frecuentemente?*

**Análisis:** Cruza `carrefour_data` con historial de precios para detectar patrones de descuentos estacionales.

### **3. Presupuesto y Metas**

**Pregunta:** *¿Cuánto me falta para mi meta de ahorro de $50.000?*

**Query:** Suma gastos del mes, resta de ingreso fijo, compara con meta.

---

## 🛠️ Setup y Deployment

### **Pre-requisitos**

1. Cuenta AWS con permisos de administrador
2. Cuenta GCP (free tier suficiente)
3. Terraform 1.5.6+
4. Docker
5. GitHub account

### **Variables Requeridas (GitHub Secrets)**

```env
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_ACCOUNT_ID
AWS_REGION
GCP_PROJECT_ID
EMAIL
TELEGRAM_BOT_TOKEN
OPENAI_API_KEY
CIFRADO_SECRET_MP
SNS_TOPIC
ECR_REPO
```

### **Deployment**

```bash
# 1. Push a branch main
git push origin main

# 2. GitHub Actions ejecuta automáticamente:
#    - Build de 10 imágenes Docker
#    - Push a ECR
#    - Terraform apply
#    - Configuración de Telegram webhook

# 3. Verificar deployment
aws lambda list-functions --query "Functions[?starts_with(FunctionName, 'extract_') || starts_with(FunctionName, 'transform_') || starts_with(FunctionName, 'load_')].[FunctionName,LastModified]"
```

---

## 🔮 Roadmap Futuro

### **Corto Plazo (1-3 meses)**

- [ ] **Categorización automática con ML:** Clasificar gastos por categoría usando NLP
- [ ] **Alertas proactivas:** "Gastaste 50% más este mes en X categoría"
- [ ] **Integración con más bancos:** BBVA, Galicia, etc.

### **Mediano Plazo (3-6 meses)**

- [ ] **App móvil:** Flutter app para iOS/Android
- [ ] **Predicción de gastos:** ML model para forecast mensual
- [ ] **Comparación con usuarios similares:** Benchmark anónimo

### **Largo Plazo (6-12 meses)**

- [ ] **Multi-currency support:** Conversión automática a moneda base
- [ ] **Inversiones tracking:** Integración con APIs de brokers
- [ ] **Gamification:** Badges por metas de ahorro

---

## 📞 Contacto

**Agustín Bettucci**

- 💼 [LinkedIn](https://www.linkedin.com/in/abettucci)
- 📧 Email: abettucci@example.com
- 🐙 GitHub: [@abettucci](https://github.com/abettucci)

---

## 📄 Documentación Adicional

- 📖 [Arquitectura Completa (ARCHITECTURE.md)](./ARCHITECTURE.md) - Detalles técnicos profundos
- 📝 [README Técnico (README.md)](./README.md) - Guía original del proyecto
- 🔧 [Terraform Files](./main.tf) - Configuración de infraestructura
- 🐳 [Dockerfiles](./extract_data_gmail/) - Imágenes de Lambdas

---

<div align="center">

### ⭐ Si te gustó este proyecto, considera darle una estrella!

**Built with ❤️ using serverless technologies**

*AWS Lambda • BigQuery • Terraform • Docker • Python • OpenAI*

</div>

