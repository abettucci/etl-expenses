# Balance mensual

La pestaña `/balance` complementa `/panorama` (proyecciones) con resultados
registrados: ingresos, gastos, ahorro neto y tasa de ahorro. El funnel ETL y
Ticket Splitter conviven en la portada con navegación común.

## Fuentes y alcance

- Gastos: el mismo SELECT predefinido y parametrizado de `finance_forecast`,
  con conversión ARS existente. No se consulta ni modifica el warehouse de otra
  cuenta. Se heredan las definiciones y cobertura de `gastos_totales`.
- Ingresos: total mensual manual, **no** inferido de transferencias entrantes.
  Guardar reemplaza el total del mes. Evitar incluir transferencias entre cuentas
  propias. No se escriben ingresos en BigQuery.
- Persistencia: tabla DynamoDB de la app existente,
  `USER#<Cognito sub>` / `FINANCE_INCOME#AAAA-MM`, importe decimal como string.
  No hay IDs de dueño aceptados desde el cliente. Solo el titular configurado
  puede acceder; ninguna nueva política IAM ni infraestructura es necesaria.
- Sin fixtures automáticas ni cache financiero en localStorage. Datos faltantes
  permanecen nulos. Cero ingresos es un valor explícito; no permite tasa porcentual.

## Cálculos

`ahorro = ingresos - gastos registrados`; `tasa = ahorro / ingresos * 100`.
Un ahorro negativo es déficit. No representa saldo bancario ni rentabilidad.
Promedios: 3 o 6 meses completos anteriores al seleccionado, excluyendo campos
faltantes; se informa el denominador de cada promedio. Tasa histórica ponderada:
suma del ahorro / suma de ingresos **solo de meses con ambas fuentes**.
Variación: nominal y porcentual frente al mes calendario anterior y frente al
promedio; denominador cero implica porcentaje indefinido. Para bases negativas
se divide por el valor absoluto de la base. La tasa se compara en puntos
porcentuales. El mes en curso es acumulado parcial, sin variación contra meses
cerrados. Los meses sin gastos registrados no se asumen de gasto cero.

## API y configuración

- `GET /v1/finance/balance`: meses históricos disponibles y mes en curso.
- `PUT /v1/finance/income/AAAA-MM`: JSON exacto
  `{"amount":"1500000.50","currency":"ARS"}`. Requiere sesión Cognito; importe
  entre cero y 999999999999.99, hasta dos decimales; no acepta meses futuros.
- Se reutilizan `FINANCE_FORECAST_ENABLED=true`, `FINANCE_FORECAST_OWNER_SUB`
  (subject Cognito del dueño), configuración BigQuery y credenciales existentes.
  **No se habilita ni se elige automáticamente un dueño.** Si no están
  configurados, el frontend informa que la fuente no está vinculada.
- Los flags se administran con las variables Terraform existentes
  `divi_finance_forecast_enabled` y `divi_finance_forecast_owner_sub`.
  El lookback lo define `FINANCE_FORECAST_LOOKBACK_MONTHS` (default 6).
- El Dockerfile incluye `finance_balance.py`; el workflow existente actualiza
  `divi_app_api` al publicar. No hay migración ni backfill de datos.

## Verificación

```sh
python -m unittest discover -s app_api/tests -v
cd divi_web
npm run build
node --experimental-strip-types --test tests/*.test.mjs
```

Tests sintéticos/mocks: balances negativos, faltantes, cero, meses parciales,
redondeo decimal, validación, sesión, aislamiento de dueño, errores sanitizados
y empaquetado del módulo. No se ejecutan escrituras cloud durante tests.
