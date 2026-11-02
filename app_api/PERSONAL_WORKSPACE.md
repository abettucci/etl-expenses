# Espacio personal y agente web

Después de “Ingresar”, el retorno predeterminado es `/perfil`. Allí se muestran
nombre/correo de la sesión, edición del nombre, dashboard real, guía de fuentes,
Telegram y acceso al asistente. `/app` también muestra el perfil en lugar del
grupo de demostración. Los flujos explícitos de grupos conservan su destino.

## Conexiones honestas

`GET /v1/profile/overview` responde solo información de la cuenta autenticada.
Gmail/bancos y reportes Mercado Pago siguen siendo pipelines administrados:
esta pantalla no implementa OAuth individual de Gmail ni importa cuentas
bancarias por sí sola. Balanz/Binance están señalados como no disponibles.
No se capturan contraseñas o API keys. “Fuente habilitada” indica permiso de
cuenta, **no** un diagnóstico de última sincronización.

Bots: configurar los nombres públicos sin `@` con las variables Terraform
`divi_finance_bot_username` y `divi_groups_bot_username`. Solo se construyen
enlaces `https://t.me/<username>` válidos; nunca se exponen tokens. La guía
distingue el bot financiero del de grupos. La autorización del chat y el
destino de alertas del ETL siguen administrados por las variables existentes.
No se simulan preferencias de notificación ni una vinculación automática.

## Mismo agente, canal web autenticado

`POST /v1/finance/assistant`, cuerpo exacto `{"question":"..."}`:

- Usa identidad Cognito validada por API Gateway; solo acepta al dueño ETL
  configurado con `FINANCE_FORECAST_ENABLED` y `FINANCE_FORECAST_OWNER_SUB`.
- Invoca exclusivamente la Lambda configurada en
  `FINANCE_AGENT_FUNCTION_NAME`, nunca una función indicada por el usuario.
- Cuota atómica DynamoDB de 30 solicitudes por hora y cuenta. No guarda preguntas
  ni respuestas. El historial del widget es efímero y las preguntas independientes.
- El adaptador interno `finance_web_question` vuelve a verificar dueño y flag
  `WEB_FINANCE_ENABLED`/`WEB_FINANCE_OWNER_SUB`; rechaza eventos HTTP.
- Reutiliza generación y formato del agente de Telegram, sin enviar mensajes
  Telegram y sin activar el manejo de tickets, ediciones o callbacks.
- Una generación de IA, sin reintentos del proveedor para web, timeout 10 s;
  BigQuery valida SELECT y tablas del proyecto/dataset, resultado máximo 20 filas,
  límite de facturación 100 MB y espera de resultados web de 10 s.
- El chat devuelve texto plano, nunca SQL, HTML activo, trazas ni errores internos.
  El proveedor de IA existente puede cobrar esta llamada; no se agregan modelos.

Terraform declara el permiso de invocación sobre la Lambda exacta y conecta
los flags de ambos servicios. No se deben publicar datos privados a todas las
cuentas para resolver una configuración faltante. Estos cambios de infraestructura
requieren el despliegue habitual; no se aplican desde tests.

## Respuesta de Telegram

Un valor único se muestra como importe (o valor) sin “1 fila / columnas”. NULL
no se transforma en cero. En el incidente del 01/11, CloudWatch confirmó un filtro
de octubre **2023** ante una pregunta de octubre sin año. Ahora el prompt lleva
fecha argentina y límites del mes más reciente; una fecha literal de otro año
se rechaza antes de ejecutar. Pedidos explícitos de otros años se respetan.

Pruebas: `python -m unittest discover -s app_api/tests`,
`python -m unittest discover -s ai_agent -p test_result_presenter.py`,
`npm run build` y `npm test` desde `divi_web`. No usan datos reales ni invocan IA,
BigQuery, Telegram o escrituras cloud.
