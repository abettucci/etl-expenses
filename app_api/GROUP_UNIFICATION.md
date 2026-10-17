# Divi Groups: migración desde SplitBot

`etl_expenses` es la plataforma canónica. SplitBot no se fusiona por Git ni se
mezcla su Terraform: se importa su información validada a la tabla de Divi.

## Matriz de paridad y orden de corte

| Capacidad | Divi en esta rama | Regla de convivencia |
| --- | --- | --- |
| Grupos, integrantes e invitaciones | API y web de lectura/alta disponibles | SplitBot sigue siendo la fuente de escritura de grupos no migrados |
| Gastos iguales, por monto, porcentaje, partes e ítems | API con aritmética decimal exacta | Validar contra un export real antes de habilitar el bot nuevo |
| Varios pagadores, balances y liquidaciones | API disponible; web muestra balances/liquidaciones | No se marcan pagos históricos como resueltos sin reconciliación |
| Vinculación de identidad Telegram | Código efímero + `/vincular` | Todo integrante legado entra inicialmente como pendiente |
| Regalos compartidos/pools | Modelo y API iniciales | No se mueve dinero ni se habilita cobro hasta completar integración de pagos |
| UX de grupos | Crear grupo, integrantes, gastos y liquidaciones conectados a `/v1` | Requiere `VITE_API_URL`, sesión Cognito y datos reales; no simula persistencia |
| Paridad completa de comandos SplitBot | Pendiente | No hay corte ni cambio de webhook hasta pruebas de regresión y reconciliación |

El siguiente bloque es importar un export saneado de SplitBot en un entorno de
staging, comparar conteos/importes/balances por grupo y recién entonces agregar
los comandos que falten al bot `Divi Groups`.

## Límites deliberados

- El bot personal de ETL no cambia. Su token, audio, fotos, BigQuery y alertas
  siguen aislados del bot `Divi Groups`.
- Divi usa importes decimales exactos. Los `float64` del export de SplitBot se
  normalizan a dos decimales y se deben reconciliar antes de cualquier corte.
- Las listas de regalos compartidas son entidades genéricas. No se transforman
  en cumpleaños, reservas privadas ni cobros Mercado Pago.
- El migrador nunca escribe sobre la tabla legado y corre en `dry_run` salvo
  que se pase explícitamente `--apply` con `APP_TABLE` configurada.

## Flujo de migración

1. Exportar un JSON saneado de SplitBot con `groups`, `members`, `expenses`,
   `splits`, `payments`, `gift_lists`, `gift_items` y `contributions`. El
   exportador sólo lee la tabla origen y el archivo resultante está ignorado:

   ```bash
   cd app_api
   python scripts/export_splitbot.py --source-table splitbot-prod --region us-east-1 \
     --output ../splitbot-export-2026-10-14.json
   ```
2. Validar localmente:

   ```bash
   cd app_api
   python scripts/migrate_splitbot.py --input ./splitbot-export.json
   ```

3. Comparar conteos, importes, balances, regalos y aportes por grupo.
4. Aplicar sólo en una ventana controlada:

   ```bash
   APP_TABLE=divi-application python scripts/migrate_splitbot.py --input ./splitbot-export.json --apply
   ```

5. Los miembros llegan como pendientes. La persona inicia sesión en Divi,
   solicita `POST /v1/identity-links` y le envía `/vincular CODIGO` al nuevo
   bot Divi Groups desde su cuenta de Telegram.

## Nuevo webhook

Terraform crea el secreto `divi/group-bot` y el endpoint público exacto
`POST /v1/group-bot/telegram`. La función valida
`X-Telegram-Bot-Api-Secret-Token` antes de procesar el update. Esta primera
tanda de comandos incluye vinculación, `/grupos` y, en chats de grupo ya
vinculados, `/saldo` y `/gastos`. El bot no invoca ETL ni escribe a BigQuery.
Configurar el webhook del nuevo token sólo después de desplegar y probar en
staging.

Variables de CI necesarias:

- `DIVI_GROUP_BOT_TOKEN`
- `DIVI_GROUP_BOT_WEBHOOK_SECRET`

La paridad completa de comandos de SplitBot debe completarse y probarse antes
de mover grupos activos a modo escritura; durante la convivencia sólo SplitBot
puede escribir en cada grupo no migrado.
