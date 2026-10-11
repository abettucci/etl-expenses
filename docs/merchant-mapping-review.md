# Revisión asistida de comercios

Este flujo evita que `comercio_unmapped_queue` crezca con una fila por
transacción y saca la clasificación automática del camino crítico del ETL.

## Flujo

1. `load_report_and_pdf` aplica primero las reglas activas de
   `dim_comercio_mapping`, incluidos los tipos `exact`, `contains`, `regex` y
   `fuzzy` ya existentes.
2. Los comercios todavía no clasificados se consolidan por
   `flow + comercio_normalizado` en `comercio_mapping_review_queue`.
3. Sólo los candidatos nuevos se publican en SQS. Un error de SQS nunca hace
   fallar la carga financiera.
4. `ai_agent` intenta, en orden:
   - reutilizar un mapeo confirmado mediante similitud determinística;
   - obtener una sugerencia estructurada con la taxonomía existente;
   - consultar Google Places únicamente como fallback opcional.
5. Telegram envía un mensaje por comercio para confirmar, corregir o rechazar.

Una coincidencia única con una regla confirmada puede aplicarse
automáticamente sólo con confianza `>= 95%` y margen `>= 10` puntos. Las
sugerencias de IA o Places siempre requieren revisión humana. Google Places no
escribe su respuesta en BigQuery: sólo se conserva el `place_id` y la
clasificación interna derivada.

## Subcategoría opcional

`categoria` continúa siendo obligatoria; `subcategoria` puede estar vacía y se
persiste como `NULL`. Ejemplos:

```text
/mapear_comercio bank|UDEMY ONLINE COURSE|Udemy|Educación
/mapear_comercio bank|UDEMY ONLINE COURSE|Udemy|Educación||exact
```

Durante una corrección por Telegram también se acepta “quitá la subcategoría”
o `subcategoría: ninguna`.

## Operación

Para reprocesar el backlog de forma acotada:

```text
/sugerir_mapeos 10 all
/sugerir_mapeos 10 bank
/sugerir_mapeos 10 mp
```

El máximo por comando es 25. La cola tiene DLQ y procesa un candidato por
invocación para aislar reintentos.

Variables nuevas:

- `MAPPING_REVIEW_TABLE`
- `MAPPING_REVIEW_STATE_TABLE`
- `MERCHANT_SUGGESTIONS_QUEUE_URL`
- `MERCHANT_SUGGESTIONS_ENABLED`
- `MERCHANT_AUTO_APPLY_ENABLED`
- `MERCHANT_PLACES_ENABLED`
- `GOOGLE_PLACES_API_KEY` (opcional, provista desde un secret de GitHub)

Si `GOOGLE_PLACES_API_KEY` está vacía, el fallback externo queda desactivado
aunque `MERCHANT_PLACES_ENABLED=true`.
