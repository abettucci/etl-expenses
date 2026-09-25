# Cotización USD/ARS BCRA

Esta Lambda guarda la serie **USD oficial minorista vendedor** del BCRA en
`PRD.fx_usd_ars_bcra`. Para gastos en USD, usa la última cotización publicada
en o antes de la fecha de la transacción; esto cubre fines de semana y feriados.

El EventBridge diario sincroniza los últimos siete días y convierte las filas
nuevas cuyo `monto_ars` todavía sea nulo. Luego del primer despliegue, ejecutar
una vez el backfill completo desde una consola con AWS CLI:

```bash
aws lambda invoke --function-name exchange_rates_bcra --cli-binary-format raw-in-base64-out --payload '{"action":"backfill_expenses","from":"2016-01-01"}' fx-backfill-output.json
```

El backfill agrega, cuando corresponde, `tipo_cambio_ars`,
`fecha_tipo_cambio`, `monto_ars` y `fuente_tipo_cambio` a las tablas de gastos.
No modifica `MONTO`/`IMPORTE` ni `DIVISA`/`currency`, que siguen siendo los
valores originales. Las filas ARS reciben `monto_ars = monto original` y
`fuente_tipo_cambio = ORIGINAL_ARS`.
