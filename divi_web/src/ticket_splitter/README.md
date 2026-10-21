# Ticket Splitter clásico

Se incorporó el frontend del repositorio personal `ticket_splitter`: la portada verde con el ejemplo de chat y la calculadora de divisiones. Los estilos originales se generan con Tailwind 3 y quedan limitados a `#ticket-splitter`, sin modificar el diseño de Mi espacio.

- `/`: portada original, con accesos a la calculadora y a Mi espacio.
- `/dividir`: divisiones rápidas sin registro, participantes, varios pagadores, reparto igual o exacto, balances, pagos, duplicación, exportación y resumen para compartir.
- `/app`: espacio existente de grupos con cuenta, actividad, finanzas, regalos y Premium.

Las divisiones rápidas se guardan en `localStorage` bajo la clave original `splitbot-web-divisions-v1`. No se sincronizan con los grupos de la API ni con otros dispositivos. Los datos guardados en el dominio anterior no son accesibles desde este dominio: podés conservarlos exportándolos desde la aplicación anterior.

La portada muestra un chat ilustrativo. No activa una integración nueva de WhatsApp.

Validación desde `divi_web`:

```sh
npm ci --registry=https://registry.npmjs.org
npm run build
npm run test:splitter
npm run dev -- --host 127.0.0.1
```

Las pruebas de cálculo requieren Node 22.6 o posterior (lectura de TypeScript mediante `--experimental-strip-types`).
