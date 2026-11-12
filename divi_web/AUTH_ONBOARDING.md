# Acceso y conexiones: correcciones y activación

## Qué cambia

- Inicio OAuth con PKCE S256, `state` y `nonce` aleatorios, scopes explícitos y retorno a un destino interno permitido. El estado pendiente vence a los 10 minutos.
- Los callbacks solo se procesan en el dominio/path configurado. Se limpian `code`, `state` y errores de la URL antes de hacer llamadas. Un error o cancelación muestra explicación y reintento, no una portada silenciosa.
- Los deploys de preview llevan primero al dominio canónico para iniciar el flujo; no crean PKCE en un origen distinto al del callback.
- Los tokens quedan en `sessionStorage` (esta pestaña), no indefinidamente en `localStorage`. La primera visita tras actualizar exige volver a ingresar para reemplazar tokens legados.
- El token se renueva con `refresh_token` al expirar o ante un 401, con una renovación compartida entre llamadas concurrentes y un único reintento. Un 403 de permisos **no** cierra la sesión. Una caída de red permite reintentar; un refresh revocado obliga a ingresar.
- Cerrar sesión elimina los tokens locales y abre el logout de Cognito para terminar también su sesión alojada. No es una función de revocación de todas las sesiones/dispositivos.
- Decodificar el JWT en el navegador solo orienta expiración/UX. No verifica su firma ni concede permisos: API Gateway valida emisor, audiencia y firma; el backend mantiene el control del dueño financiero.
- Conexiones permite identificar **tu propia cuenta** mediante `/v1/me` y preparar su habilitación. No hay botón que otorgue acceso a un warehouse ajeno.
- Telegram de grupos permite generar el código real mediante `POST /v1/identity-links`. Los nuevos códigos tienen 192 bits aleatorios, vencen a los 15 minutos y no se registran en la auditoría. La vinculación de cuentas nuevas sin grupos legados es atómica y rechaza un Telegram previamente vinculado a otra cuenta. El comando se acepta solo por chat privado.

## Configuración pendiente de producción

Estos cambios no habilitan servicios ni publican infraestructura por sí solos. El workflow ahora pasa a Terraform valores que antes no estaban conectados:

| Tipo en GitHub | Nombre | Valor |
|---|---|---|
| Secret | `DIVI_FINANCE_FORECAST_OWNER_SUB` | ID Cognito del propietario **confirmado** del historial ETL |
| Variable | `DIVI_FINANCE_FORECAST_ENABLED` | `true`, solo después de confirmar el dueño; por defecto `false` |
| Variable | `DIVI_FINANCE_BOT_USERNAME` | Usuario público del bot financiero, sin `@` |
| Variable | `DIVI_GROUPS_BOT_USERNAME` | Usuario público del bot de grupos, sin `@` |

El ID se obtiene desde Conexiones → Preparar vinculación. No es una contraseña. Solo un administrador debe configurarlo como dueño. No inferirlo del primer usuario registrado ni desactivar el control de ownership para resolver un 403.

Los usernames no reemplazan los tokens/webhooks existentes ni autorizan un chat financiero. El bot de grupos requiere que su webhook esté desplegado y configurado. Sus códigos se envían en privado y no deben compartirse ni aparecer en capturas públicas.

## Límites que siguen vigentes

- Gmail y Binance tienen conexiones personales independientes del ETL administrado. Gmail importa compras Santander compatibles y Binance consulta saldos y movimientos con permisos de lectura; ver `app_api/USER_CONNECTIONS.md` para sus límites y configuración. Otros bancos y Balanz aún no tienen conector autoservicio.
- El bot de finanzas personales sigue separado del bot de grupos. La vinculación Telegram de grupos no habilita BigQuery ni destinos de alertas financieras.
- `lang=es` solicita español en Cognito **managed login**. La Hosted UI clásica puede seguir en inglés; no se cambió de plan/tier ni se contrataron servicios para localizarla.
- El almacenamiento en el navegador sigue expuesto ante XSS en ese origen. Para una solución con cookies HttpOnly hace falta un BFF; no se presenta este cambio como sustituto de esa arquitectura.
- Las pruebas OAuth son con un proveedor sintético. El ingreso real después de introducir credenciales necesita una sesión de prueba autorizada; no se usaron contraseñas ni tokens reales.

## Pruebas

Desde el repositorio:

```sh
python -m unittest discover -s app_api/tests -q
```

Desde `divi_web`:

```sh
npm test
npx tsc -b
npm run build
```

Para pruebas locales del recorrido OAuth, compilar un artefacto separado con el callback local y mantener bloqueados/mocked los proveedores:

```sh
VITE_COGNITO_REDIRECT_URI=http://localhost:5178/auth/callback npx vite build --outDir /tmp/finance-onboarding-qa-build
npx vite preview --host 127.0.0.1 --port 5178 --outDir /tmp/finance-onboarding-qa-build
# En otra terminal, con Playwright y Chromium instalados:
UNIFIED_WEB_URL=http://localhost:5178 PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p test_unified_browser.py -v
```

Nunca publicar el artefacto QA como build productivo. El `dist` normal conserva el callback productivo. En un worktree con `node_modules` de solo lectura, añadir `--configLoader runner` a Vite.
