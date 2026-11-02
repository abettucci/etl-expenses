# Un solo producto

La portada explica finanzas personales y gastos compartidos con la misma identidad verde del ETL. El funnel conserva su composición y su CSS; el cambio en su cabecera es de navegación/copy. La calculadora anónima conserva modelo, operaciones y almacenamiento local, con colores del producto.

Al ingresar se abre **Inicio**, salvo que se haya solicitado un destino explícito (por ejemplo, una invitación a un grupo). La navegación tiene tres destinos principales:

- **Inicio**: resumen mensual personal y grupos reales. Sus importes no se suman entre sí.
- **Mis finanzas**: Balance mensual y Proyección/recurrentes como navegación interna.
- **Grupos**: integrantes, gastos y liquidaciones de cada grupo.

Conexiones y el asistente son herramientas comunes. Mi perfil/Ajustes contienen datos de cuenta, no otro dashboard. Ajustes conserva los accesos al checkout de suscripción y a la conexión Mercado Pago existentes, sin cambiar su backend. En móvil, Inicio/Mis finanzas/Grupos/Más forman la navegación inferior. “Más” incluye enlaces a conexiones, cuenta, calculadora y portada.

## Datos y límites

Se reutilizan las APIs existentes de balance, proyección, grupos y perfil. Esta refactorización no agrega conectores, cambia permisos, habilita fuentes ni despliega infraestructura. Las fuentes financieras deben estar previamente habilitadas para la cuenta. Las proyecciones `preview` no se muestran como dinero real y los errores tienen estados explícitos. No se calcula un saldo global mezclando monedas o gastos personales/compartidos.

Las antiguas pantallas de demostración (actividad/billetera/regalos con valores fijos) no son destinos del workspace real: sus enlaces antiguos llevan a Mis finanzas o Grupos. Esto no modifica los datos ni las APIs de regalos. Una pantalla real de regalos pendiente de integración no se presenta como funcional.

Rutas canónicas: `/inicio`, `/finanzas/balance`, `/finanzas/proyeccion`, `/grupos`, `/conexiones`, `/perfil`, `/ajustes`, `/dividir`, `/como-funciona`. Alias: `/app`, `/balance`, `/panorama`, `/etl`. El botón Atrás del navegador funciona entre secciones.

## Verificación local

Desde `divi_web`:

```sh
npm test
npx tsc -b
npm run build
npm run preview -- --host 127.0.0.1 --port 5178
```

Pruebas de navegador opcionales, con Python + Playwright/Chromium instalados y el preview abierto:

```sh
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p test_unified_browser.py -v
```

`UNIFIED_WEB_URL` cambia el URL local (por defecto `http://127.0.0.1:5178`). Todos los pedidos externos se interceptan y los fixtures son sintéticos; no se usan credenciales ni servicios productivos. Se prueban portada/funnel/calculadora, sesión simulada, rutas antiguas, balance interno, perfil separado, errores, ausencia de datos y tamaños 320/768/1440.

En worktrees que reutilizan `node_modules` mediante un enlace de solo lectura, usar `node node_modules/vite/bin/vite.js build --configLoader runner` y `preview --configLoader runner` evita el caché de configuración en ese directorio. No hay nuevas variables de producción.
