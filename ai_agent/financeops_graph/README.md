# FinanceOps Copilot (POC)

FinanceOps Copilot explora cómo ayudar a freelancers y microempresas a entender sus movimientos de dinero, investigar variaciones y preparar correcciones de categorización de forma segura.

## Arquitectura

`FinanceOpsCopilot` es un `StateGraph` con los nodos: normalizar intención, planificar una llamada tipada, ejecutar una herramienta read-only, solicitar aprobación humana cuando existe un borrador y construir una respuesta final.

El estado conserva la pregunta, intención, períodos, evidencia, borrador, aprobación y auditoría de ejecución. Usa `InMemorySaver`: es útil sólo para desarrollo local y **no sobrevive a reinicios**. Una versión productiva debería inyectar un checkpointer durable y un store auditable, tras definir retención, acceso por usuario y cifrado. Este POC no crea infraestructura.

## Datos reales y simulados

El modo predeterminado (`FINANCEOPS_DATA_MODE=synthetic`) usa sólo `fixtures.py`. No llama AWS, Telegram, BigQuery ni APIs externas.

`FINANCEOPS_DATA_MODE=bigquery` habilita opcionalmente un adaptador de BigQuery exclusivamente de lectura. Requiere Application Default Credentials o `GOOGLE_APPLICATION_CREDENTIALS`, además de `GCP_PROJECT_ID`. Usa plantillas `SELECT`/`WITH` predefinidas y parámetros; no acepta SQL generado por un modelo, no usa Secrets Manager y no expone operaciones de escritura.

Variables opcionales para ese modo: `BQ_LOCATION` (por defecto `US`), `BQ_DATASET_PROD` (por defecto `PRD`) y `FINANCEOPS_GASTOS_VIEW` (por defecto `gastos_totales`).

## Herramientas y conocimiento

Las herramientas estructuradas consultan datos financieros mediante contratos Pydantic: resumen, comparación, transacciones sin categorizar/sospechosas y conteo de impacto. La recuperación tipo RAG sólo consulta el corpus local de políticas, diccionario de datos y ejemplos de mapeo. Separar ambos caminos evita que conocimiento textual produzca SQL o cambios financieros.

`draft_category_mapping` crea un objeto en memoria. LangGraph usa `interrupt()` para devolver un payload JSON con evidencia, razón y cantidad afectada. `approve` sólo asigna `approved_in_simulation`; `reject` descarta el borrador; `edit` muestra un borrador revisado y requiere una aprobación posterior. Ninguna decisión persiste datos.

## Ejecutar

```bash
cd ai_agent
python -m pip install -r financeops_graph/requirements.txt
python -m financeops_graph.demo --scenario software-increase
python -m financeops_graph.demo --scenario uncategorized
python -m financeops_graph.demo --scenario draft-mapping
python -m unittest discover -s financeops_graph/tests -p 'test_*.py'
```

Para lectura opcional de BigQuery:

```bash
export FINANCEOPS_DATA_MODE=bigquery
export GCP_PROJECT_ID=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/read-only-service-account.json
python -m financeops_graph.demo --scenario software-increase
```

No uses una cuenta de servicio con permisos de escritura. El adaptador está diseñado para lectura, pero el principio de menor privilegio también debe imponerse en IAM.
