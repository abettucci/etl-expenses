<h1 align="center">Weekly Expenses ETL jobs</h1>

<div align="center">
  Proceso ETL productivo en AWS que extrae PDFs de tickets de supermercado de Gmail a través de la API de Gmail y extrae reportes de gastos de Mercado Pago programados a través de un webhook a la API de Mercado Pago, los procesa y los carga en una tabla de una base de datos de Redshift.
</div>
<br>

## Construido con 

- Backend (Lambda): Python
- Testing de endpoints: Postman.
- Versionado: Github.
- CI/CD: Docker y Github Actions.
- Deployment: Terraform.
- Step Functions: para articular la ejecucion de las funciones Lambda.
- Data governance y data quality: Glue Data Catalog y Glue Crawler.
- API: HTTP API Gateway para recibir webhook de Mercado Pago.
- Almacenamiento y actualización de tokens de forma segura: Secret Manager, Parameter Store.
- Bucket S3: almacenamiento de archivos raw descargados y archivos transformados.
- Almacenamiento de filtros (a futuro): Dynamo DB.
- API de Gmail - GCP para obtener credenciales.
- API de Mercado Pago.
- Monitoreo:
  - Alertas de CloudWatch
  - Notificacion por mail con SNS
    
## Como lo realicé

Todo el flujo es orquestado por Step Functions de AWS el cual ejecuta 3 funciones lambda distintas. 

El job de Step Functions que extrae y carga los datos de los pdfs recibidos en Gmail es disparado por un cron schedule.
El job ejecuta una primera función Lambda que cumple el rol de “Extract” y extrae los PDFs a través de la API de Gmail, se valida que no se hayan cargado ya esos PDFs y luego se los carga en un bucket de S3 en caso de no estar repetido. 
Luego se ejecuta una segunda funcion Lambda como “Transform” que transforma los pdfs almacenados en la carpeta del bucket en S3 en dataframes para normalizar los datos y facilitar su manipulacion y agregacion. 
Por ultimo se ejecuta la funcion Lambda de “Load” que ejecuta validaciones de datos con la tabla productiva en Redshift y en caso de superar los checks, carga los datos en la tabla.

El job de Step Functions que extrae y carga los datos de los reportes de Mercado Pago es disparado por el webhook de Mercado Pago que está atado a la configuracion definida en la pagina de Mercado Pago para generar los reportes automaticamente. Cada vez que se crea automaticamente un reporte en Mercado Pago, se envia un POST request a traves del webhook y es recibido por una API Gateway que ejecuta una funcion lambda “Dispatcher” para extraer los metadatos del reporte, que luego son descargados a traves de un GET request a la API de Mercado Pago de reportes.

El despliegue de los servicios utilizados de AWS lo realicé con Terraform.


## Links utiles

1. <a href="https://www.mercadopago.com.ar/developers/es/reference">Mercado Pago API Docs</a> - Documentacion de endpoints de las distintas APIs de Mercado Pago, consideraciones de diseño, novedades de las APIs y guias de comienzo y autenticacion.
2. <a href="https://aws.amazon.com/es/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all">AWS Pricing</a> - AWS Free Tier para calculo de costos.
   
## Instalaciones requeridas

1. Crear una app en el <a href="https://www.mercadopago.com.ar/developers/panel/app">Dev Center</a> de Mercado Pago.
2. Crear una cuenta nominal en Google Cloud Platform, crear unas credenciales de Oauth2. Habilitar la API de Gmail, descagar las credenciales en formato JSON y ejecutar el script en bash "gcp_token_offline.sh" que lo que hace es generar un access token y un refresh token que luego se van a utilizar al intentar usar la API de Gmail. El script genera un file que lo almacenamos en Secrets Manager de AWS.


## Consideraciones de diseño

1. Cree una regla en ECR para eliminar imagenes no tagueadas que se ejecuta por AWS y evita sobrecostos por cada pusheo de una imagen actualizada que deja una imagen vieja sin taguear en el repositorio.
2. Estrategia de construccion de imagenes en Docker: dado que todas las funciones lambda de los dos ETL comparten las mismas librerias y algunas funciones agregan 1 o 2 librerias diferentes nada mas, decidi crear una imagen “lambda-base” y luego al crear las imagenes de cada funcion referenciar a esa imagen e instalar encima de esa imagen las librerias extras correspondientes. Esto no ocupa espacio en memoria ya que se reutiliza una imagen ya creada.
3. Como debía construir y pushear varias imágenes de Docker, decidi utilizar una matriz para automatizar esta tarea y que se utilicen los nombres de los dockerfiles de un listado dado. 
4. Comparacion de imagen Docker remota y local para evitar resubir imagenes que no cambiaron al repositorio de ECR.
5. La orquestacion es realizada en Step Functions y no Glue debido al bajo volumen de datos a procesar.
6. Validación de archivos ya ingestados en S3: para evitar abrir y parsear un PDF que ya fue procesado primero se guarda un registro del nombre o hash del PDF del contenido binario. en una tabla de archivos ingestados en Redshift y se verifica antes de procesar si el hash ya existe es que ya se cargó ese archivo y se lo saltea. 
7. Data governance con Glue Data Catalog para mantener un catálogo centralizado y detectar esquemas, tener descripciones, tipos de datos, ubicaciones y auditar cambios.
8. Glue Crawlers recorren rutas de S3 y registran o actualizan tablas en el Glue Data Catalog. Glue Crawlers mantienen los metadatos actualizados sin intervención manual. Al finalizar cada ETL, se ejecuta el crawler de cada ETL que escanea el bucket correspondiente  y actualiza los metadatos automaticamente.
9. Monitoreo de la orquestación de Step Functions en Cloudwatch
10. Alertado a traves de SNS suscrito a gmail.
11. Flujo compensatorio: en caso de que falle la descarga del PDF de Gmail o la carga de datos a Redshift, se ejecuta una funcion lambda como flujo compensatorio que hace un rollback de los cambios temporales realizados en S3 y Redshift. 

## Consideraciones de costos de AWS

1. API Gateway de AWS cuenta con un free tier de 1 millon de llamados gratis por mes.
2. Elastic Container Registry cuenta con un free tier hasta 500 MB por mes en el total de imagenes almacenadas (esto es clave a la hora de seleccionar las librerias que no sean tan pesadas, actualmente la imagen del backend tiene un peso de 285 MB).  El costo por el excedente es de 0,1 USD/GB adicional. Por cada actualizacion del codigo en Github se ejecuta Github Actions que es un CI/CD externo a AWS y hace un rebuild de la imagen y un push a ECR, por esta transferencia de datos de Github a AWS se cobra 0,01 USD por cada GB de datos transferidos, es decir, si la imagen pesa 250 MB aprox, se necesitan 40 actualizaciones de codigo para que se cobre 0,01 USD. Por ultimo, Lambda siempre utiliza una imagen cacheada de ECR por lo que no tiene costo esa conexion, salvo que se haga un push de una iamgen nueva. En ese caso, Lambda debe hacer un nuevo pull de la iamgen y por cada 1000 requests de pull se cobra 0,001 USD.
3. Lambda Function cuenta con un free tier de 1 millon de llamados gratis por mes. Luego se cobra 0,20 USD por cada millón de solicitudes excedentes y 0,0000166667 USD por cada GB/segundo de procesamiento hasta llegar a los primeros 6 mil millones de GB/segundo por mes.
4. Parameter Store cuenta con un free tier por el servicio estandar, sin embargo, se cobra 0,05 USD por cada 10.000 interacciones de la API, o sea por cada 10.000 autenticaciones que se realizan.
5. 12 meses gratis: estas ofertas de la capa gratuita están disponibles exclusivamente para los nuevos clientes de AWS y solo durante doce meses a partir de la fecha de inscripción en AWS. Cuando finalicen los 12 meses de uso gratuito o si el uso de su aplicación supera las capas, tendrá que pagar las tarifas de servicio estándar por uso (consulte la página de cada servicio para obtener información completa sobre los precios). Existen restricciones; consulte las condiciones de la oferta para obtener más detalles.
6. Gratis para siempre: estas ofertas de la capa gratuita no vencen automáticamente al finalizar los 12 meses de la capa gratuita de AWS, sino que están disponibles tanto para clientes ya existentes como para nuevos clientes de AWS de forma indefinida.
