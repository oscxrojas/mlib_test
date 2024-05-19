# Prueba técnica DE Mercado Libre
~~WTF! What does DE really mean? I think it means Donut Expert, anyway...~~<br><br>
Candidato: **Oscar Rojas**<br>
País: **Colombia 🇨🇴**

## Base script
El script base para esta prueba se encuentra en `src/main.py`. Al correr este proceso, el resultado se almacenará en `src/outputs`.

## Hallazgos
1. Existen registros que muestran pagos que no se relacionan con ningún click.
2. En entornos digitales, puerta abierta 24 horas, existe la probabilidad de que una impresión del día anterior cause una compra el día presente; más que nada en momentos cercanos a la media noche. Conservar la hora de los eventos podría se util; de igual manera, conocer el id de la impresión que correlacione los datos podría ayudarnos en esta tarea.

## Ambigüedades en el requerimiento
> [!WARNING]<br>
> En un ambiente real, estas dudas deben aclararse antes de inicar el desarrollo.
1. El requerimiento no es claro cuando se refiere a la última semana; se asume que se trata de la última semana móvil, entendiendose que también se podría referir a la última semana calendario (iniciando domingo o lunes), o semana financiera propia de cada organización.
2. Aúnque creo que es claro, pero ya que seguridad mató a confianza, me hubiese gustado tener la certeza de que las 3 semanas que se enuncian en este requerimiento se toman desde el día inmediatamente anterior al de la iteración.


## Decisiones técnicas
1. Quizá sea la primera pregunta de quien vea este código: ¿Polars?... Polars es una librería disponible para Python y Rust, escrita en Rust. Esta librería ha demostrado un mejor rendimiento en tiempo de ejecución y uso de memoria a través de varios benchmark disponibles en la web.
2. Aunque podría imaginar la volumetría de datos que maneja este negocio, para esta prueba decidí implementar una lectura perezosa para jsonl y csv; se estableció un tamaño del batch de 1024 por defecto.
3. Creé algunos datasets de prueba con una cantidad de datos pequeña que represente algunos de los casos que mi mente, ignorante de conocimientos de negocio especificos para MeLi, se imaginó.
4. En cuanto a la escritura de los archivos resiltantes, se implementó una escritura en formato parquet, con un particionamiento a través de la fecha del evento, y un ordenamiento ascendenta para las columnas "value_prop" e "impressions", a fin de optimizar el consumo de almacenamiento. En la practica, deberia hacerse un analisis de que particionamiento y ordenamiento nos da un mejor rendimiento en aspectos como: escritura, almacenamiento y lectura.
5. También se implementó una escritura en archivo excel para leer más cómodamente el resultado de esta prueba.

## Pensamientos finales
1. La prueba esta interesante.
