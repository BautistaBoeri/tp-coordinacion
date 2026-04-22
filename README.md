# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe

### Coordinación entre instancias de Sum

Las instancias de `Sum` actúan como *competing consumers* sobre una única cola de trabajo (`input_queue`), de modo que cada registro `(fruta, cantidad)` lo procesa exactamente una réplica. Cada réplica mantiene un estado parcial por cliente (`amount_by_fruit_by_client`), acumulando su porción de los datos.

El problema es que el EOF de un cliente llega a **una sola** réplica por la naturaleza de la cola de trabajo, mientras el resto conserva estado que también debe volcarse. Para resolverlo se usa un **exchange fanout** (`{SUM_PREFIX}_flush`) al que cada réplica se suscribe mediante una cola propia `{SUM_PREFIX}_flush_{ID}`. Cuando una réplica recibe el EOF de cliente, publica una notificación de *flush* en ese exchange; todas las réplicas la reciben y vuelcan su estado parcial para ese `client_id`, enviando luego su propio EOF hacia Aggregation.

Ambos consumidores (trabajo y *flush*) comparten **un único canal de Pika con `prefetch_count=1`**, lo que garantiza serialización: no puede entregarse un *flush* mientras un registro de datos está siendo procesado, evitando la race condition donde una réplica volcaba estado antes de procesar un mensaje ya desencolado.

### Coordinación entre instancias de Aggregation

Cada `Sum`, al volcar su estado, particiona sus resultados por fruta mediante `hash(fruta) % AGGREGATION_AMOUNT` y los envía a la instancia de Aggregation correspondiente a través de un **direct exchange** con una routing key por réplica. Así, cada fruta queda asignada determinísticamente a una única instancia de Aggregation, eliminando procesamiento redundante.

Los EOFs, en cambio, se envían a **todas** las instancias de Aggregation. Cada Aggregation lleva un contador `eof_count_by_client` y recién cuando recibe `SUM_AMOUNT` EOFs para un cliente considera cerrada la ingesta de ese cliente, calcula su top parcial y lo envía al `Join`.

El `Join`, a su vez, espera `AGGREGATION_AMOUNT` tops parciales por cliente antes de consolidar y enviar el top final al gateway.

### Escalabilidad

- **Respecto a los clientes**: el estado de todos los filtros (`Sum`, `Aggregation`, `Join`) está indexado por `client_id`, por lo que varios clientes pueden ser procesados en el pipeline en paralelo sin interferencia.

- **Respecto a la cantidad de controles**: `SUM_AMOUNT` y `AGGREGATION_AMOUNT` son variables de entorno del `docker-compose`. Al cambiar sus valores:
  - Las réplicas de `Sum` se balancean solas gracias a la cola compartida.
  - Las réplicas de `Aggregation` se direccionan automáticamente porque Sum computa el índice con el módulo sobre `AGGREGATION_AMOUNT` y publica con la routing key correspondiente.
  - `Join` ajusta dinámicamente cuántos parciales espera usando `AGGREGATION_AMOUNT`, y cada Aggregation cuántos EOFs espera usando `SUM_AMOUNT`.
