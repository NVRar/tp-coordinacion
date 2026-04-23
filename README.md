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

## Informe de resolución

### Sincronización de Sums y Aggreagators 

Las instancias de sum comparten una única work queue. RabbitMQ distribuye los mensajes que pueden ser de DATA o EOF, el asunto es que desde el gateway se envía un único EOF que llega a un único Sum, y es ahí donde tiene que existir un mecanismo de coordinación entre Sums para avisarse que desde el gateway llegó un EOF.

Cada paquete de data viene con un client_id y como esos paquetes puede que se repartan en varias instancias diferentes de Sums, es necesario que todas puedan recibir el EOF de ese client_id para poder mandar su info a los aggregators y eliminarla de su memoria.
Para lograr esto, hice que un Sum, mediante un hilo, consuma de la input_queue y categorice el mensaje: si es de DATA lo agrega; si es de EOF, lo pushea al exchange de control. Desde otro hilo se consumen los datos de ese exchange, y al momento de recibir un EOF ahí se toman los datos del cliente, se los manda al aggregator correspondiente, y lo elimina de la memoria del Sum.

De esto último que nombré se desprenden situaciones: la primera es que tenía que asegurarme de que el hilo que procesa el EOF no lo haga mientras se esté procesando DATA porque de esa manera se perdería esa DATA y estaría mal. Esto surge porque por un lado se maneja info entrante del gateway pero por otro info de control que te dice que tenés que flushear la info. Ahí es donde como primera instancia al notar que dos hilos están guardando, obteniendo y registrando sobre el mismo lugar, se necesita coordinación, por eso opté por crear un monitor para estas operaciones y surgió el FruitStore. Con eso pensé en un principio que cubría que sea thread-safe el tema del manejo de la info y además la coordinación entre DATA y EOF se resolvía. 

Pero luego me di cuenta que el monitor venía a resolver lo thread-safe pero en sí la coordinación que yo necesitaba era de "momentos de trabajo": no podés estar procesando un posible paquete de DATA y como todavía no estás en el add (que es seguro), el lock del monitor no está tomado y desde el proceso de flushear sí te deja obtener los datos o eliminarlos. Es decir, resolví lo de acceso seguro a los datos, no así resolver ese momento todavía. Por eso terminé agregando un lock, que desde el hilo consumidor de la input_queue lo tomo al momento inmediato de llamada del método, entonces ahí digo no se puede procesar un EOF, todavía no sé si es DATA o no, pero entre que está deserializando y demás no se puede flushear, porque así me evito que por ejemplo esté deserializando y el otro hilo haga el flush. Y ahí entendí que ese lock venía a dar turnos de comportamiento en sí, y que por eso no alcanzaba con un monitor solamente.

### Detalles de comunicación entre Sums y Aggreagators

Otro asunto que nombré al inicio es que desde el hilo que flushea se mandan los datos al aggregator correspondiente. Ahí quiero hacer zoom en la explicación porque aclara la parte de coordinación y trabajo entre sums con aggregators. La idea es que el sum, para saber a qué aggregator mandarle los datos, hashea y saca módulo, así le da un número de aggregator a quién mandarle. Es decir, sabemos que todas las frutas "manzana" de todos los clientes van a caer siempre al mismo aggregator, y capaz las "peras" a otro aggregator. En sí no hay coordinación entre aggregators porque cada uno recibe todos los datos de frutas en particular, y sabemos por diseño que no pueden caer datos de cualquier fruta en dos o más aggregators. 
Desde el lado de los Sums, tenemos un exchange por cada aggregator que existe. Se conectan todos los sums al exchange y solo un aggregator específico a ese exchange. Esto lo hice de esta manera porque al método send() de mi middleware no se le puede especificar qué routing_key queremos, entonces termina mandándole a todas las colas asociadas, por eso es que termino creando AGGREGATION_AMOUNT de exchanges. Así que cualquier instancia de sum puede conectarse con el aggregator que debe conectarse.

Además los aggregators pasaron a esperar por SUM_AMOUNT EOFs, una vez que reciben todos procesan su top parcial y lo envían al Joiner. Esto funciona porque cada instancia de Sum al recibir el control EOF, siempre envía su EOF a todos los Aggregators independientemente de si tenía datos de ese cliente o no. Así el Aggregator sabe con certeza que cuando llegaron SUM_AMOUNT EOFs, ya recibió toda la información posible y puede calcular su top parcial y mandarlo.
De la misma forma, el Joiner espera AGGREGATION_AMOUNT EOFs antes de integrar los resultados parciales y enviarle el ranking definitivo al Gateway.
De esta manera para ambos nos garantizamos que tenemos todos los datos antes de mandar la info al siguiente.

### Detalles de mi implementación

Luego me di cuenta de un posible problema debido a los retardos de la red. Existe la posibilidad de que un paquete de DATA de un cliente enviado desde el Gateway tenga que ir al Sum0 pero este sufra retraso en la red y quede "en vuelo". Si a continuación el Gateway despacha el paquete EOF de ese mismo cliente hacia el nodo Sum1, y viaja más rápido, el Sum1 procesará el EOF y hará el broadcast hacia el exchange de control.
El problema ocurre si este mensaje de control vuelve a ser rápido y llega al Sum0 antes que el paquete de DATA original que todavía sigue en vuelo. En este escenario, el Sum0 procesaría el EOF primero, realizando el flush de la información para ese cliente, y cuando el paquete de DATA finalmente llegue al Sum0, la memoria ya habrá sido vaciada.

Para este TP entiendo que, por estar en la misma red local en la PC y no exponerse a internet, es poco probable que tarde más en ir un paquete del Gateway al Sum0, que ir del Gateway al Sum1, procesarlo, reenviarlo y que le llegue al Sum0. Pero sí es algo a considerar en otros contextos. 

Una posible solución sería que solo un Sum consuma de la input_queue y él distribuya los paquetes a los demás Sums, si distribuye en base a un hasheo por client_id, solo a ese Sum que recibe los datos de ese cliente específico le importaría su EOF. El problema de esto es que podría pasar que quede un único Sum trabajando con ese cliente y tener otras instancias de Sums que no están realizando trabajo en ese momento. 

Por eso se pueden explorar más opciones analizando su debido trade-off, y creo que para el alcance de este TP se puede tolerar ese posible escenario ya que es muy improbable dentro de nuestro entorno. 

### Cómo escala el sistema?
El sistema puede atender múltiples clientes de forma concurrente porque todos los mensajes llevan un client_id. Cada Sum, Aggregator, y el Join mantiene su estado indexado por client_id, por lo que los datos de distintos clientes nunca se mezclan y fluyen entre ellos de forma independiente.
Agregar o sacar instancias de Sum o Aggregator no requiere cambios en el código. Los valores SUM_AMOUNT y AGGREGATION_AMOUNT se leen de variables de entorno, por lo que alcanza con modificar el docker-compose.yaml.

- Sum: Al agregar más instancias de Sum, se acelera el procesamiento de los paquetes provenientes del Gateway ya que consumen mediante fair dispatch de la misma cola work_queue. Es decir, se distribuye en más instancias el mismo trabajo. Escala horizontalmente.

- Aggregator: Al aumentar los Aggregators, la variedad de frutas posibles se divide en un universo mucho mayor porque la función de hash que distribuye saca módulo del AGGREGATION_AMOUNT actual. En la práctica, esto significa que la carga se reparte y que cada Aggregator va a procesar en memoria una menor variedad de frutas, agilizando los ordenamientos para generar los tops parciales de cada cliente. Acá también se termina distribuyendo la misma cantidad de frutas pero en más aggregators. 

Un detalle a considerar es que si tenés pocas frutas y muchos aggregators, no te sirve de mucho para acelerar el sistema. Ejemplo: tengo 4 tipos de frutas y 2 aggregators, digamos que por hash cada aggregator procesa dos frutas. Uno podría pensar que agregar muchos aggregators ayudaría, pero si paso a tener 10 aggregators, como máximo 4 van a estar trabajando (si al hashear esas frutas, las 4 terminan en diferentes aggregators). Los otros 6 no van a hacer nada, así que agregarlos no agiliza el procesamiento. 
Todo lo contrario pasa con los Sums: como los mensajes se distribuyen entre los workers disponibles, agregar muchos Sums significa que el mismo trabajo sí se reparte en varias instancias, por lo que a cada una le toca un volumen de trabajo menor.
