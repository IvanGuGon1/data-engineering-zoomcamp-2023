### Starting project

- Nos damos de alta en clouddbt, y creamos una cuenta de servicio apropiada para esto, generando un JSON con las claves, que después importaremos en el asistente.
- Además hemos asociado un repositorio de github al proyecto, introduciendo una clave ssh en github.
- En clouddbt, abrimos el editor, inicializamo proyecto, y después creamos una nueva rama para poder editar.
- Modificamos nombre proyecto en dbt_project.yml

### Modelos

- Tenemos una carpeta "modelos", en la cual se gestionan los accesos a las bases de datos.
- Vamos añadiendo los parametros, tipo framework.
- Corremos los modelos con "dbt run" para correrlos todos, o especificando el modelo.

### Macros

- Usan estructuras de control IF y LOOP
- Usa variables de entorno para los depliegues.
- Son como funciones que retornan cosas, se usan cuando quieres hacer determinada operaciones en consultas de diferentes tablas, por ejemplo.

### Packages

### Variables

- Son útiles para definir valores que se utilizan a lo largo del proyecto.
- Se pueden pasar como parametro 

al ejecutar dbt run --select stg_green_tripdata --var 'nombre_variable:contenido variable'