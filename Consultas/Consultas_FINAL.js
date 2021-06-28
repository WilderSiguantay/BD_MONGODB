/*a) Realizar un stored procedure que devuelva la tabla de posiciones en cualquier
momento. Como parámetro debe recibir la temporada (id o año) y tener dos
parámetros excluyentes, el número de jornada y la fecha. Si recibe la fecha calcula
la tabla a la fecha indicada aun así no haya terminado la jornada, y si recibe la
jornada debe traer las posiciones hasta esa jornada. Si ambos están vacíos toma
como si fuera el final de temporada. */


//tabla posiciones por jornada


function consulta_jornada(T,J) {
var t=db.detalle_partido.aggregate(
    [
    {$match:{"TEMPORADA": T, "NO_JORNADA": {$lte:J}}},
    {$group:{_id:'$LOCAL', 
        PJ:{$sum:1},
        PG: {"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        PP: {"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
        PE: {"$sum": {$cond: {if: { $eq: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        GF:{$sum:'$GOLES_LOCAL'},
        GC:{$sum:'$GOLES_VISITA'},
        PUNTOS:{$sum:'$PUNTOS_LOCAL'}
        }
        },
        {$sort :{ puntos_local : -1, GF:-1 } }
    ]
    );
    return t
}

//temporada, jornada
consulta_jornada(2019,15);


//tabla posiciones por fecha
//TEMPORADA, FECHA (2019-09-30)
function consulta_fecha(T,F) {

var t =db.detalle_partido.aggregate(
    [
    {$match:{"TEMPORADA": T, "FECHA": {$lte: ISODate(F + " 00:00:00.000Z")}}},
    {$group:{_id:'$LOCAL', 
        PJ:{$sum:1},
        PG: {"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        PP: {"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
        PE: {"$sum": {$cond: {if: { $eq: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        GF:{$sum:'$GOLES_LOCAL'},
        GC:{$sum:'$GOLES_VISITA'},
        PUNTOS:{$sum:'$PUNTOS_LOCAL'}
        }
        },
        {$sort :{ PUNTOS : -1, GF:-1 } }
    ]
    ) ;

return t;

}

consulta_fecha(2019,'2019-09-30');
 

/*b) Vista que muestre los primeros 4 lugares de los últimos 40 años (TOP 10) columnas,
puesto 1, puntos 1, puesto 2, puntos 2, puesto 3, puntos 3, puesto 4, puntos 4. La
vista tendrá entonces un total de 40 filas. */

    db.consulta_b.find({},{_id:0});


/*c) Consulta que muestre los equipos que ha ganado la liga más veces en los últimos 20
años (TOP 5)*/

db.tabla_posiciones.aggregate([
    {$match:{POSICION:{$eq:1},ANIO:{$gte:1999}}},
    {$unwind: "$EQUIPO"},
    {$group:{ _id:'$EQUIPO', LIGAS: {$sum:1}}},
    {$sort:{LIGAS:-1}}
])


/*d) Realizar una stored procedure que muestre que equipos descendieron y no
aparecen en la temporada que se envíe por parámetro.*/
function consulta_d(anio) {
    var num = (db.tabla_posiciones.find({"ANIO": anio-1},{_id:0,"EQUIPO": 1}).count() -3)
    var t = db.tabla_posiciones.find({"ANIO":anio-1},{_id:0,"EQUIPO": 1}).skip(num);
    return t;
 }
consulta_d(1979);

/*e) Realizar una vista que devuelva las victimas favoritas de un equipo, en otras
palabras, a quien han derrotado más veces. */

db.consulta_e.find()

/*f) Realizar un stored procedure que reciba el equipo (id o nombre) y que devuelva las
posiciones que ha ocupado en cada una de las temporadas, goles y puntos.*/
function consulta_f(equipo) {
    var t =db.tabla_posiciones.aggregate([
       {$match:{"EQUIPO": equipo}},
       {$project: {
           _id:0,
           POSICION: 1,
           EQUIPO: 1, 
           TEMPORADA: "$ANIO",
           GF: 1,
           GC: 1,
           PTS: 1
       }},
    
    ]);
    return t;
 }

consulta_f("Barcelona")


/*g) Responder ¿Cuál ha sido la victoria más abultada de los últimos 40 años? Partido,
equipos y marcador*/
function consulta_g() {
    var t = db.partidos.aggregate([
       {$project: {
          _id:0,
          LOCAL:  '$LOCAL.NOMBRE_EQUIPO',
          VISITANTE: '$VISITANTE_1.NOMBRE_EQUIPO',
          GOLES_LOCAL: 1,
          GOLES_VISITA: 1,
          FECHA: 1
        }},
       {$sort:{GOLES_LOCAL:-1, GOLES_VISITA:1}},
       {$limit: 1},
       ]).toArray();
       
       t.push(db.partidos.aggregate([
       {$project: {
          _id:0,
          LOCAL:  '$LOCAL.NOMBRE_EQUIPO',
          VISITANTE: '$VISITANTE_1.NOMBRE_EQUIPO',
          GOLES_LOCAL: 1,
          GOLES_VISITA: 1,
          FECHA: 1
        }},
       {$sort:{GOLES_VISITA:-1, GOLES_LOCAL: 1}},
       {$limit: 1},
       ]).toArray());
       return t;
    }
    
    consulta_g();

/*h) Realizar un stored procedure que la temporada (id o año) y que devuelva el historial
de los equipos que han ocupado el primer puesto de la liga de inicio a fin de
temporada, con fechas y puntos. */



/*i) Realizar un stored procedure que la temporada (id o año) y que devuelva el historial
de los equipos que han ocupado el último puesto de la liga de inicio a fin de
temporada, con fechas y puntos. */



/*j) Vista que muestre, cuántos goles se anotaron en cada temporada, que equipo anoto
más, que equipo anoto menos.*/

db.goles.find()




/*k) Consulta que muestre, al equipo con más victorias, más derrotas y más empates. */

function consulta_k(PARAMETRO){
var t;
if(PARAMETRO=="VICTORIAS"){
    t = db.partidos_completos.aggregate([
        {$group:{
            _id: "$LOCAL",
            VICTORIAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
            DERROTAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
            EMPATES:{"$sum": {$cond: {if: { $eq: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}}
        }},
        {$sort:{"VICTORIAS":-1}},
        {$limit: 1}
    ]);
}else if(PARAMETRO== "EMPATES"){
    t = db.partidos_completos.aggregate([
        {$group:{
            _id: "$LOCAL",
            VICTORIAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
            DERROTAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
            EMPATES:{"$sum": {$cond: {if: { $eq: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}}
        }},
        {$sort:{"EMPATES":-1}},
        {$limit: 1}
    ]);
}else{
    t = db.partidos_completos.aggregate([
        {$group:{
            _id: "$LOCAL",
            VICTORIAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
            DERROTAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
            EMPATES:{"$sum": {$cond: {if: { $eq: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}}
        }},
        {$sort:{"DERROTAS":-1}},
        {$limit: 1}
    ]);
}
 
return t;
}

consulta_k('VICTORIAS');
consulta_k('DERROTAS');
consulta_k('EMPATES');

/*l) Un grupo de stored procedures que efectué una simulación con la pueda calcular
nuevamente los datos de todas las consultas anteriores. Debe ser capaz de modificar
resultados, ingresando los parámetros de año, jornada, resultado de visita y local. Y
un stored procedure que al ejecutarlo retorne todo a su estado original como si no
hubiera hecho ningún cambio por medio de la simulación. */

