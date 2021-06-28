/*CONSULTA PARA PASAR DATOS A MONGODB*/
/*SE INGRESA EN STUDIO 3T FOR MONGO DB*/


/*CONSULTAS MONGO DB*/
//VER PARTIDOS POR FECHA
db.partidos.find({
},{
   LOCAL: "$LOCAL.NOMBRE_EQUIPO" ,
   GOLES_LOCAL: "$GOLES_LOCAL",
   GOLES_VISITA: "$GOLES_VISITA",
   PUNTOS_LOCAL: "$PTS_LOCAL",
   PUNTOS_VISITA: "$PTS_VISITANTE",
   FECHA: "$FECHA",
   "JORNADA.NO_JORNADA": 1,
   "TEMPORADA.AÑO_INICIO": 1
},
{$sort :{ FECHA : -1} }
).toArray();

db.partidos.find({
},{
   LOCAL: "$VISITANTE_1.NOMBRE_EQUIPO" ,
   GOLES_LOCAL: "$GOLES_VISITA",
   GOLES_VISITA: "$GOLES_LOCAL",
   PUNTOS_LOCAL: "$PTS_VISITANTE",
   PUNTOS_VISITA: "$PTS_LOCAL",
   FECHA: "$FECHA",
   "JORNADA.NO_JORNADA": 1,
   "TEMPORADA.AÑO_INICIO": 1
},
{$sort :{ FECHA : -1} }
);


//con la ayuda de mongoimport importamos toda la data que esta en el archivo PELICULAS.json

//mongoimport --db laliga --collection detalle_partido --type json --file PARTIDOSLOCAL.json --jsonArray --legacy 


//EXPRESION REGULAR PARA REEMPLAZAR '/**/':   [/][/\/\*]\d+[*\*\//][/]  --para reemplazar el _id "_id":ObjectId[(]["].+["][)][,]



db.getCollection('partidos').find({})


//tabla posiciones por jornada

db.detalle_partido.aggregate(
   [
   {$match:{"TEMPORADA": 2019, "NO_JORNADA": {$lte:38}}},
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
   )



//tabla posiciones por fecha
db.detalle_partido.aggregate(
   [
      {$match:{"TEMPORADA": 2019, "FECHA": {$lte: ISODate("2019-09-30 00:00:00.000Z")}}},
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
   )



   //VISTA TABLA POSICIONES GENERAL
   db.createView(
      "tabla_posiciones",
      "detalle_partido",
      [
      
  {$group:{
      _id:{equipo:'$LOCAL', año:'$TEMPORADA'},
      POSICION:{$sum:1}, 
      PJ:{$sum:1},
      PG: {"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
      PP: {"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
      PE: {"$sum": {$cond: {if: { $eq: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
      GF:{$sum:'$GOLES_LOCAL'},
      GC:{$sum:'$GOLES_VISITA'},
      DG:{$subtract: [ '$GOLES_LOCAL', '$GOLES_VISITA' ]},
      PUNTOS:{$sum:'$PUNTOS_LOCAL'}
      }
      },   
      {$sort :{ "_id.año": 1 ,PUNTOS : -1, GF:-1 } },
      {$project:{
      POSICION: '$POSICION',
      EQUIPO: '$_id.equipo',
      PARTIDOS: '$PJ',
      GANADOS: '$PG',
      EMPATES: '$PE',
      PERDIDOS: '$PP',
      GF: '$GF',
      GC: '$GC',
      DG: '$DG',
      PTS: '$PUNTOS',
      AÑO: '$_id.año'
      }},
 
      ]
      
      
      )
      

      
db.createView(
   "consulta_b",
   "tabla_posiciones",
   [ 
{$match:{"POSICION": {$lte:4}}},   
   {$project:{
   POS: '$POSICION',
   EQUIPO: '$EQUIPO',
   PJ: '$PARTIDOS',
   PG: '$GANADOS',
   PE: '$EMPATES',
   PP: '$PERDIDOS',
   GF: '$GF',
   GC: '$GC',
   PTS: '$PTS',
   AÑO: '$ANIO'
   }},
   ]
   )
   
   
   

   db.tabla_posiciones.aggregate([
       {$project:{
           POS: '$POSICION',
           EQUIPO: '$EQUIPO',
           PJ: '$PARTIDOS',
           PG: '$GANADOS',
           PE: '$EMPATES',
           PP: '$PERDIDOS',
           GF: '$GF',
           GC: '$GC',
           PTS: '$PTS',
           AÑO: '$ANIO'
           }},
   ])


   db.consulta_b.find({},{_id:0});


   /*CONSULTA D*/ 

   function consulta_d(anio) {
      var t = db.tabla_posiciones.find({"ANIO": anio-1},{_id:0,"EQUIPO": 1}).skip(db.tabla_posiciones.find({"ANIO": anio-1},{_id:0,"EQUIPO": 1}).count() - 3);
      return t;
   }
     
     consulta_d(1979);



/*CONSULTA E*/
function partidos_completos() {
   var m = {};
   var t = db.partidos.find({
   },{
      _id:0,
      LOCAL: "$LOCAL.NOMBRE_EQUIPO" ,
      VISITANTE: "$VISITANTE_1.NOMBRE_EQUIPO",
      GOLES_LOCAL: "$GOLES_LOCAL",
      GOLES_VISITA: "$GOLES_VISITA",
      PUNTOS_LOCAL: "$PTS_LOCAL",
      PUNTOS_VISITA: "$PTS_VISITANTE",
      FECHA: "$FECHA",
      JORNADA: "$JORNADA.NO_JORNADA",
      TEMPORADA: "$TEMPORADA.AÑO_INICIO"
   },
   {$sort :{ FECHA : -1} }
   ).toArray();
   var r = db.partidos.find({
   
       },{
      _id:0,
      LOCAL: "$VISITANTE_1.NOMBRE_EQUIPO" ,
      VISITANTE: "$LOCAL.NOMBRE_EQUIPO",
      GOLES_LOCAL: "$GOLES_VISITA",
      GOLES_VISITA: "$GOLES_LOCAL",
      PUNTOS_LOCAL: "$PTS_VISITANTE",
      PUNTOS_VISITA: "$PTS_LOCAL",
      FECHA: "$FECHA",
      JORNADA: "$JORNADA.NO_JORNADA",
      TEMPORADA: "$TEMPORADA.AÑO_INICIO"
   },
   {$sort :{ FECHA : -1} }
   ).toArray();
   t.push(r);
   return t;
}

partidos_completos();




db.partidos.find({
},{
   LOCAL: "$LOCAL.NOMBRE_EQUIPO" ,
   VISITANTE: "$VISITANTE_1.NOMBRE_EQUIPO",
   GOLES_LOCAL: "$GOLES_LOCAL",
   GOLES_VISITA: "$GOLES_VISITA",
   PUNTOS_LOCAL: "$PTS_LOCAL",
   PUNTOS_VISITA: "$PTS_VISITANTE",
   FECHA: "$FECHA",
   "JORNADA.NO_JORNADA": 1,
   "TEMPORADA.AÑO_INICIO": 1
},
{$sort :{ FECHA : -1} }
);

db.partidos.find({
},{
   LOCAL: "$VISITANTE_1.NOMBRE_EQUIPO" ,
   VISITANTE: "$LOCAL.NOMBRE_EQUIPO",
   GOLES_LOCAL: "$GOLES_VISITA",
   GOLES_VISITA: "$GOLES_LOCAL",
   PUNTOS_LOCAL: "$PTS_VISITANTE",
   PUNTOS_VISITA: "$PTS_LOCAL",
   FECHA: "$FECHA",
   "JORNADA.NO_JORNADA": 1,
   "TEMPORADA.AÑO_INICIO": 1
},
{$sort :{ FECHA : -1} }
);

//insertar en coleccion partidos_completos

//mongoimport --db laliga --collection partidos_completos --type json --file partidos_completos.json --jsonArray --legacy 

//CONSULTA

db.partidos_completos.aggregate([
   {$unwind: "$LOCAL"},
         {$group:{_id:{EQUIPO: '$LOCAL', RIVAL:'$VISITANTE'}, 
         VICTORIAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}}}},
         {$project:{_id: 0, EQUIPO: '$_id.EQUIPO', VICTORIAS: 1, RIVAL: '$_id.RIVAL'}},
         {$sort:{VICTORIAS: -1,EQUIPO:1, RIVAL: 1}},
   ])
      

   db.createView(
      "consulta_e",
      "partidos_completos",
      [
      
         {$unwind: "$LOCAL"},
         {$group:{_id:{EQUIPO: '$LOCAL', RIVAL:'$VISITANTE'}, 
         VICTORIAS:{"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}}}},
         {$project:{_id: 0, EQUIPO: '$_id.EQUIPO', VICTORIAS: 1, RIVAL: '$_id.RIVAL'}},
         {$sort:{EQUIPO:1, RIVAL: 1, VICTORIAS: -1}},
      ]
      )
      
      
      db.createView(
         "consulta_b",
         "tabla_posiciones",
         [ 
      {$match:{"POSICION": {$lte:4}}},   
         {$project:{
         POS: '$POSICION',
         EQUIPO: '$EQUIPO',
         PJ: '$PARTIDOS',
         PG: '$GANADOS',
         PE: '$EMPATES',
         PP: '$PERDIDOS',
         GF: '$GF',
         GC: '$GC',
         PTS: '$PTS',
         AÑO: '$ANIO'
         }},
         ]
         )

//CONSULTA F


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


//CONSULTA G

//CONSULTA G

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


/*---------------------
//consulta j//
----------------------*/

db.createCollection("goles");

db.goles.find();

//INSERTAR EN GOLES
db.goles.insert(
 
db.partidos_completos.aggregate([
  
{$group:{
    _id:{TEMPORADA: '$TEMPORADA', EQUIPO: '$LOCAL'},
    golesxequipo: {$sum: '$GOLES_LOCAL'}
}},

{
    $project:{
    _id:0,
    ANIO:'$_id.TEMPORADA',
    EQUIPO: '$_id.EQUIPO',
    golesxequipo: 1 
    }
    },
    {$sort: {ANIO:1, golesxequipo: -1}},
]).toArray()
   
);
//funcion para modificar los campos y agregar uno nuevo con el total de goles
function agregar_campo() {

    var miCursor = db.partidos.aggregate([
        {$group:{
            _id:"$TEMPORADA",
            goles_local: {$sum: '$GOLES_LOCAL'},
            goles_visita: {$sum: '$GOLES_VISITA'},
            
        }},
        {
         $addFields:{
           total_goles: { $sum: ["$goles_local", "$goles_visita"] }
         }},
         {
         $project:{
         _id:0,
         ANIO:'$_id.AÑO_INICIO',
         total_goles: 1    
         }
         },
         {$sort: {ANIO:1}}
    ]);
        
    while (miCursor.hasNext()){
            datos = miCursor.next();
            db.goles.updateMany(
                {ANIO: datos.ANIO},
                {$set:{TOTAL_GOLES: datos.total_goles}}
            )
            }
    }  
//EJECUTAMOS FUNCION ANTERIOR
agregar_campo();
//CONSULTAMOS LA TABLA
db.goles.find()