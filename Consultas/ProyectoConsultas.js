use laliga

db.partidos.aggregate(
[
{$match:{}},
{$group:{
    _id:'$LOCAL.NOMBRE_EQUIPO', 
    count: {$sum:1},
    Goles_local: '$GOLES_LOCAL',
        Goles_visita: '$GOLES_VISITA',
        PuntosLocal: '$PTS_LOCAL',
        PuntosVisita: '$PTS_VISITANTE',
        Fecha: '$FECHA',
        Jornada: '$JORNADA',
        Temporada: '$TEMPORADA',
    }
    },
        {$sort :{ Fecha: -1 } }
    ])
    

db.partidos.find()


db.partidos.find({
},{
   LOCAL: "$LOCAL.NOMBRE_EQUIPO" ,
   GOLES_LOCAL: "$GOLES_LOCAL",
   GOLES_VISITA: "$GOLES_VISITA",
   PUNTOS_LOCAL: "$PTS_LOCAL",
   PUNTOS_VISITA: "$PTS_VISITANTE",
   FECHA: "$FECHA",
   NO_JORNADA: "$JORNADA.NO_JORNADA",
   TEMPORADA: "$TEMPORADA.AÑO_INICIO"
},
{$sort :{ FECHA : -1} }
);

db.partidos.find({
},{
   LOCAL: "$VISITANTE_1.NOMBRE_EQUIPO" ,
   GOLES_LOCAL: "$GOLES_VISITA",
   GOLES_VISITA: "$GOLES_LOCAL",
   PUNTOS_LOCAL: "$PTS_VISITANTE",
   PUNTOS_VISITA: "$PTS_LOCAL",
   FECHA: "$FECHA",
   NO_JORNADA: "$JORNADA.NO_JORNADA",
   TEMPORADA: "$TEMPORADA.AÑO_INICIO"
},
{$sort :{ FECHA : -1} }
);


db.detalle_partido.find().count()

db.detalle_partido.find()

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
       {$sort :{ PUNTOS : -1, GF:-1 } }
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
        {$sort :{ PUNTOS : -1, GF:-1 } }
    ]
    ) 
        
        
        
//tabla posiciones por fecha
db.detalle_partido.aggregate(
    [
    {$group:{
        _id:{equipo:'$LOCAL', año:'$TEMPORADA'},
        PJ:{$sum:1},
        PG: {"$sum": {$cond: {if: { $gt: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        PP: {"$sum": {$cond: {if: { $gt: ['$GOLES_VISITA', '$GOLES_LOCAL'] }, then: 1, else: 0}}},
        PE: {"$sum": {$cond: {if: { $eq: ['$GOLES_LOCAL', '$GOLES_VISITA'] }, then: 1, else: 0}}},
        GF:{$sum:'$GOLES_LOCAL'},
        GC:{$sum:'$GOLES_VISITA'},
        PUNTOS:{$sum:'$PUNTOS_LOCAL'}
        }
        },   
        
        {$sort :{ "_id.año": 1 ,PUNTOS : -1, GF:-1 } },
        {$project:{
        EQUIPO: '$_id.equipo',
        PJ: '$PJ',
        PG: '$PG',
        PE: '$PE',
        PP: '$PP',
        GF: '$GF',
        GC: '$GC',
        PTS: '$PUNTOS',
        AÑO: '$_id.año'
        }},
   
        ]
    ) 
   
   
        
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
        PUNTOS:{$sum:'$PUNTOS_LOCAL'}
        }
        },   
        
        {$sort :{ "_id.año": 1 ,PUNTOS : -1, GF:-1 } },
        {$project:{
        EQUIPO: '$_id.equipo',
        PJ: '$PJ',
        PG: '$PG',
        PE: '$PE',
        PP: '$PP',
        GF: '$GF',
        GC: '$GC',
        PTS: '$PUNTOS',
        AÑO: '$_id.año'
        }},
   
        ]
        
        
        )
        

db.tabla_posiciones.find()


//CONSULTA B
        
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
    
db.consulta_b.find({},{_id:0});
db.tabla_posiciones.find()


 db.consulta_b.aggregate(
        [
        {$project:
            {
                _id:0,
                POS: 1,
                EQUIPO: 1,
                PTS:1,
                AÑO:1
            
            }
            },
        ]
        ) 

db.tabla_posiciones.find(); 
        
            
    db.tabla_posiciones.aggregate([
        {$project:{
            POSICION : 1,
            EQUIPO : 1,
            PARTIDOS: 1,
            GANADOS: 1,
            EMPATES: 1,
            PERDIDOS: 1,
            GF: 1,
            GC: 1,
            PTS: 1,
            ANIO: 1
            }},
    ])

    




