# Backend

Architecture

    MongoDB
        │
        ▼
    GET /patients
        │
        ├────────────► GET /general-info
        │
        ├────────────► GET /disease-info
        │
        ├────────────► GET /temporal-info
        │
        └────────────► GET /patient-paths
                                │
                                ▼
                         GET /graph


Directory

    backend/
    │
    ├── main.py
    │
    ├── routers/
    │   ├── analytics.py
    │   ├── graph.py
    │   └── patients.py
    │
    ├── services/
    │   ├── analytics_service.py
    │   ├── graph_service.py
    │   └── patient_service.py
    │
    ├── db/
    │   └── mongodb.py
    │
    └── models/

ENDPOINTS 

    GET /analytics/general-info
    GET /analytics/disease-info
    GET /analytics/temporal-info
    
    GET /graph
    GET /graph/nodes
    
    GET /patients
    GET /patients/{id}
## API 1 : Aceess to the db

Environment : FastAPI, MongoCLient

**Only get data**

### Get call

Filters :

    ´´´´
    municipio: int | None
    sexo: Literal["F", "M"] | None
    regimen: Literal["C", "S"] | None
    anio: int | None
    edad_min: int | None
    edad_max: int | None
    tipo_evento: Literal["C", "U", "P", "H"] | None
    ´´´´

**Measures of population**

 - get_general_info (municipio = municipio,sexo=F,M,None,"",regimen = 'C','S',None,"",anio = int, None,edad = (inf,sup),None,
                     tipo_evento_medico = "C","U","P",H")
   return
     { "Numero de pacientes" : ##,
       "Consultas promedio por paciente" : ##,
       "Top 10 diag prin" : [],
       ""
     }
- get_info_disease (municipio = municipio,sexo=F,M,None,"",regimen = 'C','S',None,"",anio = int, None,edad = (inf,sup),None,
                     tipo_evento_medico = "C","U","P",H",Diag = "X##")
  return 
    { "Numero de pacientes" : ##,
       "Consultas promedio por paciente" : ##,
       "Top 10 diag" : [],
       ""
     }

- get_temporal_info(municipio = municipio,sexo=F,M,None,"",regimen = 'C','S',None,"",anio = int, None,edad = (inf,sup),None,
                     tipo_evento_medico = "C","U","P",H")
  return
  {"tiempo promedio entre consultas" : ##,
   "tiempo promedio consulta/procedimiento" : ##,
   ""
  }
**For graph**

- get_patient_path (municipio = municipio,sexo=F,M,None,"",regimen = 'C','S',None,"",anio = int, None,edad = (inf,sup),None,
                     tipo_evento_medico = "C","U","P",H")

  return
  {[D1,D2],
   [D3],
   [D1,D3,D2],
   ...
  }

## API 2: Graph generation

- get_graph(municipio = municipio,sexo=F,M,None,"",regimen = 'C','S',None,"",anio = int, None,edad = (inf,sup),None,
                     tipo_evento_medico = "C","U","P",H",format = "glm")
  call get_nodes from API 1
