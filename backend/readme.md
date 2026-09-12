# API [FastAPI]

The API works to get information and build graphs from filters on the mongoDB data. Only GET calls.

## Filters

**Structure**

To more detail description and valies see: 

 (Lineamientos RIPS)[https://www.minsalud.gov.co/sites/rid/Lists/BibliotecaDigital/RIDE/DE/OT/Lineamientos-tecnicos-para-IPS.pdf]

  - municipio : Divipola code (5 digits)
  - sexo : [F,M]
  - regimen : [1,2,3,4,5,6,7,8]
  - edad_min : 
  - edad_max :
  - anio : [2014 to 2024]
  - etnia :
  - discapacidad :
    
## Functionality

### Analytics (Aggregates)

The analytics services provides descriptive analytics about the given population

### Graphs (Aggregates)

The graph service provides access to morbidity networks

#### Graph Filters

**Type networks**
 - Consults : The individual graph build preserves the weight of coo-ocurrence of diagnostics based of consults.
 - Patients : The indiviudal graph build is an adjacency grpah.

**Edge-weighted models**

 - Prevalence :
 - SCI :
 - Custom :

**Format**


### Patients (Patient)





