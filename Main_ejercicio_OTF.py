# %%
from data_processing import contact_collection, country_recognition, found_emails, fix_phone_numbers, saving_contacts, found_name, duplicate_management
import pandas as pd
import matplotlib.pyplot as plt

# Token de acceso
access_token = "pat-na1-3c7b0af9-bb66-40e7-a256-ce4c5eb27e81"
my_access_token = "pat-na1-cc16d6cd-9273-49a1-b953-d21bfb8d02f8"

# Obtener los contactos
contacts = contact_collection(access_token)
# Lista donde guardare la informacion de todos los contactos extraidos de HubSpot y guardar solo los datos que usaremos en la ETL
extract_contacts = []
# Lista donde guardare la informacion de todos los contactos despues de hacer las transformaciones de todos los datos
transform_contacts = []
# Lista donde guardare la informacion de todos los contactos que cargare a mi API privada de HubSpot, despues de aplicar la funcion duplicate_management() y eliminar registros duplicados 
load_contacts = []
# Creacion de archivo .csv donde se guardara los datos de los contactos recien extraidos del API privada de HubSpot
df = pd.DataFrame(contacts)
df.to_csv('Extraction1.csv', index=False)
# Titulo de la 1 tabla que se muestra, datos extraidos de HubSpot
print()
print("HubSpot Contacs")
print()
# Recorro cada uno de los contactos y aplico las funciones de la transformacion de datos
for contact in contacts:
    # Imprimo contacto directamente de como sale de HubSpot
    print(contact)
    # Guardo en un directorio solo atributos (propiedades) que necesito transformar de cada contacto
    contacto = {
        'id' : contact["id"],
        'email': contact["properties"]["raw_email"],
        'telefono': contact["properties"]["phone"],
        'pais': contact["properties"]["country"],
        'technical_test___create_date' : contact["properties"]["technical_test___create_date"],
        'industry' : contact["properties"]["industry"],
        'hs_object_id' : contact["properties"]["hs_object_id"]
    }
    # Guardo todos los contactos extraidos con solo los atributos (propiedades) que necesito transformar en una lista de diccionarios
    extract_contacts.append(contacto)
    country = contact["properties"]["country"]
    country_name = (country_recognition(country))
    # Guardo en un directorio los atributos (propiedades) transformados de cada contacto
    contacto = {
        'email': found_emails(contact["properties"]["raw_email"]),
        'nombre' : found_name(contact["properties"]["raw_email"]),
        'telefono': fix_phone_numbers(country_name,contact["properties"]["phone"]),
        'pais': country_name[0],
        'ciudad' : country_name[1],
        'technical_test___create_date' : contact["properties"]["technical_test___create_date"],
        'industry' : contact["properties"]["industry"],
        'hs_object_id' : contact["properties"]["hs_object_id"]
    }
    # Guardo todos los contactos con sus atributos (propiedades) transformados en una lista de diccionarios
    transform_contacts.append(contacto)

df = pd.DataFrame(extract_contacts)
print()
# DataFrame 2 que contiene, contactos extraidos de HubSpot con atributos (propiedades) necesarios antes de hacer la transformacion de datos 
print("DataFrame Extract Contacs")
print()
# Imprimir DataFrame 2
print(df.to_string(index=False))
# Creacion de archivo .csv donde se guardara los datos de los contactos extraidos del API privada de HubSpot con solo atributos (propiedades) necesarios
df.to_csv('Extraction2.csv', index=False)
df = pd.DataFrame(transform_contacts)
print()
# DataFrame 3 que contienea, contactos con sus atributos (propiedades) transformados antes de filtrar datos repetidos
print("DataFrame Transform Contacs")
print()
# Imprimir DataFrame 3
print(df.to_string(index=False))
# Creacion de archivo .csv donde se guardara los datos de los contactos con los atributos (propiedades) transformados
df.to_csv('Transformation.csv', index=False)
load_contacts  = duplicate_management(transform_contacts)
df = pd.DataFrame(load_contacts)
print()
# DataFrame 4 que contiene, contactos con sus atributos (propiedades) transformados y despues de aplicar funcion duplicate_management para filtrar datos repetidos, 
# estos son los contactos que se van a cargar a mi API privada de HubSpot 
print("DataFrame Load Contacs")
print()
# Imprimir DataFrame 4
print(df.to_string(index=False))
print()
print()
print()
# Lista dea confirmacion de los contactos cargados a mi API privada de HubSpot con los email de cada contactos
print("DataFrame Transform Contacs")
print()
saving_contacts(load_contacts,my_access_token)

# %% interactive graphic(s)

# Obtener el número de registros por país
df = pd.DataFrame(load_contacts)
country_counts = df['pais'].value_counts()

# Crear la gráfica de barras
plt.figure(figsize=(10, 6))
country_counts.plot(kind='bar')
plt.title('Número de Registros por País')
plt.xlabel('País')
plt.ylabel('Número de Registros')
plt.xticks(rotation=45)
plt.show()

# Obtener el número de registros por Ciudad
city_counts = df['ciudad'].value_counts()

# Crear la gráfica de barras
plt.figure(figsize=(10, 6))
city_counts.plot(kind='bar')
plt.title('Número de Registros por Ciudad')
plt.xlabel('Ciudad')
plt.ylabel('Número de Registros')
plt.xticks(rotation=45)
plt.show()

# Obtener el número de registros por industry
industry_counts = df['industry'].value_counts()

# Crear la gráfica de barras
plt.figure(figsize=(10, 6))
industry_counts.plot(kind='bar')
plt.title('Número de Registros por industria')
plt.xlabel('Industry')
plt.ylabel('Número de Registros')
plt.xticks(rotation=45)
plt.show()
# %%
