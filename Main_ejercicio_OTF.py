# %%
from data_processing import contact_collection, country_recognition, found_emails, fix_phone_numbers, saving_contacts, found_name, duplicate_management
import pandas as pd
import matplotlib.pyplot as plt

# Access token  Token de acceso
access_token = "pat-na1-3c7b0af9-bb66-40e7-a256-ce4c5eb27e81"
my_access_token = "pat-na1-cc16d6cd-9273-49a1-b953-d21bfb8d02f8"
# Create an instance of HubSpotDataExtractor  Crear una instancia de HubSpotExtractor
#extractor = extraction_functions(access_token)

# Extract contacts   Obtener los contactos
contacts = contact_collection(access_token)
extract_contacts = []
load_contacts = []
transform_contacts = []
df = pd.DataFrame(contacts)
df.to_csv('load.csv', index=False)
# Process the extracted contacts  Hacer algo con los contactos obtenidos
for contact in contacts:
    # Process each contact record as needed
    print(contact)
    contacto = {
        'id' : contact["id"],
        'email': contact["properties"]["raw_email"],
        'telefono': contact["properties"]["phone"],
        'pais': contact["properties"]["country"],
        'technical_test___create_date' : contact["properties"]["technical_test___create_date"],
        'industry' : contact["properties"]["industry"],
        'hs_object_id' : contact["properties"]["hs_object_id"]
    }
    extract_contacts.append(contacto)
    country = contact["properties"]["country"]
    country_name = (country_recognition(country))
    #countries_names.append(country_recognition(country)) 
    #email = (contact["properties"]["raw_email"]) 
    #found_emails(email)
    #emails.append(found_emails(email)) 
    #phone_number = (contact["properties"]["phone"]) 
    #fix_phone_numbers(country_name,phone_number)
    #phones_numbers.append(fix_phone_numbers(country_name,phone_number))
    #name = found_name(email)
    #names.append(found_name(email))
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
    transform_contacts.append(contacto)
    #contactos.append(contacto)
    #saving_contacts(transform_contacts,access_token2)
df = pd.DataFrame(extract_contacts)
print()
print("DataFrame Extract Contacs")
print()
print(df)
df = pd.DataFrame(transform_contacts)
print()
print("DataFrame TransformContacs")
print()
print(df)
load_contacts  = duplicate_management(transform_contacts)
df = pd.DataFrame(load_contacts)
print()
print("DataFrame Load Contacs")
print()
print(df)
#print (names)
#print (countries_names)
#print(emails)
#print(phones_numbers)
#print (transform_concacts)
load_contacts  = duplicate_management(transform_contacts)
#print(load_contacts)
saving_contacts(duplicate_management(load_contacts),my_access_token)

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
