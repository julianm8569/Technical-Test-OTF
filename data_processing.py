import requests
import geonamescache
import pandas as pd
import re
#%%  Extraction Functions:

def contact_collection(access_token):

    """
        Description: Contact collection, collects all records marked "true" in the "allowed_to_collect" attribute of the source account.
        Arguments: access_token - string
        Returns: contacts - dictionary list

    """

     # URL de la API para buscar contactos
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    # Parámetros de la solicitud, key de la API de HubSpot
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    # Cuerpo de la solicitud (filtrar por allowed_to_collect: true)
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "allowed_to_collect",
                        "operator": "EQ",
                        "value": "true"
                    }
                ]
            }
        ],
        "properties": [
            "raw_email",
            "country",
            "phone",
            "technical_test___create_date",
            "industry",
            "address",
            "hs_object_id"
        ]
    }
    # Realizar la solicitud POST a la API de HubSpot
    response = requests.post(url, headers=headers, json=payload)
    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # Obtener los datos de la respuesta en formato JSON
        data = response.json()
        # Obtener los contactos de la respuesta
        contacts = data.get("results", [])
        return contacts
    else:
        # En caso de que la solicitud no sea exitosa, mostrar el código de estado y mensaje de error
        print("Error:", response.status_code, response.text)
        return []
#%%  Transformation functions:

def country_recognition(country):

    """
        Description: Country Recognition, for each country record recognizes if this record value corresponds to a country or a city and return a country or/and city name
        Arguments: country - string
        Returns: countri_name,city_name - string,string

    """
    gc = geonamescache.GeonamesCache()
    
    # Obtener los nombres de países y ciudades de la base de datos
    countries = gc.get_countries_by_names()
    cities = gc.get_cities()
    
    # Creo listas que solo contengan nombres de las ciudades, codigo del paises, y nombre de los paises cada una
    cities_names = [cities[key]['name'] for key in cities]
    countrycode = [cities[key]['countrycode'] for key in cities]
    countries_names = [countries[key]['name'] for key in countries]
    countries_code = [countries[key]['iso'] for key in countries]

    # Buscar si el string country de entrada corresponde a una ciudad
    for city_name in cities_names:
        if country.lower() == city_name.lower():
            index = cities_names.index(country)
            code = countrycode[index]
            for countri_code in countries_code:
                if code.lower() == countri_code.lower(): 
                    index = countries_code.index(code)
                    countri_name = countries_names[index]
                    break  
            #print(city_name,code,countri_name)
            return (countri_name,city_name)
    
    # Si no es una ciudad, buscar si es un país
    for countri_name in countries_names:
        if country.lower() == countri_name.lower():
            #print(countri_name, '')
            return (countri_name, '')
        
    # Si no coincide con ningún país ni ciudad, retornar "Unknown"
    return "Unknown"

def found_emails(raw_email):

    """
        Description: Found Emails, for each record, in the property “raw_email” searches and return for any sequence of characters that is enclosed between the characters < and >
        Arguments: raw_email - string
        Returns: new_email - string

    """
    
    # Captura el texto que se encuentra dentro de los caracteres <>
    new_email = re.search(r'<([^>]+)>', raw_email)
    # Si encuentra algun texto entre los caracteres < y >, retornar esa cadena de texto
    if new_email:
        #print(new_email.group(1))
        return(new_email.group(1))
        
    # Si no encuentra ningun texto entre los caracteres < y >, retornar "Unknown"
    else:
        return "Unknown"
    
def  fix_phone_numbers(country,phone):

    """
        Description: Fix Phone Numbers, for each record, add the call code of the corresponding country and apply the format
        Arguments: country,phone - tuple containing (country_found, city_found) string, string
        Returns: new_phone - string

    """

    gc = geonamescache.GeonamesCache()
    # Obtengo solo el pais de la tupla de entrada country que contiene el nombre de pais y ciudad
    country = country[0]
    # Obtener la info de los países de la base de datos
    countries = gc.get_countries_by_names()
    # Creo dos listas que contenga los nombres de los paises y el codigo de llamada de los paises
    countries_names = [countries[key]['name'] for key in countries]
    countries_code = [countries[key]['phone'] for key in countries]
    aux = 0
    # Recorro la lista de paises countries_names uno por uno
    for countri_name in countries_names:
        # Buscar si el string country de entrada corresponde a un pais
        if country.lower() == countri_name.lower():
            # Si encuentro el pais, guardo el indice donde se encuentra dentro de la lista countries_names
            index = countries_names.index(country)
            # Con el indice creo el call_code con la lista countries_code
            call_code = f"(+{countries_code[index]})"
            # Eliminar el '0' si es el primer dígito y eliminar los guiones '-' del numero phone que se recibe como parametro
            clean_number = phone.replace('-', '').lstrip('0')
            # Creo el formato con espacios XXXX XXXXXX 
            format_numeber = '{} {}'.format(clean_number[:4], clean_number[4:])
            # Uno el call_code con format_numeber para obtener el nuevo numero con el formato final (+XXX) XXXX XXXXXX
            new_phone = f"{call_code} {format_numeber}"
            #print (new_phone)
            aux = 1
            return (new_phone)
    # Si no encuentra ningun pais que coincida para obtener el codigo de llamada pongo por defecto (+000) con el numero de entrada phone con el nuevo formato"
    if aux == 0:
        call_code = "(+000)"
        # Eliminar el '0' si es el primer dígito y eliminar los guiones '-' del numero phone que se recibe como parametro
        clean_number = phone.replace('-', '').lstrip('0')
        # Creo el formato con espacios XXXX XXXXXX 
        format_numeber = '{} {}'.format(clean_number[:4], clean_number[4:])
        # Uno el call_code con format_numeber para obtener el nuevo numero con el formato final (+XXX) XXXX XXXXXX
        new_phone = f"{call_code} {format_numeber}"
        #print (new_phone)
        return (new_phone)
    
def found_name(raw_email):

    """
        Description: Found Names, for each record, in the property “raw_email” searches and return the first part of the character string up to before the '<' character containing the first name
        Arguments: raw_email - string
        Returns: name - string

    """
    
    # Obtener la primera parte de raw_email que contiene solo el nombre sin espacios
    name = raw_email.split('<')[0].strip()
    # Si encuentra algun nombre antes del caracter <, retornar esa cadena de texto
    if name:
        return(name)
        
    # Si no encuentra ningun nombre antes del caracter <, retornar "Unknown"
    else:
        return "Unknown"
    
# %% Load Functions:

def saving_contacts(contacts,access_token):

    """
        Description: Saving Contacts, Take contact records after data transformation and upload to HUBSPOT private API 
        Arguments: contacts,access_toke - dictionary list,string
        Returns: any

    """

    # URL de la API para buscar contactos
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    # Parámetros de la solicitud, key de la API de HubSpot
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    for contact in contacts:
        # Cuerpo de la solicitud 
        payload = {

            "id" : contact["hs_object_id"],

            "properties": {
                    "email": contact["email"],
                    "firstname" : contact["nombre"],
                    "phone": contact["telefono"],
                    "country": contact["pais"],
                    "city": contact["ciudad"],
                    "original_create_date": contact["technical_test___create_date"],
                    "original_industry": contact["industry"],
                    "temporary_id": contact["hs_object_id"]


            }
        }
        # Realizar la solicitud POST a la API de HubSpot
        response = requests.post(url, headers=headers, json=payload)
        # Verificar el estado de la respuesta
        if response.status_code == 200 or response.status_code == 201:
            print('Contacto guardado en HubSpot:', contact['email'])
        else:
            print('Error al guardar el contacto:', response.status_code, contact['email'], response.text)
     
# %% Duplicates Management

def duplicate_management(contacts):

    """
        Description: Duplicates Management, algorithm to eliminate contacts that have duplicate emails, or if they do not have email, duplicate names, too
        if an old record contains data that the new one does not, please add it to the most recent record
        Arguments: contacts - list of dictionary
        Returns: merged_datas - list of dictionary

    """
    # Ordenar la lista de diccionarios por fecha de technical_test___create_date de forma descendente
    contacts_sorted = sorted(contacts, key=lambda x: x['technical_test___create_date'], reverse=True)
    # Convierto la lista de diccionarios en un DataFrame para manejar con mayor facilidad
    df = pd.DataFrame(contacts_sorted)
    # Lista auxiliar donde guardare los valores de cada posicion para luego compararlos con el resto de contactos
    merged_datas = []
    aux = 0
    # Ciclo for para recorer cada uno de los contactos
    for row in range(df.shape[0]):
        # Guardo variables que usare para comparar 
        email = df.iloc[row]['email']
        nombre = df.iloc[row]['nombre']
        industria = df.iloc[row]['industry']
        # Guardo en el directorio merged_data los valores de cada contacto que recorro en el ciclo for
        merged_data = {
            'email': df.iloc[row]['email'],
            'nombre' : df.iloc[row]['nombre'],
            'telefono': df.iloc[row]['telefono'],
            'pais': df.iloc[row]['pais'],
            'ciudad' : df.iloc[row]['ciudad'],
            'technical_test___create_date' : df.iloc[row]['technical_test___create_date'],
            'industry' : df.iloc[row]['industry'],
            'hs_object_id' : df.iloc[row]["hs_object_id"]
        }
        #print (email,nombre,industria,df.shape[0],aux,row)
        aux = aux+1
        # Si es el ultimo registro, no tengo mas registros para comparar, por lo cual es unico, lo guardo y rompo el ciclo
        if (df.shape[0]-1) == row :
            merged_datas.append(merged_data)
            break
        # Ciclo for para recorrer los registros de contactos con los que voy a compara los valores del diccionario merged_data
        for i in range(aux,df.shape[0]):
            #print ("llega aqui")
            #print (i)
            # comparo si el email guardado en merged_data es igual a los emails de los otros contactos 
            if  i <= (df.shape[0]-1) and email == df.iloc[i]['email']:
                #print("entra aqui1")
                # Si algun dato del diccionario merged_data esta vacio remplacelo por el dato del contacto mas reciente con el mismo correo 
                if merged_data['nombre'] == '' :
                    merged_data['nombre'] = df.iloc[i]['nombre']
                if merged_data['telefono'] == '' :
                    merged_data['telefono'] = df.iloc[i]['telefono']
                if merged_data['pais'] == '' :
                    merged_data['pais'] = df.iloc[i]['pais']
                if merged_data['ciudad'] == '' :
                    merged_data['ciudad'] = df.iloc[i]['ciudad']
                if merged_data['technical_test___create_date'] == '' :
                    merged_data['technical_test___create_date'] = df.iloc[i]['technical_test___create_date']
                if merged_data['industry'] != df.iloc[i]['industry'] :
                    merged_data['industry'] = merged_data['industry'] + ';' + df.iloc[i]['industry']
                #print (merged_data)
                # Eliminar el registro con correo repetido en el dataframe
                df = df.drop(i)
        # Guardar los contactos que no estan repetidos en el diccionario final
        merged_datas.append(merged_data)
    # Reinicio valores auxiliales para repetir el mismo proceso para los casos donde un contacto tiene correo el correo vacio pero el el mismo nombre que otro contacto
    merged_datas = []
    aux = 0
    # Repito el mismo proceso pero ahora verificando que haya un correo vacio y nombres iguales 
    for row in range(df.shape[0]):
        email = df.iloc[row]['email']
        nombre = df.iloc[row]['nombre']
        industria = df.iloc[row]['industry']
        merged_data = {
            'email': df.iloc[row]['email'],
            'nombre' : df.iloc[row]['nombre'],
            'telefono': df.iloc[row]['telefono'],
            'pais': df.iloc[row]['pais'],
            'ciudad' : df.iloc[row]['ciudad'],
            'technical_test___create_date' : df.iloc[row]['technical_test___create_date'],
            'industry' : df.iloc[row]['industry'],
            'hs_object_id' : df.iloc[row]["hs_object_id"]
        }
        #print (email,nombre,industria,df.shape[0],aux,row)
        aux = aux+1
        if (df.shape[0]-1) == row :
            merged_datas.append(merged_data)
            break
        for i in range(aux,df.shape[0]):
            #print ("llega aqui")
            #print (i)
            if  i <= (df.shape[0]-1) and email == "" and nombre == df.iloc[i]['nombre']:
                #print("entra aqui1")
                if merged_data['email'] == '' :
                    merged_data['email'] = df.iloc[i]['email']
                if merged_data['telefono'] == '' :
                    merged_data['telefono'] = df.iloc[i]['telefono']
                if merged_data['pais'] == '' :
                    merged_data['pais'] = df.iloc[i]['pais']
                if merged_data['ciudad'] == '' :
                    merged_data['ciudad'] = df.iloc[i]['ciudad']
                if merged_data['technical_test___create_date'] == '' :
                    merged_data['technical_test___create_date'] = df.iloc[i]['technical_test___create_date']
                if merged_data['industry'] != df.iloc[i]['industry'] :
                    merged_data['industry'] = merged_data['industry'] + ';' + df.iloc[i]['industry']
                #print (merged_data)
                df = df.drop(i)
        merged_datas.append(merged_data)
    #print (merged_datas)
    #print (df)
    return (merged_datas)

