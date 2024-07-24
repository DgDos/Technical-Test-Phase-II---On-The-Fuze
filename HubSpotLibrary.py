import requests
import re
import pandas as pd
import datetime
import pytz

def getcontacts(api_key, offset):
    """
    Devuelve una lista de contactos de HubSpot API

    Args:
        api_key (str): la llave de la API que se usa para autenticacion.
        offset (int): La informacion de la pagina que sigue.

    Returns:
        dict: Un diccionario con la lista de contactos que tienen la informacion y la paginacion 
    """

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {"Authorization": f"Bearer {api_key}"}
    data = {"after": offset, "limit":100,"filterGroups": [{"filters": [{"propertyName": "allowed_to_collect","operator": "EQ","value": "true"}]}],
              "properties":["firstname","lastname","raw_email","country","phone","technical_test___create_date","industry","address","hs_object_id","allowed_to_collect"]}
    
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()

def contacts(api_key):
    """
    Obtiene la informacion de los contactos de la API que tienen "true" en la propiedad "allowed_to_collect"

    Args:
        api_key (str): la llave de la API que se usa para autenticacion.

    Returns:
        list: Una lista con la informacion de los contactos
    """
    contacts = []
    seguir = True
    offset=0
    while seguir:
        response = getcontacts(api_key,offset)
        for contact in response["results"]:
            if contact["properties"]["allowed_to_collect"] == "true":
                contact_data = {
                    "firstname": contact["properties"]["firstname"],
                    "lastname": contact["properties"]["lastname"],
                    "raw_email": contact["properties"]["raw_email"],
                    "country": contact["properties"]["country"],
                    "phone": contact["properties"]["phone"],
                    "technical_test___create_date": contact["properties"]["technical_test___create_date"],
                    "industry": contact["properties"]["industry"],
                    "address": contact["properties"]["address"],
                    "hs_object_id": contact["properties"]["hs_object_id"]
                }
                contacts.append(contact_data)
        if 'paging' in response.keys():
            offset = response["paging"]["next"]["after"]
        else:
            break          
    return contacts

def cityorcountry(city):
    """
    Dado que los valores de country son 11 podemos crear un diccionario manualmente en esta funcion se reconoce
    si el valor city corresponde a una ciudad o a un pais y devuelve un vector con esa informacion.

    Args:
        city (str): ciudad que se usara para verificar informacion

    Returns:
        tuple: Una tupla que se compone de (pais, ciudad), si city es un pais, regresa vacia la ciudad, de ser nan, regresa vacia la tupla
    """

    dict={'Waterford':'Ireland','Limerick':'Ireland','Dublin':'Ireland','Plymouth':'England','Milton Keynes':'England','Cork':'Ireland','Oxford':'England','London':'England','Winchester':'England','England':'','Ireland':'',}
    if city in dict.keys():
        if dict[city]=='':
            return (city, "")
        else:
            return (dict[city], city)
    else:
        return ('','')

def emailidentifier(info):
    """
    Extrae el email de la string enviada
    Para los emails usaremos un regex, estuve revisando y esta en el formato de [ Nombre < correo > Contact Info. ] o no existe.

    Args:
        info (str): String de la cual extraeremos el email

    Returns:
        string: Un string con el email o vacio en caso de ser nan o no encontrar email
    """
    if pd.isna(info):
        return ''
    patron = r"[A-Za-z]+ <([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>"
    buscar = re.search(patron, info)
    if buscar:
        email = buscar.group(1)
        return email
    else:
        return ''
    
def phonecorrection(phone, country):
    """
    Formatea la string de telefono enviada para dejarla en formato (+XX) XXXX XXXXXX 
    Quitamos los -, luego los 0, damos el espacio despues de 4 numeros y por ultimo ponemos el indicativo del pais

    Args:
        phone (str): Telefono que se usara para transformar
        country (str): Pais que se usara para la verificacion de la extension
    Returns:
        string: Un string que cumple con el formato solicitado
    """
    if pd.isna(phone) or phone=='':
        return ''
    phone= re.sub(r'-', '', phone)
    while True:
        if phone[0] == '0':
            phone=phone[1:]
        else:
            break
    phone = phone[0:4]+ ' '+phone[4:]
    if(country == 'England'):
        phone=('(+44) ')+phone
    else:
        phone=('(+353) ')+phone
    return phone

def duplicatemanagement(df):
    '''
    Funcion encargada de manejar los duplicados y regresar la información según lo estipulado

    Args:
        df (DataFrame): DataFrame con toda la información usada para el proceso
    Returns:
        DataFrame: Que cumple con no tener duplicados ni en email ni en full_name
    '''
    df['full_name'] = ''+df['firstname'].fillna('') +' '+ df['lastname'].fillna('')
    A=df.sort_values(by='technical_test___create_date',ascending=False)
    B = pd.DataFrame(columns=A.columns)  # Crear un nuevo dataframe B con las mismas columnas que A
    for index, row in A.iterrows():  # Recorrer cada fila del dataframe A

        emailex=False #Existe Email
        if (not pd.isna(row['email']) and row['email']!= ''):
            emailex=True
            
        full_nameex=False #Existe Full_Name
        if (not pd.isna(row['full_name']) and row['full_name']!= ''):
            full_nameex=True
        
        if emailex or full_nameex: # Revisar si la fila tiene email o full_name

            existing_row_email=pd.Series()
            if emailex:
                email = row['email']
                existing_row_email = B[B['email'] == email] # Buscar si ya existe una fila con ese email en B
                
            existing_row_full_name=pd.Series()
            if full_nameex:
                full_name = row['full_name']
                existing_row_full_name= B[B['full_name'] == full_name] # Buscar si ya existe una fila con ese full_name en B

            if existing_row_email.empty and existing_row_full_name.empty:  # Si no existe, copiar la fila de A a B
                B = pd.concat([B, row.to_frame().T], ignore_index=True)
            else:  # Si existe, llenar la información que le falta en B con la de A
                if not existing_row_email.empty: #si el email encaja escoger ese index, de lo contrario escoger el index de full_name
                    existing_index = existing_row_email.index[0]
                else: 
                    existing_index = existing_row_full_name.index[0] 
                for col in A.columns: #copiar la informacion que no existe
                    if pd.isna(B.loc[existing_index, col]) and not pd.isna(row[col]):
                        B.loc[existing_index, col] = row[col]
                
                newindustry=row['industry'] 
                actualindustry=B.loc[existing_index, 'industry']
                if actualindustry.find(newindustry) == -1: #verificar que no exista la industria dentro del string ya hecho
                    if B.loc[existing_index,'industry'][0]!=';': #ponerle un punto y coma al inicio en caso de que no tenga
                        B.loc[existing_index,'industry']=';'+B.loc[existing_index,'industry']
                    B.loc[existing_index,'industry']+=';'+newindustry #agregar la nueva industria
        else:
            B = pd.concat([B, row.to_frame().T], ignore_index=True)
    return B

def postcontactos2(df, api_key):
    '''
    Función encargada de subir la información por lotes de 100 a HubSpot, transforma la fecha en tiempo UTC con milisegundos como lo solicita HubSpot

    Args:
        df (DataFrame): DataFrame con toda la información usada para el proceso
    '''
    df=pd.DataFrame(df)
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"
    headers = {"Authorization": f"Bearer {api_key}"}
    localtimezone = pytz.timezone("UTC")
    contactos = []
    cuenta=0
    for index, row in df.iterrows():
        if cuenta==100:
            data = {"inputs": contactos}
            response = requests.post(url, headers=headers, json=data)
            contactos = []
            cuenta=0
        contacto = {"properties": {}}
        if pd.notna(row["email"]):
            contacto["properties"]["email"] = row["email"]
        if pd.notna(row["telefono"]):
            contacto["properties"]["phone"] = row["telefono"]
        if pd.notna(row["pais"]):
            contacto["properties"]["country"] = row["pais"]
        if pd.notna(row["ciudad"]):
            contacto["properties"]["city"] = row["ciudad"]
        if pd.notna(row["firstname"]):
            contacto["properties"]["firstname"] = row["firstname"]
        if pd.notna(row["lastname"]):
            contacto["properties"]["lastname"] = row["lastname"]
        if pd.notna(row["address"]):
            contacto["properties"]["address"] = row["address"]
        if pd.notna(row["industry"]):
            contacto["properties"]["original_industry"] = row["industry"]
        if pd.notna(row["hs_object_id"]):
            contacto["properties"]["temporary_id"] = row["hs_object_id"]
        if pd.notna(row["technical_test___create_date"]):
            ocd = str(row["technical_test___create_date"])
            dateformat = datetime.datetime.strptime(ocd, "%Y-%m-%d")
            localdateformat = localtimezone.localize(dateformat)
            utcdatetime = localdateformat.astimezone(pytz.utc)
            unixtime = int(utcdatetime.timestamp() * 1000)
            contacto["properties"]["original_create_date"] = unixtime
        contactos.append(contacto)
        cuenta+=1
    data = {"inputs": contactos}
    response = requests.post(url, headers=headers, json=data)