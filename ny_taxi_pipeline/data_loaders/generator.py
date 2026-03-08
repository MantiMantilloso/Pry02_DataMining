@data_loader
def load_data(*args, **kwargs):
    # Generamos la lista de diccionarios con las ejecuciones necesarias
    # Mage disparará el siguiente bloque una vez por cada elemento de esta lista
    tasks = []
    for year in range(2022, 2023):
        for month in range(1, 4):
            for service in ['yellow', 'green']:
                tasks.append({
                    "year": year,
                    "month": month,
                    "service_type": service
                })
    
    # Se retorna una lista de diccionarios, y una lista de metadatos (puede ir vacía)
    return [tasks, []]