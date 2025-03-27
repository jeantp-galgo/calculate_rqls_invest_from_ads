def classify_ads(texto):
    # Lista completa de marcas
    marcas = ['Bajaj', 'Italika', 'Honda', 'Vento', 'CF Moto', 'Yamaha', 'KTM',
            'Benelli', 'Suzuki', 'TVS', 'Kawasaki', 'Zmoto', 'Zontes',
            'Dinamo', 'MB', 'Hero', 'Royal Enfield', 'Super Soco', 'BDS',
            'Treck']

    if not isinstance(texto, str):
        return "Generica"
    
    texto = texto.lower()
    
    # Diccionario de marcas con variaciones comunes
    marcas_dict = {
        "bajaj": ["bajaj", "pulsar", "boxer"],
        "italika": ["italika"],
        "honda": ["honda", "cb190r"],
        "vento": ["vento"],
        "cf moto": ["cf moto", "cfmoto", "cf-moto"],
        "yamaha": ["yamaha"],
        "ktm": ["ktm"],
        "benelli": ["benelli"],
        "suzuki": ["suzuki"],
        "tvs": ["tvs"],
        "kawasaki": ["kawasaki", "kawa"],
        "zmoto": ["zmoto", "z-moto"],
        "zontes": ["zontes"],
        "dinamo": ["dinamo"],
        "mb": ["mb"],
        "hero": ["hero"],
        "royal enfield": ["royal enfield", "royalenfield", "royal-enfield", "enfield"],
        "super soco": ["super soco", "supersoco", "super-soco"],
        "bds": ["bds"],
        "treck": ["treck"]
    }
    
    # Buscar coincidencias en el texto
    for marca, variaciones in marcas_dict.items():
        for variacion in variaciones:
            if variacion in texto:
                # Devolver marca con formato original de la lista proporcionada
                for m in marcas:
                    if m.lower() == marca:
                        return m
                return marca.title()  # Formato alternativo si no se encuentra exactamente
    
    return "Generica"