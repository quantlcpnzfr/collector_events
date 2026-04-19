import json
import pycountry

def build_unified_country_db(names_file: str, codes_file: str, output_file: str):
    # 1. Carregar os JSONs originais
    with open(names_file, 'r', encoding='utf-8') as f:
        name_to_iso = json.load(f)
        
    with open(codes_file, 'r', encoding='utf-8') as f:
        iso_db = json.load(f)

    unified_db = {}

    # 2. Iterar sobre a base principal de códigos
    for iso_code, data in iso_db.items():
        name = data.get("name", "")
        keywords = data.get("keywords", [])
        
        # Encontrar todos os aliases (nomes alternativos) que apontam para este ISO
        aliases = [k for k, v in name_to_iso.items() if v == iso_code]
        
        # Juntar aliases e keywords, removendo duplicatas de forma performática usando set
        all_keywords = list(set(keywords + aliases))
        
        # 3. Descobrir a moeda usando pycountry
        currency_code = "UNKNOWN"
        try:
            country_obj = pycountry.countries.get(alpha_2=iso_code)
            if country_obj:
                # Busca a moeda oficial do país
                currency = pycountry.currencies.get(numeric=country_obj.numeric)
                if currency:
                    currency_code = currency.alpha_3
        except Exception:
            pass # Mantém UNKNOWN caso não ache (ex: territórios disputados)

        # Hardcodes financeiros importantes (ajuste fino)
        if iso_code == "EU": currency_code = "EUR"
        elif iso_code == "GB": currency_code = "GBP"
        elif iso_code == "US": currency_code = "USD"
        elif iso_code == "CH": currency_code = "CHF"

        unified_db[iso_code] = {
            "name": name,
            "currency": currency_code,
            "keywords": all_keywords
        }

    # 4. Salvar o JSON final otimizado
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unified_db, f, indent=2, ensure_ascii=False)
    
    print(f"Dicionário unificado gerado em {output_file} com {len(unified_db)} registros.")

if __name__ == "__main__":
    import os
    _dir = os.path.dirname(os.path.abspath(__file__))
    build_unified_country_db(
        os.path.join(_dir, "country-names.json"),
        os.path.join(_dir, "country-codes.json"),
        os.path.join(_dir, "unified-countries.json")
    )
    