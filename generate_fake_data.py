import csv
import random
from faker import Faker
import os

def generate_fake_data_for_ext(output_dir):
    fake = Faker()

    # Define schemas for each table
    schemas = {
        "Extraction_Reglement_Sinistre_Auto": [
            "CODEINTE", "INTERMADIAIRE", "VILLE_INTERMEDIAIRE", "SINISTRE", "COMMENTAIRE", "POLICE", "USAGE", "CODNATSI", "NATURE_SIN", "DATE_SURVENANCE", "DATEDECL", "DATRECCO", "DATE_OUVERTURE", "OUVERT_PAR", "DATE_CLOTURE", "ASSURESP", "TAUX_RESP_ASSURE", "FLAG__HP", "CANAL", "LIEUSINI", "VILLE_SINISTRE", "PAYS", "IMMATRICULATION_ASSURE", "IDECONBA", "LIB_BAREME", "FAIT_GENERATEUR", "CAUSE_CIRCONSTANCE", "TYPE_RECOURS", "COMPAGNIE_ADVERSE", "NBR_VICTIME", "GARANTIES", "ETAPE", "CODEGARA", "GARANTIE", "RUBRIQUE", "MNT_REGLEMENT", "MODE_PAIEMENT", "TYPE_REGLEMENT", "NATURE_REGLEMENT", "DATESAIS", "UTILSAIS", "DATEVALI", "UTILVALI", "DATESIGN", "UTILSIGN", "STATUT_REG", "STATUT_DOSS", "NOM_BENE"
        ],
        "Extraction_Reouvertures_Sinistre_Auto_Par_Garantie": [
            "NUMESINI", "CODNATSI", "NUMPOLICE", "DATESURV", "ASSURESP", "LIEUSINI", "CODEINTE", "INTERMEDIAIRE", "ASSURE", "IMMATRICULATION_ASSURE", "DATE_OUVERTURE", "DATE_CLOTURE", "DATE_REOUVERTURE", "USER_REOUVERTURE", "CODEGARA", "MNT_RESERVE", "MNT_REGLEMENT", "MOTIF"
        ],
        "Extraction_Ouvertures_Sinistre_Auto": [
            "CODEINTE", "INTERMADIAIRE", "VILLE_INTERMEDIAIRE", "SINISTRE", "SIN_GS", "ASSURE", "VILLE_ASSURE", "IMMATRICULATION_ASSURE", "MARQUE_VEHICULE", "TYPE_VEHICULE", "POLICE", "DATE_EFFET_POLICE", "DATE_FIN_POLICE", "USAGE", "PRODUIT", "CODNATSI", "NATURE_SIN", "DATE_SURVENANCE", "DATEDECL", "DATRECCO", "DATE_CREATION_OUVERTURE", "DATE_VALIDATION_OUVERTURE", "USER_CREATION_OUVERTURE", "USER_VALIDATION_OUVERTURE", "DATE_CLOTURE", "ASSURESP", "TAUX_RESP_ASSURE", "FLAG__HP", "CANAL", "LIEUSINI", "VILLE_SINISTRE", "PAYS", "IMMATRICULATION_ASSURE_1", "IDECONBA", "LIB_BAREME", "FAIT_GENERATEUR", "CAUSE_CIRCONSTANCE", "TYPE_RECOURS", "COMPAGNIE_ADVERSE", "NBR_VICTIME", "GARANTIES", "CODEGARA", "MNT_REGLEMENT_P", "MNT_REGLEMENT_H", "ETAPE", "FLAGCICA", "TELE", "ATTESTATION", "CIN", "TYPE_SINISTRE_DOUTEUX", "DECISION_FINAL_INVESTIGATION", "MONTANT_RESERVE_DEBUT_P", "MONTANT_RESERVE_DEBUT_H", "MONTANT_RESERVE_H", "MONTANT_RESERVE_P", "USERGEST", "LIBE_DERNIER_ACTE", "DATE_DERNIER_ACTE", "MODE_PAIEMENT_SOUHAITE", "FRANCHISE", "DOSSCONT", "MOTIF_CLOTURE"
        ],
        "Extraction_Cloture_Sinistre_Auto": [
            "NUMESINI", "CODNATSI", "NUMPOLICE", "DATESURV", "ASSURESP", "LIEUSINI", "CODEINTE", "INTERMEDIAIRE", "ASSURE", "IMMATRICULATION_ASSURE", "DATE_OUVERTURE", "DATE_CLOTURE", "USER_CLOTURE", "CODEGARA", "MNT_RESERVE", "MNT_REGLEMENT", "MOTIF"
        ]
    }

    for table_name, columns in schemas.items():
        data = []
        for _ in range(160):  # Generate 160 unique rows
            row = [
                fake.random_int(min=100000, max=999999) if col == "NUMESINI" else  # Generate realistic integers for NUMESINI
                fake.random_int(min=1000, max=9999) if col.startswith("CODE") else  # Generate realistic integers for code columns
                fake.city() if "VILLE" in col else  # Generate city names for columns containing "VILLE"
                fake.company() if "INTERMEDIAIRE" in col else  # Generate company names for intermediaries
                fake.date_this_decade() if "DATE" in col else  # Generate realistic dates for date columns
                fake.license_plate() if "IMMATRICULATION" in col else  # Generate license plate numbers
                fake.name() if "ASSURE" in col else  # Generate names for insured individuals
                fake.random_int(min=1, max=5) if col == "TYPE_VEHICULE" else  # Generate vehicle types as integers (1-5)
                fake.random_number(digits=5) if "MNT" in col else  # Generate realistic monetary values
                fake.word() for col in columns  # Default to random words for other columns
            ]
            data.append(row)
        data += random.choices(data, k=40)  # Add 20% duplicates

        # Save to CSV
        output_file = os.path.join(output_dir, f"{table_name}.csv")
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(data)

if __name__ == "__main__":
    output_directory = "c:/PoCv4/fake_data"

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    generate_fake_data_for_ext(output_directory)
    print(f"Fake data for ext.xlsx tables has been generated and saved to {output_directory}")