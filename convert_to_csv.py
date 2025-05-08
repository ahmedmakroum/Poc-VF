import pandas as pd
import re
import os

def clean_column_name(column_name):
    # Remove special characters and convert to uppercase
    column_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name)  # Remove special characters
    column_name = re.sub(r'\s+', '_', column_name)  # Replace spaces with underscores
    column_name = replace_french_special_characters(column_name)  # Replace French special characters
    return column_name.upper()  # Convert to uppercase for consistency

def replace_french_special_characters(value):
    # Replace French special characters while preserving case
    replacements = {
        'é': 'E', 'É': 'E',
        'à': 'A', 'À': 'A',
        'è': 'E', 'È': 'E',
        'ç': 'C', 'Ç': 'C',
        'ô': 'O', 'Ô': 'O'
    }
    for char, replacement in replacements.items():
        value = value.replace(char, replacement)
    return value

# Updated process_excel_to_csv to ensure all column names and data are consistently cleaned
def process_excel_to_csv(file_path, output_dir):
    # Load the Excel file
    excel_data = pd.ExcelFile(file_path)

    # Process each sheet in the Excel file
    for sheet_name in excel_data.sheet_names:
        df = excel_data.parse(sheet_name)

        # Clean and replace special characters in column names
        df.columns = [clean_column_name(col) for col in df.columns]

        # Replace French special characters in data
        df = df.applymap(lambda x: replace_french_special_characters(str(x)) if isinstance(x, str) else x)

        # Save to CSV
        output_file = os.path.join(output_dir, f"{sheet_name}.csv")
        df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_file = "c:/PoCv4/ext.xlsx"
    output_directory = "c:/PoCv4/csv_output"

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    process_excel_to_csv(input_file, output_directory)
    print(f"CSV files have been saved to {output_directory}")