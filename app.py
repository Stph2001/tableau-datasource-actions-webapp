import pandas as pd
import xml.etree.ElementTree as ET
import re
import html
import streamlit as st
from io import BytesIO

# Diccionario global para almacenar cálculos y sus captions
calculation_pairs = {}

def search_calculation(calculation_id, root):
    """Busca el caption de un cálculo en el archivo TDS."""
    if calculation_id in calculation_pairs:
        return calculation_pairs[calculation_id]
    for column in root.findall(f".//column[@name='[Calculation_{calculation_id}]']"):
        caption = column.get('caption')
        if caption:
            calculation_pairs[calculation_id] = caption
            return caption
    return calculation_id

def calculations_to_captions(text, root):
    """Reemplaza IDs de cálculo en el texto por sus captions correspondientes."""
    calculations = re.findall(r'\[Calculation_\d+\]', text)
    for calculation in calculations:
        calculation_id = calculation.strip('[]')
        caption = search_calculation(calculation_id, root)
        text = text.replace(calculation, caption)
    return text

def transform_formula(formula):
    """Decodifica y transforma la fórmula HTML."""
    return html.unescape(formula)

def clean_name_in_snowflake(name):
    """Limpia el nombre para su uso en Snowflake, eliminando corchetes."""
    return re.sub(r'^\[(.*)\]$', r'\1', name)

def get_table_name_for_column(root, column_name):
    """Busca el nombre de la tabla para una columna dada."""
    for metadata_record in root.findall(".//metadata-record[@class='column']"):
        local_name = metadata_record.find('local-name')
        if local_name is not None and local_name.text == column_name:
            parent_name = metadata_record.find('parent-name')
            return parent_name.text.split(" ")[0] if parent_name.text else ''
    return ''

def process_tds_file(tds_file):
    """Procesa el archivo TDS y devuelve un DataFrame con las columnas y sus características."""
    tree = ET.parse(tds_file)
    root = tree.getroot()

    data = []

    for column in root.findall('.//column'):
        caption = column.get('caption', '')

        if caption == '':
            continue

        name_in_snowflake = clean_name_in_snowflake(column.get('name', ''))
        hidden = True if column.get('hidden') else False
        datatype = column.get('datatype', '')

        # Manejo de la descripción
        desc = column.find('.//desc/formatted-text/run')
        description = desc.text if desc is not None else ''

        calculation_pairs[name_in_snowflake] = caption
        calculation = column.find('calculation')
        transformed_formula = ''
        if calculation is not None:
            if name_in_snowflake.find("(copia)") != -1 or name_in_snowflake.find("(copy)") != -1:
                name_in_snowflake = "Calculado"
            if 'formula' in calculation.attrib:
                transformed_formula = transform_formula(calculation.attrib['formula'])[:500]
            else:
                if calculation.get('class') == 'categorical-bin':
                    bins_descriptions = []
                    for bin_element in calculation.findall('bin'):
                        bin_range = bin_element.get('value').strip('"')
                        bins_descriptions.append(f"Rango {bin_range}")
                    transformed_formula = "; ".join(bins_descriptions)

        # Obtener el nombre de la tabla
        table_name = get_table_name_for_column(root, '[' + name_in_snowflake + ']')[1:-1]

        if re.search(r'__(tableau_internal_object_id__)][.]\[([^]]+)', name_in_snowflake):
            table_name = caption
            name_in_snowflake = 'tableau_internal_object_id'

        column_data = {
            'Nombre': caption,
            'Descripción': description,
            'Fórmula': calculations_to_captions(transformed_formula, root),
            'Oculto': hidden,
            'Nombre en Snowflake': name_in_snowflake,
            'Tabla': table_name,
            'Tipo de Dato': datatype
        }

        data.append(column_data)

    return pd.DataFrame(data)

# Streamlit App
st.title("Descarga de metadata de fuente de datos de Tableau")

st.write("Sube un archivo .tds para generar un Excel con las columnas y sus características.")

uploaded_file = st.file_uploader("Subir archivo TDS", type="tds")

if uploaded_file is not None:
    if st.button("Procesar archivo"):
        try:
            # Procesar el archivo
            df = process_tds_file(uploaded_file)

            # Guardar el DataFrame en un archivo Excel
            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)

            # Descargar el archivo
            st.success("Archivo procesado con éxito.")
            st.download_button(
                label="Descargar Excel",
                data=excel_buffer,
                file_name="Columnas_Universos.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Error al procesar el archivo: {e}")
