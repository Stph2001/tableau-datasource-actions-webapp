import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import re
import html
from io import BytesIO
import zipfile
import os

# ------------------------- #
# FUNCIONES EXISTENTES      #
# ------------------------- #

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
    """Procesa el contenido de un archivo TDS y devuelve un DataFrame con las columnas y sus características."""
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
            if "(copia)" in name_in_snowflake or "(copy)" in name_in_snowflake:
                name_in_snowflake = "Calculado"
            if 'formula' in calculation.attrib:
                transformed_formula = transform_formula(calculation.attrib['formula'])[:500]
            else:
                # Manejo de bins (ejemplo con 'categorical-bin')
                if calculation.get('class') == 'categorical-bin':
                    bins_descriptions = []
                    for bin_element in calculation.findall('bin'):
                        bin_range = bin_element.get('value').strip('"')
                        bins_descriptions.append(f"Rango {bin_range}")
                    transformed_formula = "; ".join(bins_descriptions)

        # Obtener el nombre de la tabla
        table_name = get_table_name_for_column(root, '[' + name_in_snowflake + ']')[1:-1]

        # Caso especial para 'tableau_internal_object_id'
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

def process_tds_or_tdsx(file_obj):
    """
    Determina si el archivo es .tds o .tdsx.
    Si es .tdsx, descomprime en memoria y procesa el .tds.
    Si es .tds, lo procesa directamente.
    """
    filename = file_obj.name.lower()
    
    if filename.endswith(".tdsx"):
        # Descomprimir y buscar .tds
        with zipfile.ZipFile(file_obj, 'r') as z:
            for name in z.namelist():
                if name.endswith(".tds"):
                    tds_data = z.read(name)
                    return process_tds_file(BytesIO(tds_data))
            raise ValueError("No se encontró ningún archivo .tds dentro del .tdsx.")
    elif filename.endswith(".tds"):
        return process_tds_file(file_obj)
    else:
        raise ValueError("El archivo no es un .tds o .tdsx válido.")


# ------------------------- #
# NUEVAS FUNCIONES          #
# ------------------------- #

def update_descriptions_in_tds(tds_content: bytes, df_descriptions: pd.DataFrame) -> bytes:
    """
    Actualiza las descripciones de las columnas en un archivo TDS (contenido binario).
    Omite actualizaciones para descripciones vacías, '-'
    o celdas con NaN.
    """
    tree = ET.parse(BytesIO(tds_content))
    root = tree.getroot()

    # Convertimos NaN a None o '' (según prefieras) antes de crear el dict
    df_descriptions = df_descriptions.fillna('')

    # Creamos un diccionario: { Nombre : Descripción } a partir del DataFrame
    desc_dict = dict(zip(df_descriptions['Nombre'], df_descriptions['Descripción']))
    
    for column in root.findall(".//column"):
        caption = column.get('caption')
        if not caption:
            continue
        
        # Buscar la descripción en el dict
        if caption in desc_dict:
            new_description = desc_dict[caption]

            # Validar para omitir '-'
            if new_description == "-":
                continue

            # Validar NaN o string vacío (después de fillna se convierte en '', pero revisamos cualquier caso)
            if not isinstance(new_description, str) or new_description.strip() == '':
                continue

            # Eliminar descripción existente si existe
            desc_elem = column.find("desc")
            if desc_elem is not None:
                column.remove(desc_elem)

            # Crear nueva descripción
            new_desc = ET.Element("desc")
            formatted_text = ET.SubElement(new_desc, "formatted-text")
            run = ET.SubElement(formatted_text, "run")
            run.text = new_description
            column.append(new_desc)

    # Guardar el árbol XML en un BytesIO
    out_bytes = BytesIO()
    tree.write(out_bytes, encoding="utf-8", xml_declaration=True)
    out_bytes.seek(0)

    return out_bytes.read()

def update_tds_or_tdsx(file_obj, df_descriptions: pd.DataFrame) -> BytesIO:
    """
    Recibe un archivo .tds o .tdsx y el DataFrame con descripciones.
    Actualiza las descripciones y devuelve el contenido binario de un nuevo archivo
    (en formato TDS si era TDS, o TDSX si era TDSX).
    """
    filename = file_obj.name.lower()
    if filename.endswith(".tdsx"):
        # 1. Descomprimir todo en memoria
        with zipfile.ZipFile(file_obj, 'r') as z_in:
            # Leemos todos los archivos en un dict {nombre: bytes}
            in_memory_files = {}
            tds_file_name = None

            for name in z_in.namelist():
                in_memory_files[name] = z_in.read(name)
                if name.endswith(".tds"):
                    tds_file_name = name

        if not tds_file_name:
            raise ValueError("No se encontró ningún archivo .tds dentro del .tdsx para actualizar.")

        # 2. Actualizar TDS
        original_tds_content = in_memory_files[tds_file_name]
        updated_tds_content = update_descriptions_in_tds(original_tds_content, df_descriptions)

        # 3. Reemplazar el contenido del TDS en el dict
        in_memory_files[tds_file_name] = updated_tds_content

        # 4. Volver a crear el .tdsx en un BytesIO
        out_tdsx = BytesIO()
        with zipfile.ZipFile(out_tdsx, 'w', zipfile.ZIP_DEFLATED) as z_out:
            for name, content in in_memory_files.items():
                z_out.writestr(name, content)
        out_tdsx.seek(0)
        return out_tdsx

    elif filename.endswith(".tds"):
        # Directo sobre un TDS
        tds_content = file_obj.read()
        updated_content = update_descriptions_in_tds(tds_content, df_descriptions)

        # Retornamos un BytesIO con el TDS actualizado
        out_tds = BytesIO(updated_content)
        out_tds.seek(0)
        return out_tds

    else:
        raise ValueError("El archivo no es un .tds o .tdsx válido.")


# --------------------------- #
# APLICACIÓN STREAMLIT        #
# --------------------------- #

st.title("Asistente de Metadata para Datasources de Tableau")

modo = st.selectbox(
    "Elige la acción a realizar:",
    ["Exportar metadata a Excel", "Actualizar descripciones en TDS/TDSX"]
)

if modo == "Exportar metadata a Excel":
    st.write("Sube un archivo .tds o .tdsx para generar un Excel con las columnas y sus características.")
    uploaded_file = st.file_uploader("Subir archivo TDS o TDSX", type=["tds", "tdsx"])

    if uploaded_file is not None:
        if st.button("Procesar archivo y generar Excel"):
            try:
                df = process_tds_or_tdsx(uploaded_file)

                # Guardar el DataFrame en un archivo Excel en memoria
                excel_buffer = BytesIO()
                df.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)

                st.success("Archivo procesado con éxito.")
                st.download_button(
                    label="Descargar Excel",
                    data=excel_buffer,
                    file_name="Metadata_Tableau.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Error al procesar el archivo: {e}")

elif modo == "Actualizar descripciones en TDS/TDSX":
    st.write("Sube un archivo .tds o .tdsx y un Excel con columnas [Nombre, Descripción].")
    
    st.markdown("### Ejemplo de Formato de Excel")
    st.write(
        """Asegúrate de que tu archivo Excel contenga **al menos** las columnas:
        **"Nombre"** y **"Descripción"**."""
    )
    
    # Mostrar tabla de ejemplo
    df_example = pd.DataFrame({
        "Nombre": ["ID_Cliente", "Nombre_Cliente", "Fecha_Alta", "Monto_Pago"],
        "Descripción": [
            "Identificador único de cada cliente.",
            "Nombre completo del cliente.",
            "Fecha en la que se dio de alta al cliente.",
            "Cantidad abonada por el cliente en su última transacción."
        ]
    })
    st.table(df_example)

    uploaded_tds = st.file_uploader("Subir archivo TDS o TDSX", type=["tds", "tdsx"])
    uploaded_excel = st.file_uploader("Subir archivo Excel con descripciones", type=["xlsx", "xls"])

    if uploaded_tds is not None and uploaded_excel is not None:
        if st.button("Actualizar Descripciones"):
            try:
                # 1. Leer el Excel para obtener el DataFrame con [Nombre, Descripción]
                df_desc = pd.read_excel(uploaded_excel)

                # 2. Actualizar descripciones en el TDS/TDSX
                updated_file_bytesio = update_tds_or_tdsx(uploaded_tds, df_desc)

                # 3. Preparar nombre de archivo resultante
                original_filename = uploaded_tds.name
                updated_filename = "Actualizado_" + original_filename

                st.success("Descripciones actualizadas con éxito.")
                st.download_button(
                    label="Descargar TDS/TDSX Actualizado",
                    data=updated_file_bytesio.getvalue(),
                    file_name=updated_filename,
                    mime="application/octet-stream"
                )
            except Exception as e:
                st.error(f"Error al actualizar descripciones: {e}")
