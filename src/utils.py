from pandas import DataFrame, Series, concat, to_numeric
from pymupdf import open, Page
from pymupdf.table import TableFinder


def trata_colunas_iniciais(df: DataFrame, num_col: int) -> DataFrame:
    """
    This function is used to clean and prepare the initial columns of a DataFrame.
    It removes unnecessary columns, sets the first row as the header,
    and renames specific columns.

    Parameters:
    df (DataFrame): The input DataFrame containing the raw data.
    num_col (int): The number of unnecessary columns at the end of the DataFrame.

    Returns:
    DataFrame: The cleaned and prepared DataFrame with the specified modifications.
    """
    df = df.iloc[:, :-num_col].join(df.iloc[:, -1:])  # remove as colunas inúteis
    columns = df.iloc[0]  # pega a primeira linha como cabeçalho
    df = df[1:]  # remove a primeira linha
    # seta a coluna RESULTADOS, COL2, COL3 como Resultados - columns 1, 2 e 3
    dict_cols = {}
    for i, c in enumerate(columns):
        if c is not None and "\n" in c:
            ficha, data = c.split("\n")
            dict_cols[i] = {"ficha": ficha, "data": data}
    df_cols = list(df.columns)
    for key, value in dict_cols.items():
        df_cols[key] = f"RES - ficha: {value['ficha']} - {value['data']}"
    df.columns = df_cols
    return df


def eliminate_junk_and_rename_cols(tab: TableFinder) -> DataFrame:
    """
    This function eliminates unnecessary columns and renames specific
    columns in a DataFrame obtained from a table in a PDF document.

    Parameters:
    tab (TableFinder): A TableFinder object representing a table in a PDF document.
    The object is obtained using the pymupdf library.

    Returns:
    DataFrame: A pandas DataFrame containing the cleaned and prepared data from the
    table.

    The function performs the following steps:
    1. Converts the table data to a pandas DataFrame.
    2. Determines the number of unnecessary columns at the end of the DataFrame.
    3. Calls the trata_colunas_iniciais function to clean and prepare the initial
    columns of the DataFrame.
    4. Returns the cleaned and prepared DataFrame.
    """
    df = tab.to_pandas()  # convert to pandas DataFrame
    num_col = df.iloc[0].value_counts().iloc[0]  # verifica se ha colunas inúteis
    if num_col != 1:
        num_col += 1
    df = trata_colunas_iniciais(df, num_col)

    return df


def parse_number_cols(cell: Series) -> Series:
    """
    This function is used to parse and clean numeric data in a pandas Series.
    It replaces specific characters, converts the data to numeric format,
    and adds a new column indicating if a reference varies with age.

    Parameters:
    cell (Series): A pandas Series containing the data to be parsed.

    Returns:
    Series: The cleaned and parsed pandas
    Series with an additional column indicating if a reference varies with age.
    """
    found = False
    for c in cell.index:
        if "(1)" in cell[c]:
            cell[c] = cell[c].replace("(1)", "")
        if "(*)" in cell[c]:
            cell[c] = cell[c].replace("(*)", "")
            found = True
        if "----" in cell[c]:
            cell[c] = cell[c].replace("----", "")
        cell[c] = to_numeric(cell[c].replace(".", "").replace(",", "."))
    cell["Referência varia com idade"] = found
    return cell


def mapeia_valores_referencia(cell: str | None) -> tuple[str | None, ...]:
    """
    This function is used to map and parse reference values from a given cell
    in a PDF table. It extracts the lower and upper limits,
    as well as the unit of measurement, from the cell content.

    Parameters:
    cell (str | None): The cell content from which to extract the reference values.

    Returns:
    tuple[str | None,...]: A tuple containing the lower limit,
    upper limit, and unit of measurement.
    If the cell content does not contain reference values,
    the tuple will contain None for all values.
    """
    superior, inferior, unidade = None, None, "Não encontrado pela IA..."
    if not isinstance(cell, str):
        return None, None, None
    if cell == "----":
        return None, None, None
    if " a " in cell:
        inferior, superior_e_unidade = cell.split(" a ")
        inferior = inferior.strip()
        if ":" in inferior.lower():
            inferior = inferior.split(":")[1]
        if " " in superior_e_unidade:
            superior, unidade = superior_e_unidade.split(" ")
            superior = superior.strip()
        elif "/" in superior_e_unidade:
            superior, unidade = superior_e_unidade.split("/")
            unidade = "/" + unidade
            superior = superior.strip()
    elif "Ver resultado tradicional" in cell:
        inferior, superior, unidade = None, None, "Ver resultado tradicional"
    elif "jejum" in cell:
        tupla_jejum = cell.split("\n")
        if len(tupla_jejum) == 2:
            com_jejum, sem_jejum = tupla_jejum
            if "menor que " in sem_jejum.lower() or "< " in sem_jejum:
                if "< " in sem_jejum:
                    inferior_e_unidade = sem_jejum.split("< ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
                else:
                    inferior_e_unidade = sem_jejum.split("enor que ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
            elif "menor que " in com_jejum.lower() or "< " in com_jejum:
                if "< " in sem_jejum:
                    inferior_e_unidade = sem_jejum.split("< ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
                else:
                    superior_e_unidade = com_jejum.split("enor que ")[1]
                    superior, unidade = superior_e_unidade.split(" ")
            elif "maior que " in sem_jejum.lower() or "> " in sem_jejum:
                if "> " in sem_jejum:
                    inferior_e_unidade = sem_jejum.split("> ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
                else:
                    inferior_e_unidade = sem_jejum.split("aior que ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
            elif "maior que " in com_jejum.lower() or "> " in com_jejum:
                if "> " in sem_jejum:
                    inferior_e_unidade = sem_jejum.split("> ")[1]
                    inferior, unidade = inferior_e_unidade.split(" ")
                else:
                    superior_e_unidade = com_jejum.split("aior que ")[1]
                    superior, unidade = superior_e_unidade.split(" ")
        else:
            inferior, unidade, superior, _ = tupla_jejum
            if "menor que " in inferior.lower() and "menor que " in superior.lower():
                inferior = inferior.split("enor que ")[1].strip()
                superior = superior.split("enor que ")[1].strip()
            if "maior que " in inferior.lower() and "maior que " in superior.lower():
                inferior = inferior.split("enor que ")[1].strip()
                superior = superior.split("enor que ")[1].strip()
            if "< " in inferior.lower() and "< " in superior.lower():
                inferior = inferior.split("< ")[1].strip()
                superior = superior.split("< ")[1].strip()
            if "> " in inferior.lower() and "> " in superior.lower():
                inferior = inferior.split("> ")[1].strip()
                superior = superior.split("> ")[1].strip()
    elif "menor que " in cell.lower():
        inferior_e_unidade = cell.split("enor que ")[1]
        inferior, unidade = inferior_e_unidade.split(" ")
    elif "maior que " in cell.lower():
        superior_e_unidade = cell.split("aior que ")[1]
        superior, unidade = superior_e_unidade.split(" ")
    elif "< " in cell:
        inferior_e_unidade = cell.split("< ")[1]
        inferior, unidade = inferior_e_unidade.split(" ")
    elif "> " in cell:
        superior_e_unidade = cell.split("> ")[1]
        superior, unidade = superior_e_unidade.split(" ")
    elif "até" in cell.lower():
        superior_e_unidade = cell.split("té ")[1]
        superior, unidade = superior_e_unidade.split(" ")
    else:
        inferior, superior, unidade = None, None, unidade

    return inferior, superior, unidade


def parseia_referencia(df: DataFrame, referencia_cols: list[str]) -> DataFrame:
    if len(referencia_cols) > 1:
        raise ValueError(
            "A IA detectou mais de uma possível coluna com valor de referência"
            f"Possíveis colunas: {referencia_cols}"
            "Favor inserir o nome da coluna"
        )
    elif len(referencia_cols) == 0:
        raise ValueError(
            "A IA não detectou nenhuma coluna com valor de referência"
            "Favor inserir o nome da coluna"
        )
    df[["Limite inferior", "Limite superior", "Unidade"]] = DataFrame(
        df[referencia_cols[0]].map(mapeia_valores_referencia).tolist(), index=df.index
    )
    for col in ["Limite inferior", "Limite superior"]:
        df[col] = to_numeric(df[col].str.replace(".", "").str.replace(",", "."))
    return df


def trata_e_extrai_limites(df: DataFrame) -> DataFrame:
    """
    This function is used to process and extract reference limits from a DataFrame.
    It identifies the relevant columns, applies parsing and cleaning operations, and
    extracts the lower and upper limits, as well as the unit of measurement.

    Parameters:
    df (DataFrame): The input DataFrame containing the raw data.

    Returns:
    DataFrame: The processed DataFrame with additional columns
    for lower limit, upper limit, and unit of measurement.

    The function performs the following steps:
    1. Identifies the relevant columns containing data related
    to the examination results.
    2. Applies the parse_number_cols function to each relevant column,
    cleaning and parsing the data.
    3. Identifies the column containing reference values.
    4. Calls the parseia_referencia function to extract the
    lower and upper limits, as well as the unit of measurement,
    from the reference values column.
    5. Returns the processed DataFrame with the additional columns.
    """
    data_cols = [column for column in df.columns if "ficha" in column]
    tratamento = df[data_cols].apply(parse_number_cols, axis=1)
    df[tratamento.columns] = tratamento
    referencia_cols = [c for c in df.columns if "valores de referência" in c.lower()]
    return parseia_referencia(df, referencia_cols)


def get_initial_data(content: bytes) -> DataFrame:
    """
    This function extracts initial data from a PDF document
    containing examination results.

    Parameters:
    content (bytes): The content of the PDF document as bytes.

    Returns:
    DataFrame: A pandas DataFrame containing the extracted data.

    The function performs the following steps:
    1. Opens the PDF document using the pymupdf library.
    2. Iterates through each page of the document.
    3. Finds tables in each page using the pymupdf library.
    4. Calls the eliminate_junk_and_rename_cols function to clean
    and prepare each table.
    5. Appends the cleaned and prepared table to a final DataFrame.
    6. Returns the final DataFrame containing the extracted data.
    """
    df = DataFrame()
    doc = open(stream=content)  # open a document
    for page in doc:  # iterate the document pages
        if isinstance(page, Page):
            tabs = page.find_tables()  # find tables in the page

            for tab in tabs:
                df = concat(
                    [df, eliminate_junk_and_rename_cols(tab)]
                )  # append to the final DataFrame
    return df


def get_df_from_pdf_exam(content: bytes) -> DataFrame:
    """
    This function extracts initial data from a PDF document
    containing examination results.

    Parameters:
    content (bytes): The content of the PDF document as bytes.

    Returns:
    DataFrame: A pandas DataFrame containing the extracted data.

    The function performs the following steps:
    1. Opens the PDF document using the pymupdf library.
    2. Iterates through each page of the document.
    3. Finds tables in each page using the pymupdf library.
    4. Calls the eliminate_junk_and_rename_cols function to clean and
    prepare each table.
    5. Appends the cleaned and prepared table to a final DataFrame.
    6. Calls the trata_e_extrai_limites function to process and extract
    reference limits from the final DataFrame.
    7. Returns the final DataFrame containing the extracted data with
    additional columns for lower limit, upper limit, and unit of measurement.
    """
    df = get_initial_data(content)
    trata_e_extrai_limites(df)
    return df
