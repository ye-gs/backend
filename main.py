from typing import Any, Dict
from fastapi import FastAPI, UploadFile
from pandas import DataFrame, Series, concat
from pymupdf import open, Page
from pymupdf.table import TableFinder


def eliminate_junk(tab: TableFinder) -> DataFrame:
    df = tab.to_pandas()  # convert to pandas DataFrame
    num_col = df.iloc[0].value_counts().iloc[0]  # verifica se ha colunas inúteis
    if num_col != 1:
        num_col += 1
    df = df.iloc[:, :-num_col].join(df.iloc[:, -1:])  # remove as colunas inúteis
    columns = df.iloc[0]  # pega a primeira linha como cabeçalho
    df = df[1:]  # remove a primeira linha
    # seta a coluna RESULTADOS, COL2, COL3 como Resultados - columns 1, 2 e 3
    print(columns)
    return df


app = FastAPI()


def verifica_colunas(cell: Series) -> Any:
    for c in cell.columns:
        if "(1)" in cell[c]:
            cell[c] = cell[c].str.replace(r"\(1\)", "", regex=True)
            cell["Extrapolou"] = True
        else:
            cell["Extrapolou"] = False
        if "*" in cell[c]:
            cell[c] = cell[c].str.replace(r"\*", "", regex=True)
            cell["Varia com idade"] = True
        else:
            cell["Varia com idade"] = False
    return cell


@app.post("/upload/")
async def create_upload_file(file: UploadFile) -> Dict[str, Any]:
    if not file.filename or not file.filename.endswith(".pdf"):
        raise ValueError("Arquivo inválido! É necessário um arquivo pdf")
    df_final = DataFrame()
    doc = open(stream=file.file.read())  # open a document
    for page in doc:  # iterate the document pages
        if isinstance(page, Page):
            tabs = page.find_tables()  # find tables in the page

            for tab in tabs:
                df = eliminate_junk(tab)  # convert to pandas DataFrame
                # df = df.apply(
                #     verifica_colunas, axis=1
                # )  # verifica se há asteriscos ou colunas na primeira linha
                df_final = concat([df_final, df])  # append to the final DataFrame

    return {
        "info": "Tabelas extraídas com sucesso!",
        "tabelas": df_final.to_dict(orient="records"),
    }
