from io import BytesIO
from functions.src.utils import get_df_from_pdf_exam


# script de teste da ia
def read_exam(file: bytes) -> None:
    df = get_df_from_pdf_exam(file)
    df = df.reset_index(drop=True)
    buf = BytesIO()
    df.to_feather(buf)
    print(df.head())
    print(df.dtypes)
    print(df.shape)
    return None


if __name__ == "__main__":
    with open("data/exam.pdf", "rb") as f:
        file = f.read()
    read_exam(file)
