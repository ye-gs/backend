# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

import functools
from io import BytesIO
import json
from firebase_functions import https_fn
from firebase_admin import initialize_app
from firebase_admin import auth
import logging

from src.utils import get_df_from_pdf_exam

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

initialize_app()


def token_required(fn: ...) -> ...:
    @functools.wraps(fn)
    def wrapper(req: https_fn.Request) -> https_fn.Response:
        if not req.headers.get("Authorization").startswith("Bearer"):
            return https_fn.Response(
                status=401,
                response=json.dumps({"message": "Unauthorized"}),
                headers={"Content-Type": "application/json"},
            )
        id_token = req.headers["Authorization"].split(" ")[1]
        try:
            decoded_token = auth.verify_id_token(id_token)
        except auth.InvalidIdTokenError:
            return https_fn.Response(
                status=401,
                response="Unauthorized",
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            return https_fn.Response(
                status=500,
                headers={"Content-Type": "application/json"},
                response=json.dumps({"message": f"Erro inesperado ocorreu: {e}"}),
            )
        uid = decoded_token["uid"]
        current_user = auth.get_user(uid)
        logger.info(f"User {current_user.email} authenticated")
        return fn(req, current_user)

    return wrapper


@https_fn.on_request(
    memory=512,
    timeout_sec=60,
    max_instances=1,
    min_instances=0,
)
def send_exam(req: https_fn.Request) -> https_fn.Response:
    if req.content_type != "application/pdf":
        return https_fn.Response(
            status=400,
            headers={"Content-Type": "application/json"},
            response=json.dumps({"message": "Content-Type must be application/pdf"}),
        )
    if req.method != "POST":
        return https_fn.Response(
            status=405,
            response=json.dumps({"message": "Method Not Allowed"}),
            headers={"Content-Type": "application/json"},
        )
    if req.content_length is None:
        return https_fn.Response(
            status=411,
            response=json.dumps({"message": "Length Required"}),
            headers={"Content-Type": "application/json"},
        )
    if req.content_length > 10_000_000:
        return https_fn.Response(
            status=413,
            response=json.dumps({"message": "Payload Too Large"}),
            headers={"Content-Type": "application/json"},
        )
    data = req.get_data()
    logger.info(f"User sent a file with {req.content_length} bytes")
    df = get_df_from_pdf_exam(data)
    df = df.reset_index(drop=True)
    buf = BytesIO()
    df.to_feather(buf)
    logger.info(df.dtypes)
    return https_fn.Response(
        status=200,
        response=json.dumps({"message": "File processed successfully"}),
        headers={"Content-Type": "application/json"},
    )
