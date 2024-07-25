from functools import wraps
from io import BytesIO
import json
from firebase_functions import https_fn
from firebase_admin import initialize_app
from firebase_admin import auth
from firebase_admin.auth import UserRecord
from firebase_functions import logger
from src.utils import get_df_from_pdf_exam

from firebase_functions.options import CorsOptions
from typing import Callable, TypeVar


initialize_app()


RT = TypeVar("RT")  # return type


def token_required(fn: Callable[..., RT]) -> Callable[..., RT | https_fn.HttpsError]:
    @wraps(fn)
    def wrapper(req: https_fn.Request, *args, **kwargs) -> RT | https_fn.HttpsError:
        error = validate_auth_header(req)

        if error:
            return error
        token = req.authorization.token
        uid_or_error = validate_token(token)
        if isinstance(uid_or_error, https_fn.Response):
            return uid_or_error
        current_user: UserRecord = auth.get_user(uid_or_error)
        return fn(req, *args, **kwargs, current_user=current_user)

    return wrapper


def validate_auth_header(req: https_fn.Request) -> https_fn.HttpsError | None:
    """
    Validates if request has authorization header and token is in it
    - req: Request object
    -----------------------
    Returns: None if valid, otherwise a error response object
    """
    if not req.authorization or not req.authorization.token:
        return https_fn.Response(
            status=https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            response=json.dumps({"message": "Authorization header required"}),
            content_type="application/json",
        )
    return None


def validate_token(id_token: str) -> https_fn.HttpsError | str:
    """Validates id_token and returns user uid if valid

    Args:
        id_token: token do usuário

    Returns:
        https_fn.HttpsError | str: Erro ou uid
    """
    try:
        decoded_token = auth.verify_id_token(id_token)
        return decoded_token["uid"]
    except (ValueError, auth.InvalidIdTokenError):
        return https_fn.Response(
            status=https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            response=json.dumps({"message": "Invalid Token"}),
            content_type="application/json",
        )
    except Exception as e:
        return https_fn.Response(
            status=https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            response=json.dumps({"message": f"Erro inesperado ocorreu {e}"}),
            content_type="application/json",
        )


@https_fn.on_request(
    memory=512,
    timeout_sec=60,
    max_instances=1,
    min_instances=0,
    cors=CorsOptions(
        cors_methods=["POST"],
        cors_origins="*",
    ),
    region="southamerica-east1",
)
@token_required
def send_exam(req: https_fn.Request, current_user: UserRecord) -> https_fn.Response:
    if req.content_type != "application/pdf":
        return https_fn.Response(
            status=400,
            content_type="application/json",
            response=json.dumps({"message": "Content-Type must be application/pdf"}),
        )
    if req.method != "POST":
        return https_fn.Response(
            status=405,
            response=json.dumps({"message": "Method Not Allowed"}),
            content_type="application/json",
        )
    if req.content_length is None:
        return https_fn.Response(
            status=411,
            response=json.dumps({"message": "Length Required"}),
            content_type="application/json",
        )
    if req.content_length > 5_000_000:
        return https_fn.Response(
            status=413,
            response=json.dumps({"message": "Payload Too Large"}),
            content_type="application/json",
        )
    data = req.get_data()
    logger.info(
        f"User {current_user.email} sent a file with {req.content_length} bytes"
    )
    df = get_df_from_pdf_exam(data)
    df = df.reset_index(drop=True)
    logger.info("File processed successfully")
    return https_fn.Response(
        status=200,
        response=json.dumps({"message": "File processed successfully"}),
        content_type="application/json",
    )
