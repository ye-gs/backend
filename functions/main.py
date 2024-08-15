import json
from functools import wraps
from typing import Callable, TypeVar
import firebase_admin
from firebase_admin import auth, credentials, firestore
from firebase_functions import https_fn, logger
from firebase_functions.options import CorsOptions
from src.utils import get_df_from_pdf_exam

# Initialize Firebase Admin SDK
cred = credentials.Certificate("./serviceAccountKey.json")
firebase_admin.initialize_app(cred)

RT = TypeVar("RT")  # return type


def token_required(fn: Callable[..., RT]) -> Callable[..., RT | https_fn.HttpsError]:
    @wraps(fn)
    def wrapper(req: https_fn.Request, *args, **kwargs) -> RT | https_fn.HttpsError:
        logger.info("Verifying for token on header")
        auth_header = req.headers.get("Authorization")
        if not auth_header:
            return https_fn.Response(
                status=401,
                content_type="application/json",
                response=json.dumps({"error": "Authorization header is missing"}),
            )

        try:
            logger.info("splitting token")
            token = auth_header.split(" ")[1]
        except IndexError:
            return https_fn.Response(
                status=401,
                content_type="application/json",
                response=json.dumps({"error": "Authorization header is malformed"}),
            )

        try:
            logger.info("Verifying token")
            decoded_token = auth.verify_id_token(token)
            logger.info("Token decoded successfully")
            current_user = auth.get_user(decoded_token["uid"])
            logger.info(f"calling function for user: {current_user.email}")
        except Exception as e:
            return https_fn.Response(
                status=401,
                content_type="application/json",
                response=json.dumps({"error": "Invalid token", "details": str(e)}),
            )

        return fn(req, current_user, *args, **kwargs)

    return wrapper


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
def send_exam(
    req: https_fn.Request, current_user: auth.UserRecord
) -> https_fn.Response:
    logger.info("Verifying content-type")
    if req.content_type != "application/pdf":
        return https_fn.Response(
            status=400,
            content_type="application/json",
            response=json.dumps({"message": "Content-Type must be application/pdf"}),
        )
    logger.info("Verifying called method and content length")
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
    logger.info("Verifying content length")
    if req.content_length > 5_000_000:
        return https_fn.Response(
            status=413,
            response=json.dumps({"message": "Payload Too Large"}),
            content_type="application/json",
        )
    logger.info("Reading file")
    data = req.get_data()
    logger.info(
        f"User {current_user.email} sent a file with {req.content_length} bytes"
    )
    try:
        logger.info("Processing file")
        df = get_df_from_pdf_exam(data)
        df = df.reset_index(drop=True)
        document = df.to_dict(orient="list")
        client = firestore.client()
        logger.info("Saving file to Firestore")
        _, doc_ref = client.collection("users/" + current_user.uid + "/exams").add(
            document
        )
        logger.info("File processed successfully")
        return https_fn.Response(
            status=200,
            response=json.dumps(
                {
                    "message": "File processed successfully",
                    "id": doc_ref.id,
                }
            ),
            content_type="application/json",
        )
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return https_fn.Response(
            status=500,
            response=json.dumps({"message": "Internal Server Error", "detail": str(e)}),
            content_type="application/json",
        )


# Ensure the function is exported for Firebase Functions
exports = {"send_exam": send_exam}
