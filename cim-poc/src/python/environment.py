import requests
import os
from dotenv import load_dotenv


IS_ON_K8S = os.getenv("KUBERNETES_SERVICE_HOST") is not None
SCOPES_DIR = os.getenv("SCOPES_DIR", "../../data/scopes")
MATCH_SCORE_THRESHOLD = float(os.getenv("MATCH_SCORE_THRESHOLD", 0.1))

def get_ping_token():
    """Used for OAUTH Authentication with Snowflake connector; this will be deprecated."""
    endpoint = "https://pingf.grainger.com/as/token.oauth2"
    request_payload = {
        "client_id": "snowflake",
        "grant_type": "password",
        "username": os.getenv("GPASS_USER"),
        "password": os.getenv("GPASS_PASSWORD"),
        "client_secret": os.getenv("OAUTH_CLIENT_SECRET"),
        "scope": "SESSION:ROLE-ANY",
    }
    resp = requests.post(endpoint, request_payload)
    return resp.json()["access_token"]


def read():
    load_dotenv()
    return {
        "role": os.getenv("SF_ROLE", "cido_validation_svc"),
        "account": os.getenv("SF_ACCOUNT", "wwgrainger.us-east-1"),
        "warehouse": os.getenv("SF_WAREHOUSE", "cido_wh_m"),
        "database": os.getenv("SF_DATABASE", "cido_validation"),
        "schema": os.getenv("SF_SCHEMA", "cim"),
        "user": os.getenv("SF_USER", "cido_validation_svc"),
        "password": os.getenv("SF_PASSWORD"),
        "ups_password": os.getenv("UPS_PASSWORD"),
        "svs_password": os.getenv("SVS_PASSWORD"),
        "ping_url": os.getenv("PING_URL"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "client_id": os.getenv("CLIENT_ID", "mim_ui"),
        "apikey": os.getenv("GEOCODIO_APIKEY"),
        "env": os.getenv("ENV", "prod"),
        "invalid_accounts_table": os.getenv("INVALID_ACCOUNTS_TABLE", "REFERENCE.INVALID_ACCOUNTS")
    }
