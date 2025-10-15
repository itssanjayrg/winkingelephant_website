import base64
import json
import requests
from boto3 import Session


def get_assumed_role_session(
    profile_name: str,
    region_name: str,
    role_arn: str,
    role_session_name: str,
) -> Session:
    session = Session(profile_name=profile_name, region_name=region_name)
    sts_client = session.client("sts")

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=role_session_name,
    )
    return Session(
        aws_access_key_id=assumed_role_object["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed_role_object["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed_role_object["Credentials"]["SessionToken"],
        region_name=region_name,
    )


def get_secret(session: Session, secret_name: str) -> dict:
    client = session.client("secretsmanager")
    secret_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret_response["SecretString"])


def get_base64_encoded_client_secret(session: Session, secret_name: str) -> str:
    secret_value = get_secret(session=session, secret_name=secret_name)

    # Base64 encode the Client ID: Secret with a colon between
    encoded = base64.urlsafe_b64encode(
        f"{secret_value['clientID']}:{secret_value['secret']}".encode("ascii")
    )

    return encoded.decode("ascii").strip("=")


def get_authorization_token(
    token_endpoint: str, role_name: str, client_secret: str
) -> str:
    payload = {
        "grant_type": "client_credentials",
        "scope": f"session:role:{role_name}",
    }

    response = requests.post(
        token_endpoint,
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {client_secret}",
            "Cache-Control": "no-cache",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
    )
    response.raise_for_status()
    response_json = json.loads(response.text)
    return response_json["access_token"]


def get_token(
    session: Session, secret_name: str, token_endpoint: str, role_name: str
) -> str:
    _client_secret = get_base64_encoded_client_secret(
        session=session, secret_name=secret_name
    )

    return get_authorization_token(
        token_endpoint=token_endpoint,
        role_name=role_name,
        client_secret=_client_secret,
    )
