import time

from authlib.integrations.requests_client import OAuth2Session

# General configuration
audience = "https://bluefield.cognitedata.com"

# Configuration Auth0
client_id = "HkPh5dLe4CF0VOksCYGGrrxjTQ8gCPVL"
device_authorization_endpoint = "https://cognite-trials.eu.auth0.com/oauth/device/code"
token_endpoint = "https://cognite-trials.eu.auth0.com/oauth/token"
scope = "IDENTITY user_impersonation"

# Configuration Cognite Greger dev client
# client_id = "6404274f-ce7c-4f50-97f4-126424cf2fac"
# tenant = "devgreger.onmicrosoft.com"
# device_authorization_endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/devicecode"
# token_endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/token"
# scope = f"{audience}/.default"


# Create an OAuth2 session
client = OAuth2Session(client_id)


# Step 1: Request Device Code
def request_device_code():
    try:
        response = client.fetch_token(
            url=device_authorization_endpoint,
            grant_type="urn:ietf:params:oauth:grant-type:device_code",
            audience=audience,
            scope=scope,
        )
        return response
    except Exception as e:
        print("Error:", e)
        exit(1)


# Step 2: Poll for Access Token
def poll_for_token(device_code):
    while True:
        try:
            token = client.fetch_token(
                url=token_endpoint,
                grant_type="urn:ietf:params:oauth:grant-type:device_code",
                device_code=device_code,
                code=device_code,
            )
            return token
        except Exception as e:
            if "authorization_pending" in str(e):
                time.sleep(5)  # Wait before polling again
            else:
                raise e


# Main flow
device_code_response = request_device_code()
print("For Microsoft: go to https://aka.ms/devicelogin")
print(f"Visit {device_code_response['verification_uri']} and enter the code: {device_code_response['user_code']}")

# Poll for the token
token = poll_for_token(device_code_response["device_code"])
print("Access Token:", token["access_token"])
