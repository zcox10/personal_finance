from google.cloud import secretmanager


class SecretsUtils:

    def __init__(self):
        pass

    def get_secrets(self, secret_id, project_id="zsc-personal", version_id="latest"):
        """
        Retrieve a secret from Google Cloud Secrets Manager.

        Args:
            project_id (str): The ID of the Google Cloud project.
            secret_id (str): The ID of the secret.
            version_id (str): The ID of the secret version.

        Returns:
            str: The plain text value of the secret.
        """

        # Create Sercet Version Reference
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Instantiate Secret Manager Client
        client = secretmanager.SecretManagerServiceClient()

        # Get Response
        response = client.access_secret_version(request={"name": name})

        # Return 'Plain Text' Key
        return response.payload.data.decode("UTF-8")

    def get_access_token_secrets(self, secrets_dict):
        """
        Extract Plaid access token secrets from a dictionary of secrets.

        Args:
            secrets_dict (dict): A dictionary containing secrets.

        Returns:
            list: A list of access token secrets.
        """

        secrets_list = []
        for k, v in secrets_dict.items():
            if k.startswith("PLAID_TOKEN_"):
                secrets_list.append(v)

        return secrets_list

    def create_secrets_dict(self, secret_type, project_id="zsc-personal", version_id="latest"):
        """
        Create a dictionary containing secrets.

        Args:
            secret_type (str): The type of secret to use.
            project_id (str, optional): The ID of the Google Cloud project. Defaults to "zsc-personal".
            version_id (str, optional): The version of the secret to retrieve. Defaults to "latest".

        Returns:
            dict: A dictionary containing the secrets needed for the specified job.
        """

        # secrets to gather
        secrets = [
            "PLAID_CLIENT_ID",
            f"PLAID_SECRET_{secret_type}",
            "PLAID_TOKEN_BOA",
            "PLAID_TOKEN_CHASE",
            "PLAID_TOKEN_ETRADE",
            "PLAID_TOKEN_FUNDRISE",
            "PLAID_TOKEN_SCHWAB",
            "PLAID_TOKEN_VANGUARD",
        ]

        secrets_dict = {}
        for secret in secrets:
            secret_key = self.get_secrets(secret, project_id, version_id)
            secrets_dict[secret] = secret_key

        return secrets_dict
