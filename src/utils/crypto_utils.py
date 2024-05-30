import requests
import numpy as np
import sys


class CryptoUtils:

    def __init__(self):
        pass

    def get_btc_balance(self, btc_address, api_key):
        """
        Retrieve the BTC balance of all addresses via xpub

        Args:
            xpub_key (str): The BTC xpub

        Returns:
            float: The balance in BTC
        """

        # define request
        url = "https://www.blockonomics.co/api/balance"
        payload = {"addr": f"{btc_address}"}
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

        # generate request
        response = requests.post(url, json=payload, headers=headers)
        balance_info = response.json()

        # sum up balance of all BTC addresses
        balance = 0
        for i in balance_info["response"]:
            balance += i["confirmed"]

        return balance / 1e8

    def get_btc_to_usd_exchange_rate(self, api_key):
        """
        Retrieve the BTC to USD exchange rate

        Args:
            api_key (str): The api_key from Blockonomics

        Returns:
            float: The value of 1 BTC in USD
        """

        # define request
        url = "https://www.blockonomics.co/api/price?currency=USD"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

        # generate request and return price in USD
        response = requests.get(url, headers=headers)
        return response.json()["price"]

    def get_eth_balance(self, eth_address, api_key):
        """
        Retrieve the ETH balance via eth_address

        Args:
            eth_address (str): The ETH address

        Returns:
            float: The balance in ETH
        """

        # URL for getting the balance of the ETH address
        etherscan_url = f"https://api.etherscan.io/api?module=account&action=balance&address={eth_address}&tag=latest&apikey={api_key}"

        # Get the balance of the ETH address
        balance_response = requests.get(etherscan_url)
        balance_data = balance_response.json()

        # Balance is returned in wei (1 ETH = 10^18 wei)
        balance_wei = int(balance_data["result"])
        balance_eth = balance_wei / 10**18
        return balance_eth

    def get_eth_to_usd_exchange_rate(self, api_key):
        """
        Retrieve the current price of ETH in USD using the Etherscan API.

        Args:
            api_key (str): Your Etherscan API key.

        Returns:
            float: The current price of ETH in USD.
        """
        url = f"https://api.etherscan.io/api?module=stats&action=ethprice&apikey={api_key}"

        response = requests.get(url)
        data = response.json()
        eth_price_usd = float(data["result"]["ethusd"])
        return eth_price_usd

    def convert_crypto_amount(self, balance, fx_rate):
        """
        Converts an amount of cryptocurrency to another currency via fx_rate.

        Args:
            balance (float): The value of the designated crypto in its local currency.
            fx_rate (float): The exchange rate for {crypto_token} to another currency.

        Returns:
            float: The value of the input cryptocurrency in the provided currency.
        """

        return balance * fx_rate

    def get_crypto_balances(self, eth_addresses, btc_addresses, eth_api_key, btc_api_key):
        """
        Retrieve and convert cryptocurrency balances to USD for given Ethereum and Bitcoin addresses.

        Args:
            eth_addresses (list): List of Ethereum addresses to check balances for.
            btc_addresses (list): List of Bitcoin addresses to check balances for.
            eth_api_key (str): API key for accessing Ethereum balance and exchange rate.
            btc_api_key (str): API key for accessing Bitcoin balance and exchange rate.

        Returns:
            dict: A dictionary containing the balances of each address in both the original cryptocurrency and USD.
                The dictionary keys are the addresses, and the values are another dictionary with the following keys:
                - "available": The balance in the original cryptocurrency.
                - "current": The balance converted to USD.
                - "limit": None (placeholder for future use).
                - "currency_code": "USD".
                - "unofficial_currency_code": "BTC" or "ETH".
        """
        # Initialize a dictionary to hold the balances
        crypto_balances = {}

        # Get the current exchange rate from BTC to USD
        btc_to_usd = self.get_btc_to_usd_exchange_rate(btc_api_key)

        # Process each Bitcoin address
        for addr in btc_addresses:
            # Get the balance of the Bitcoin address
            btc_balance = self.get_btc_balance(addr, btc_api_key)

            # Store the balance and converted amount in the dictionary
            crypto_balances[f"{addr}"] = {
                "available": btc_balance,
                "current": self.convert_crypto_amount(btc_balance, btc_to_usd),
                "limit": np.nan,
                "currency_code": "USD",
                "unofficial_currency_code": "BTC",
            }

        # Get the current exchange rate from ETH to USD
        eth_to_usd = self.get_eth_to_usd_exchange_rate(eth_api_key)

        # Process each Ethereum address
        for addr in eth_addresses:
            # Get the balance of the Ethereum address
            eth_balance = self.get_eth_balance(addr, eth_api_key)

            # Store the balance and converted amount in the dictionary
            crypto_balances[f"{addr}"] = {
                "available": eth_balance,
                "current": self.convert_crypto_amount(eth_balance, eth_to_usd),
                "limit": np.nan,
                "currency_code": "USD",
                "unofficial_currency_code": "ETH",
            }

        return crypto_balances
