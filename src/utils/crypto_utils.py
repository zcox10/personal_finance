import requests
import sys


class CryptoUtils:

    def __init__(self):
        pass

    def btc_cost_basis(self, btc_address):
        # URL to get the transaction history of the BTC address
        transactions_url = f"https://blockchain.info/rawaddr/{btc_address}"

        # Make the request to get transaction history
        response = requests.get(transactions_url)

        transactions_data = response.json()
        transactions = transactions_data["txs"]

        total_bought = 0  # To keep track of total BTC bought

        for tx in transactions:
            for output in tx["out"]:
                if output["addr"] == btc_address:
                    total_bought += output["value"]  # Value is in satoshis

        # Convert from satoshis to BTC
        total_bought_btc = total_bought / 100000000
        return total_bought_btc

    def get_btc_balance(self, btc_address):
        """
        Retrieve the BTC balance of multiple BTC addresses

        Args:
            addrs (list): The BTC addresses

        Returns:
            float: The balance in BTC
        """

        # URL for getting the balance of the BTC address
        balance_url = f"https://blockchain.info/balance?active={btc_address}"

        # Get the balance of the BTC address
        balance_response = requests.get(balance_url)

        # Balance is returned in satoshis (1 BTC = 100,000,000 satoshis)
        balance_satoshis = int(next(iter(balance_response.json().values()))["final_balance"])
        balance_btc = balance_satoshis / 100000000
        return balance_btc

    def get_btc_to_usd_exchange_rate(self):
        """
        Retrieve the BTC to USD exchange rate

        Returns:
            float: The value of 1 BTC in USD
        """

        # URL for getting the BTC to USD exchange rate
        exchange_rate_url = "https://blockchain.info/ticker"

        # Get the BTC to USD exchange rate
        exchange_rate_response = requests.get(exchange_rate_url)

        exchange_rates = exchange_rate_response.json()
        btc_to_usd = exchange_rates["USD"]["last"]
        return btc_to_usd

    def get_eth_balance(self, api_key, eth_address):
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

        # if balance_response.status_code == 200:
        balance_data = balance_response.json()
        if balance_data["status"] == "1":
            # Balance is returned in wei (1 ETH = 10^18 wei)
            balance_wei = int(balance_data["result"])
            balance_eth = balance_wei / 10**18
            return balance_eth
        else:
            print("STATUS CODE:", balance_response.status_code)
            print(balance_data["status"])
            sys.exit(1)

    def get_eth_to_usd_exchange_rate(self):
        """
        Retrieve the ETH to USD exchange rate

        Returns:
            float: The value of 1 ETH in USD
        """
        # URL for getting the ETH to USD exchange rate from CoinGecko
        coingecko_url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

        # Get the ETH to USD exchange rate
        exchange_rate_response = requests.get(coingecko_url)

        exchange_rates = exchange_rate_response.json()
        eth_to_usd = exchange_rates["ethereum"]["usd"]
        return eth_to_usd

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
