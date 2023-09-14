def get_etherscan_link(network: str, path: str) -> str:
    if network.lower() == 'mainnet':
        return f'https://etherscan.io/{path}'
    else:
        return f'https://goerli.etherscan.io/{path}'
