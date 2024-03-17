from pickle import FALSE
from web3 import Web3
from ratelimit import limits, sleep_and_retry
import setting

ACC_INVALID = 0
ACC_EOA = 1
ACC_CONTRACT = 2


class EthereumModel():

    def __init__(self):
        pass

    @sleep_and_retry
    @limits(calls=600, period=60)
    def account_type(self, addr):
        '''
        description: query account type
        return {*}
        '''
        w3 = Web3(Web3.HTTPProvider(setting.RPC_SETTINGS["polygon"]))

        if w3.is_address(Web3.to_checksum_address(addr)) == FALSE:
            return ACC_INVALID
        bytecode = w3.eth.get_code(Web3.to_checksum_address(addr))
        if len(bytecode) == 0:
            return ACC_EOA
        return ACC_CONTRACT
