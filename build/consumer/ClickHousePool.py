from clickhouse_driver import Client
import Config

class ClickHousePool():
    __active__connection__ = []
    __available__connection__ = []

    def __init__(self, size=200):
        self.__available__connection__ += [self.__new__connection__() for i in range(size)]

    def __new__connection__(self):
        return Client(host=Config.CLICKHOUSE_HOST, port=Config.CLICKHOUSE_PORT, user=Config.CLICKHOUSE_USER, password=Config.CLICKHOUSE_PASSWORD)

    def get_connection(self):
        try:
            client = self.__available__connection__.pop(0)
        except:
            client = self.__new__connection__()
        
        self.__active__connection__.append(client)
        return client

    def release_connection(self, client):
        self.__active__connection__.remove(client)
        self.__available__connection__.append(client)

    def len_active(self):
        return len(self.__active__connection__)

    def len_available(self):
        return len(self.__available__connection__)
