import json

from insite_plugin import InsitePlugin
from rcs_zetta import Zetta, ZettaParams


class Plugin(InsitePlugin):
    def can_group(self):
        return True

    def fetch(self, hosts):

        try:

            self.collector

        except Exception:

            params: ZettaParams = {
                "port": 3000,
                "apikey": "1234",
                "username": "admin",
                "password": "password",
                "host": hosts[-1],
            }

            self.collector = Zetta(**params)

        documents = self.collector.collect()

        return json.dumps(documents)
