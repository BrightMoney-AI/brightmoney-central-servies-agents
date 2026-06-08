"""
Script to create .env file from Consul for brightmoney-central-servies-agents.
"""

import consul
from pathlib import Path
import os


file_path = os.path.abspath(__file__)
directory = Path(os.path.dirname(file_path))


def create_env():
    CONSUL_ENV_KEY = "central-services-agents/prod/env"
    client = consul.Consul(token=os.environ['CONSUL_TOKEN'], host='consul.brightmoney.co', port=80)
    data = client.kv.get(CONSUL_ENV_KEY, recurse=True)

    env_values = []

    for key_item in data[1]:
        _, key = key_item["Key"].split(CONSUL_ENV_KEY, 1)
        key = key.strip("/")
        value = key_item["Value"].decode() if key_item["Value"] else ""
        env_values.append("{}={}".format(key, value))

    env_data = "\n".join(env_values)

    with open(str(Path(directory.parent.parent) / ".env"), "w") as fp:
        fp.write(env_data)


if __name__ == "__main__":
    create_env()
