"""
Bootstraps the Cosmos DB emulator for the dynein-dynamodb case study.

aws2azure's DynamoDB module proxies onto an *existing* Cosmos database --
it does not create the database itself (see docs/getting-started.md in
aws2azure: "The database must already exist"). This talks to the Cosmos
emulator directly (bypassing aws2azure) with the Azure Cosmos SDK to
create the "aws2azure" database, using the emulator's well-known fixed
master key (the same constant Microsoft documents for every Cosmos
emulator instance -- not a secret).

Must be run once after `docker compose up -d` and before any DynamoDB
call is issued through aws2azure.
"""
import sys
import time

from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

COSMOS_ENDPOINT = "http://localhost:8081/"
# Well-known fixed master key documented by Microsoft for every Cosmos DB
# emulator instance (identical across machines/installs) -- not a secret.
COSMOS_KEY = (
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
)
DATABASE_NAME = "aws2azure"


def main() -> None:
    # vnext-preview's gateway returns internal container IPs in its
    # replica-set discovery response, which aren't reachable from the
    # host -- disabling endpoint discovery keeps every request pinned to
    # the endpoint we were given.
    client = CosmosClient(
        COSMOS_ENDPOINT,
        credential=COSMOS_KEY,
        connection_mode="Gateway",
        enable_endpoint_discovery=False,
    )

    last_err = None
    for attempt in range(30):
        try:
            db = client.create_database_if_not_exists(DATABASE_NAME)
            print(f"Cosmos database '{db.id}' ready")
            return
        except CosmosHttpResponseError as e:
            last_err = e
            time.sleep(2)
    print(f"FAIL: could not create Cosmos database after retries: {last_err}")
    sys.exit(1)


if __name__ == "__main__":
    main()
