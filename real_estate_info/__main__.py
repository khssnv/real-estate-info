import os

import requests_cache
import sodapy
import uvicorn
from dotenv import load_dotenv
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from .datasource import SocrataDataSource
from .web import app as web_app
from .web import socrata_data_provider


@web_app.on_event("startup")
async def startup():
    FastAPICache.init(InMemoryBackend(), expire=int(os.environ["CACHE_TTL"]))


if __name__ == "__main__":
    load_dotenv()

    sodapy_socrata_client = sodapy.Socrata(
        os.environ["SOCRATA_DOMAIN"],
        os.environ["SOCRATA_APP_TOKEN"],
        username=os.environ["SOCRATA_API_KEY_ID"],
        password=os.environ["SOCRATA_API_KEY_SECRET"],
        timeout=60,
    )
    requests_cache.install_cache("sodapy-socrata-cache", backend="memory")
    socrata_data_source = SocrataDataSource(
        sodapy_socrata_client,
    )
    socrata_data_provider.set_instance(socrata_data_source)
    uvicorn.run(web_app, host=os.environ["HOST"], port=int(os.environ["PORT"]))
