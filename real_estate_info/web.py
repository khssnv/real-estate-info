from fastapi import Depends, FastAPI
from fastapi_cache.decorator import cache
from starlette.requests import Request
from starlette.responses import Response

from .datasource import SocrataDataSource

app = FastAPI()


class SocrataDataProvider:
    def __init__(self, instance: SocrataDataSource | None = None):
        self._instance = instance

    def set_instance(self, instance: SocrataDataSource) -> SocrataDataSource:
        self._instance = instance

    def __call__(self) -> SocrataDataSource:
        return self._instance


socrata_data_provider = SocrataDataProvider()


class Commons:
    def __init__(
        self,
        top: int = 10,
    ):
        self.top = top


@app.get("/sales-volume")
@cache()
async def read_sales_volume(
    request: Request,
    response: Response,
    commons: Commons = Depends(),
    socrata: SocrataDataSource = Depends(socrata_data_provider),
):
    return {
        year: await socrata.sales_volume(year, commons.top)
        for year in await socrata.years_available()
    }


@app.get("/sales-volume/{year}")
@cache()
async def read_sales_volume_by_year(
    request: Request,
    response: Response,
    year: int,
    commons: Commons = Depends(),
    socrata: SocrataDataSource = Depends(socrata_data_provider),
):
    return {year: await socrata.sales_volume(year, commons.top)}


@app.get("/sales-ratio")
@cache()
async def read_sales_ratio(
    request: Request,
    response: Response,
    commons: Commons = Depends(),
    socrata: SocrataDataSource = Depends(socrata_data_provider),
):
    return {
        year: await socrata.avg_sales_ratio(year, commons.top)
        for year in await socrata.years_available()
    }


@app.get("/sales-ratio/{year}")
@cache()
async def read_sales_ratio_by_year(
    request: Request,
    response: Response,
    year: int,
    commons: Commons = Depends(),
    socrata: SocrataDataSource = Depends(socrata_data_provider),
):
    return {year: await socrata.avg_sales_ratio(year, commons.top)}
