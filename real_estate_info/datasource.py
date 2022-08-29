import asyncio
import os

import numpy as np
import pandas as pd
from sodapy import Socrata


class SocrataDataSource:
    def __init__(
        self,
        socrata_client: Socrata,
        api_req_page_size: int = 1000,
    ):
        self._client = socrata_client
        self._api_req_page_size = api_req_page_size

    async def years_available(self) -> list[int]:
        query = """
        select distinct listyear
        """
        resp = await asyncio.to_thread(
            self._client.get,
            os.environ["DATASET_ID"],
            query=query,
        )
        years = sorted([item["listyear"] for item in resp])
        return years

    async def sales_volume(self, year: int, top: int):
        query = f"""
        select town, sum(saleamount) as salesvolume
        where listyear = {year}
        group by town
        order by salesvolume desc
        limit {top}
        """
        resp = await asyncio.to_thread(
            self._client.get,
            os.environ["DATASET_ID"],
            query=query,
        )
        return resp

    async def avg_sales_ratio(self, year: int, top: int):
        query = f"""
        select count(*)
        where listyear = {year}
        """
        resp = await asyncio.to_thread(
            self._client.get,
            os.environ["DATASET_ID"],
            query=query,
        )
        total_items = int(resp[0]["count"])

        page_size = self._api_req_page_size
        pages = total_items // page_size + 1 if total_items % page_size else 0

        rows = []
        for page_number in range(pages):
            query = f"""
            select town, salesratio
            where listyear = {year}
            order by serialnumber
            offset {page_number * page_size}
            limit {page_size}
            """
            resp = await asyncio.to_thread(
                self._client.get,
                os.environ["DATASET_ID"],
                query=query,
            )
            rows.extend(resp)
        df = pd.DataFrame.from_records(rows)
        df["salesratio"] = pd.to_numeric(df.salesratio)
        df = (
            df.groupby("town")
            .agg(np.average)
            .sort_values(by="salesratio", ascending=False)[:top]
        )
        df = df.reset_index().values.tolist()
        return df
