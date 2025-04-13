# flake8: noqa: E501

import httpx
import json

POP_JSON = "https://raw.githubusercontent.com/clarkzjw/starlink-geoip-data/refs/heads/master/map/pop.json"


def get_pop_data(centralLat, centralLon, offsetLat, offsetLon):
    try:
        response = httpx.get(POP_JSON)
        response.raise_for_status()
        data = response.json()
        lats = []
        lons = []
        names = []
        for pop in data:
            if (
                pop.get("show") == True
                and pop.get("code") != ""
                and pop.get("type") == "netfac"
            ):
                if (
                    float(pop.get("lat")) < centralLat - offsetLat
                    or float(pop.get("lat")) > centralLat + offsetLat
                ):
                    continue
                if (
                    float(pop.get("lon")) < centralLon - offsetLon
                    or float(pop.get("lon")) > centralLon + offsetLon
                ):
                    continue
                lats.append(pop.get("lat"))
                lons.append(pop.get("lon"))
                names.append(pop.get("code"))
        return {
            "lats": lats,
            "lons": lons,
            "names": names,
        }
    except httpx.RequestError as e:
        print(f"An error occurred while fetching POP data: {e}")
        return None
    except ValueError as e:
        print(f"An error occurred while parsing POP data: {e}")
        return None
