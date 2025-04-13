# flake8: noqa: E501

from pprint import pprint

import httpx
from fastkml import kml

GS_KML = "https://www.google.com/maps/d/kml?mid=1805q6rlePY4WZd8QMOaNe2BqAgFkYBY&resourcekey&forcekml=1"


def get_gs_data(centralLat, centralLon, offsetLat, offsetLon):
    try:
        response = httpx.get(GS_KML)
        response.raise_for_status()
        k = kml.KML()

        doc = k.from_string(response.content)
    except httpx.RequestError as e:
        print(f"An error occurred while fetching POP data: {e}")
        return None
    except ValueError as e:
        print(f"An error occurred while parsing POP data: {e}")
        return None


if __name__ == "__main__":
    centralLat = 37.7749
    centralLon = -122.4194
    offsetLat = 0.1
    offsetLon = 0.1
    get_gs_data(centralLat, centralLon, offsetLat, offsetLon)
