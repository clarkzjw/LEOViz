import re
import httpx
import subprocess

POP_JSON = "https://raw.githubusercontent.com/clarkzjw/starlink-geoip-data/refs/heads/master/map/pop.json"


def get_pop_data(centralLat, centralLon, offsetLat, offsetLon) -> dict:
    try:
        response = httpx.get(POP_JSON)
        response.raise_for_status()
        data = response.json()
        lats = []
        lons = []
        names = []
        for pop in data:
            if pop.get("show") is True and pop.get("code") != "" and pop.get("type") == "netfac":
                if float(pop.get("lat")) < centralLat - offsetLat or float(pop.get("lat")) > centralLat + offsetLat:
                    continue
                if float(pop.get("lon")) < centralLon - offsetLon or float(pop.get("lon")) > centralLon + offsetLon:
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
        return {}
    except ValueError as e:
        print(f"An error occurred while parsing POP data: {e}")
        return {}


def get_home_pop():
    regex = r"^customer\.(.+)\.pop\.starlinkisp\.net\.$"

    def _get_home_pop(ipversion: int = 4):
        cmd = ["curl", "-4" if ipversion == 4 else "-6", "ipconfig.io", "-s"]
        try:
            ip = subprocess.check_output(cmd).decode().strip()
            cmd = ["dig", "-x", ip, "+short"]
            try:
                hostname = subprocess.check_output(cmd).decode().strip()

                match = re.match(regex, hostname)
                if match:
                    return match.group(1)
                else:
                    print(f"{hostname} does not match customer.<pop>.pop.starlinkisp.net. format")
                    return ""
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while dig DNS PTR record for {ip}: {e}")
                return ""
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while fetching IP address: {e}")
            return ""

    pop4 = _get_home_pop(4)
    pop6 = _get_home_pop(6)
    if pop4 == pop6:
        return pop4
    else:
        print(
            f"IPv4 and IPv6 PoPs do not match: {pop4} (IPv4) vs {pop6} (IPv6). Likely Starlink DNS configuration error."
        )
        return pop4
