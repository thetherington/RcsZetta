"""Module providing collection of the RCS Zetta Simple API station status"""

import sys
from itertools import zip_longest
from threading import Thread
from typing import Dict, List, NotRequired, Tuple, TypedDict, Unpack

import requests
from requests.auth import HTTPBasicAuth
from requests.sessions import Session


class Station(TypedDict):
    """station information"""

    uuid: str
    name: str
    callLetters: str
    role: str
    internalId: int
    groups: List[str]


class StationOrganization(TypedDict):
    """organization list information"""

    uuid: str
    name: str
    stationUUIDCollection: List[str]


class StationPlaylistItem(TypedDict):
    """station playlist item"""

    playPosition: str
    duration: str
    durationToSegue: str
    uuid: str
    type: str
    assetType: str
    chainType: str
    artist: str
    title: str
    statusCode: str
    assetTypeName: str
    editCode: str
    airTime: str


class StationStatus(TypedDict):
    """Station Onair Status Payload"""

    onAirStatusLogEvents: List[StationPlaylistItem]
    mode: str
    status: str


class Fields(TypedDict):
    """station document fields"""

    s_uuid: str
    s_name: str
    s_callLetters: str
    s_role: str
    i_internalId: int
    as_groups: List[str]
    s_mode: str
    s_status: str
    s_current_title: NotRequired[str]
    s_current_status_code: NotRequired[str]
    s_current_artist: NotRequired[str]
    s_current_chainType: NotRequired[str]
    s_current_assetType: NotRequired[str]


class Document(TypedDict):
    """inSITE poller document"""

    host: str
    name: str
    fields: Fields


class ZettaParams(TypedDict):
    """Zetta Poller initialization parameters"""

    host: NotRequired[str]
    port: NotRequired[int]
    http: NotRequired[str]
    apikey: str
    username: str
    password: str


# Number of stations in a polling group
POLLNG_GROUPS: int = 25


class Zetta:
    """RCS Zetta Simple API Station Collector"""

    def __init__(self, **kwargs: Unpack[ZettaParams]) -> None:
        self.http: str = "http"
        self.host: str = "127.0.0.1"
        self.port: int = 3139

        self.apiversion: str = "1.0"
        self.apikey: str = ""
        self.username: str = ""
        self.password: str = ""

        self.headers: Dict[str, str] = {}
        self.auth: HTTPBasicAuth | None = None

        self.station_list_url: str = ""
        self.organization_list_url: str = ""
        self.station_status_url: str = ""

        self.station_store: Dict[str, Station] = {}

        for key, value in kwargs.items():
            value = str(value)

            if "host" in key and value:
                self.host = value

            if "apikey" in key and value:
                self.apikey = value

            if "username" in key and value:
                self.username = value

            if "password" in key and value:
                self.password = value

            if "http" in key and value:
                self.http = value

            if "port" in key and value:
                self.port = int(value)

        if self.apikey == "" or self.username == "" or self.password == "":
            print("Error missing parameters: APIKEY, Zetta account (username password)")
            sys.exit()

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "APIKEY": self.apikey,
        }
        self.auth = HTTPBasicAuth(self.username, self.password)

        # Get the station list and organization list on init
        #
        with requests.Session() as http_session:
            self.station_store.update(self.collect_stations(http_session))

            organization_lists = self.collect_org_groups(http_session)

            for _, station in self.station_store.items():
                for name, station_list in organization_lists.items():

                    if station["uuid"] in station_list:
                        station["groups"].append(name)

        print(len(self.station_store.keys()), "Total Stations")

    def collect_stations(self, http_session: Session) -> Dict[str, Station]:
        """fetches the full list of stations"""

        class APIStationResponse(TypedDict):
            """API Response for Station List"""

            dataObject: List[Station]
            responseType: str
            syncCounter: int

        # http://localhost:3139/1.0/Station/list
        station_list_url = (
            f"{self.http}://{self.host}:{self.port}/{self.apiversion}/Station/list"
        )

        station_dict: Dict[str, Station] = {}

        try:
            resp: APIStationResponse = http_session.get(
                url=station_list_url,
                headers=self.headers,
                auth=self.auth,
                timeout=5.0,
            ).json()

            if not resp.get("responseType") or resp["responseType"] != "success":
                raise ValueError(
                    "responseType",
                    resp.get("responseType", "no responseType key exists"),
                )

            # sample station:
            #   {
            #       "uuid": "628b0a00-734f-4804-b8b4-3b05ad207ba9",
            #       "name": "Classic Rock Party-PMCRKP",
            #       "callLetters": "PMCRKP",
            #       "role": "station",
            #       "internalId": 3
            #   }
            for station in resp["dataObject"]:
                station.update({"groups": []})
                station_dict.update({station["uuid"]: station})

        except Exception as error:  # pylint: disable=broad-exception-caught
            print(error)

        return station_dict

    def collect_org_groups(self, http_session: Session) -> Dict[str, List[str]]:
        """fetches the organization groupings"""

        class APIOrganizationResponse(TypedDict):
            """API Response for Organization List"""

            dataObject: List[StationOrganization]
            responseType: str
            syncCounter: int

        # http://localhost:3139/1.0/Organization/list
        organization_list_url = (
            f"{self.http}://{self.host}:{self.port}/{self.apiversion}/Organization/list"
        )

        org_dict: Dict[str, List[str]] = {}

        try:
            resp: APIOrganizationResponse = http_session.get(
                url=organization_list_url,
                headers=self.headers,
                auth=self.auth,
                timeout=5.0,
            ).json()

            if not resp.get("responseType") or resp["responseType"] != "success":
                raise ValueError(
                    "responseType",
                    resp.get("responseType", "no responseType key exists"),
                )

            # sample organization item:
            #   {
            #       "uuid": "41f82e29-e80b-4a39-a008-c7a5c080e414",
            #       "name": "60s",
            #       "stationUUIDCollection": [
            #            "628b0a00-734f-4804-b8b4-3b05ad207ba9",
            #            "c3b74f82-9de8-457d-a228-7894eef52aab",
            #       ],
            #   }
            for org in resp["dataObject"]:
                # check if the stationUUIDCollection key exists
                if col := org.get("stationUUIDCollection"):
                    org_dict.update({org["name"]: col})

        except Exception as error:  # pylint: disable=broad-exception-caught
            print(error)

        return org_dict

    def collect_station_status(
        self, uuid: str, http_session: Session
    ) -> StationStatus | None:
        """fetches a station on air status information"""

        class APIStationSatusResponse(TypedDict):
            """API Response for the Onair Station Status"""

            dataObject: StationStatus
            responseType: str
            syncCounter: int

        # http://localhost:3139/1.0/StationScheduleLog/OnAir/Status/1131EB8B-CD09-47A1-A14C-AEBB79AA97F8
        station_status_url = f"{self.http}://{self.host}:{self.port}/{self.apiversion}/StationScheduleLog/OnAir/Status/{uuid}"

        try:
            resp: APIStationSatusResponse = http_session.get(
                url=station_status_url,
                headers=self.headers,
                auth=self.auth,
                timeout=5.0,
            ).json()

            if not resp.get("responseType") or resp["responseType"] != "success":
                raise ValueError(
                    "responseType",
                    resp.get("responseType", "no responseType key exists"),
                )

            # sample station status item:
            #   {
            #       "onAirStatusLogEvents": [StationPlaylistItem, StationPlaylistItem...],
            #       "mode": "auto",
            #       "status": "onAir"
            #   }
            return resp["dataObject"]

        except Exception as error:  # pylint: disable=broad-exception-caught
            print(error)

        return None

    def station_process(
        self, stations: Tuple[str], collection: Dict[str, StationStatus]
    ) -> None:
        """threaded function that generates a get request for each station in the stations tuple
        then merges the response into the collection
        """

        with requests.Session() as http_session:

            for uuid in stations:
                # item might be a None
                if not uuid:
                    continue

                if resp := self.collect_station_status(uuid, http_session):
                    collection.update({uuid: resp})

    def collect(self) -> List[Document]:
        """Get the station on air status for each station in the store"""

        # seperate station uuids into collection groups of 25
        groups = list(
            zip_longest(
                *[iter([station for station in self.station_store])] * POLLNG_GROUPS
            )
        )

        collection: Dict[str, StationStatus] = {}

        threads = [
            Thread(
                target=self.station_process,
                args=(
                    group,
                    collection,
                ),
            )
            for group in groups
        ]

        for x in threads:
            x.start()

        for y in threads:
            y.join()

        documents: List[Document] = []

        for k, v in collection.items():
            fields: Fields = {
                "s_uuid": k,
                "s_name": self.station_store[k]["name"],
                "s_callLetters": self.station_store[k].get("callLetters"),
                "s_role": self.station_store[k].get("role"),
                "i_internalId": self.station_store[k].get("internalId"),
                "as_groups": self.station_store[k]["groups"],
                "s_mode": v.get("mode"),
                "s_status": v.get("status"),
            }

            if v.get("onAirStatusLogEvents") and len(v["onAirStatusLogEvents"]) > 0:
                log_event: StationPlaylistItem = v["onAirStatusLogEvents"][0]

                fields["s_current_artist"] = log_event.get("artist")
                fields["s_current_assetType"] = log_event.get("assetType")
                fields["s_current_chainType"] = log_event.get("chainType")
                fields["s_current_status_code"] = log_event.get("statusCode")
                fields["s_current_title"] = log_event.get("title")

            document: Document = {"fields": fields, "host": self.host, "name": "zetta"}

            documents.append(document)

        print(len(documents))
        print(documents[0])

        return documents


def main() -> None:
    """main function called when executed from the command line"""

    params: ZettaParams = {
        "port": 3000,
        "apikey": "1234",
        "username": "admin",
        "password": "password",
    }

    zetta = Zetta(**params)

    for document in zetta.collect():
        print(
            document["fields"]["s_name"],
            "--",
            document["fields"]["s_callLetters"],
            "---",
            document["fields"]["s_status"],
            "---",
            document["fields"].get("s_current_status_code"),
        )


if __name__ == "__main__":
    main()
