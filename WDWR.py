import json as jason
import requests as web

import asyncio
import datetime

from typing import Iterable, Literal
from enum import StrEnum
from geopy.distance import geodesic

class Status(StrEnum):
    Operating = "OPERATING"
    Down = "DOWN"
    Closed = "CLOSED"
    Refurbishment = "REFURBISHMENT"

class URL(StrEnum):
    liveData = "https://api.themeparks.wiki/v1/entity/{}/live"
    entityChildrenData = "https://api.themeparks.wiki/v1/entity/{}/children"
    entityData = "https://api.themeparks.wiki/v1/entity/{}"

class ActivityTypes(StrEnum):
    Attraction = "ATTRACTION"
    Show = "SHOW"
    Restaurant = "RESTAURANT"

class UtilFuncs:
    def __init__(self) -> None:
        pass

    def WifiCheck(self):
        try:
            web.get("https://www.google.com", timeout=5)
            return True
        except (web.ConnectTimeout, web.ConnectionError):
            return False
        
class LongitudeLatitude:
    longitude: float
    latitude: float
    
    def __init__(self, longitude: float, latitude: float) -> None:
        self.latitude = latitude
        self.longitude = longitude

    @property
    def toTuple(self):
        return (self.longitude, self.latitude)
    
    @property
    def toDict(self):
        return {
            "longitude" : self.longitude,
            "latitude" : self.latitude
            }
    
    def distanceBetween(self, location: LongitudeLatitude):
        return geodesic(self.toTuple, location.toTuple)
    
    def __str__(self) -> str:
        return f"Longitude is {self.longitude}; Latitude is {self.latitude}"

    def __repr__(self) -> str:
        return f"LongitudeLatitude({self.longitude}, {self.latitude})"

class _BaseEntity:
    name: str
    location: LongitudeLatitude
    currentStatus: Status | None
    waitTime: int | None
    id: str
    
    def __init__(self, name: str, location: LongitudeLatitude, id:str) -> None:
        self.name = name
        self.location = location
        self.waitTime = None
        self.id = id
        self.currentStatus = None

    def __repr__(self) -> str:
        return f"{type(self)}({self.name}, {self.location}, waitTime:{self.waitTime}, status: {self.currentStatus})"

class Attraction(_BaseEntity):
    distanceFromUser: float | None
    isRide: bool

    def __init__(self, name: str, location: LongitudeLatitude, id:str) -> None:
        super().__init__(name, location, id)
        self.isRide = False
        self.distanceFromUser = None
    
    def __repr__    (self) -> str:
        return f"Attraction({self.name}, isRide: {self.isRide}, {self.location}. {self.waitTime})"
    
class Restaurant(_BaseEntity):
    def __init__(self, name: str, location: LongitudeLatitude, id: str) -> None:
        super().__init__(name, location, id)

class Show(_BaseEntity):
    nextPerformance: int | None
    isMeetGreet: bool

    def __init__(self, name: str, location: LongitudeLatitude, id) -> None:
        super().__init__(name, location, id)

class ActivityList(list):
    activityType: type

    _sortTypes = Literal["alpha", "waitTime", "distance", "currentStatus"]
    
    def __init__(self, iterable: Iterable, activityType) -> None:
        super().__init__(iterable)
        self.activityType = activityType

    def customSort(self, sortType:_sortTypes = "alpha", currentLocation: LongitudeLatitude | None = None, reverse: bool = False ):
        if self.activityType not in (Attraction, Show) and sortType == "waitTime":
            raise ValueError(f"{self.activityType} is not able to be sorted by {sortType}")
        
        if sortType == "distance" and currentLocation is None:
            raise ValueError(f"Need the current location in order to proceed")

        sortFuncs = {
            "alpha" : lambda activity: activity.name,
            "waitTime" : lambda activity: (activity.waitTime is None, activity.waitTime or float("inf")),
            "distance" : lambda activity: activity.location.distanceBetween(currentLocation),
            "currentStatus" :lambda activity: (activity.currentStatus is None, activity.currentStatus or "None Stated")
        }
        
        self.sort(key=sortFuncs[sortType], reverse=reverse)

    @property
    def rides(self):
        if self.activityType is not Attraction:
            raise ValueError(f"ride cannot be called on type {self.activityType}")
        return [ride for ride in self if ride.isRide]

    @property
    def meetGreet(self):
        if self.activityType is not Show:
            raise ValueError(f"Meet and Greets cannot be called on type {self.activityType}")
        return [meetGreet for meetGreet in self if meetGreet.isMeetGreet]
    
    @property
    def names(self):
        return [item.name for item in self]
         
class Park:
    name: str
    slug: str
    location: LongitudeLatitude
    lastTimeCheck: datetime.datetime
    attractions: ActivityList
    shows: ActivityList
    restaurants: ActivityList
    waitBetweenTimeChecks: int

    async def _catagorizeActivites(self, entityType: ActivityTypes, parkData):
        return [activity for activity in parkData if activity["entityType"] == entityType]

    async def _getParkActivitiesData(self):
        response = web.get(URL.entityChildrenData.format(self.slug)).json()
        tasks = asyncio.gather(self._catagorizeActivites(ActivityTypes.Attraction, response["children"]),
                               self._catagorizeActivites(ActivityTypes.Show, response["children"]),
                               self._catagorizeActivites(ActivityTypes.Restaurant, response["children"])
                               )
        
        catagorizedActivities: list[list[dict]] = await tasks #type: ignore 
        
        self._parseData(ActivityTypes.Attraction, catagorizedActivities[0])
        self._parseData(ActivityTypes.Show, catagorizedActivities[1])
        self._parseData(ActivityTypes.Restaurant, catagorizedActivities[2])

    def _parseData(self, activityType: ActivityTypes, data: list):
        def makeAttraction(item):
                return Attraction(item["name"], LongitudeLatitude(item["location"]["longitude"], item["location"]["latitude"]), item["id"])
        
        def makeShow(item):
                return Show(item["name"], LongitudeLatitude(item["location"]["longitude"], item["location"]["latitude"]), item["id"])

        def makeRestaurant(item):
                return Restaurant(item["name"], LongitudeLatitude(item["location"]["longitude"], item["location"]["latitude"]), item["id"])

        if activityType == ActivityTypes.Attraction:
            self.attractions = ActivityList(map(makeAttraction, data), Attraction)

        if activityType == ActivityTypes.Show:
            self.shows = ActivityList(map(makeShow, data), Show)
        
        if activityType == ActivityTypes.Restaurant:
            self.restaurants = ActivityList(map(makeRestaurant, data), Restaurant)

    def _checkRideGreet(self, response):
        parsedResponse = [ride["name"] for ride in response["liveData"] if "queue" in ride]


        def addIsRide(ride):
            ride.isRide = ride.name in parsedResponse

        def addIsMeetGreet(show):
            show.isMeetGreet = show.name in parsedResponse

        for ride in self.attractions:
            addIsRide(ride)

        for ride in self.shows:
            addIsMeetGreet(ride)

    def _getWaitTimes(self, response):
        parsedResponse = [activity for activity in response["liveData"] if "queue" in activity]
        rideNames = [ride.name for ride in self.attractions.rides]
        greetNames = [meet.name for meet in self.shows.meetGreet]
        restaurantNames = [restaurant.name for restaurant in self.restaurants]

        for activity in parsedResponse:
            if activity["name"] in rideNames:
                self.attractions.rides[rideNames.index(activity["name"])].waitTime = activity["queue"]["STANDBY"]["waitTime"]  
            elif activity["name"] in greetNames:
                self.shows.meetGreet[greetNames.index(activity["name"])].waitTime = activity["queue"]["STANDBY"]["waitTime"]
            elif activity["name"] in restaurantNames:
                self.restaurants[restaurantNames.index(activity["name"])].waitTime = activity["queue"]["STANDBY"]["waitTime"]
    
    def _getStatus(self, response):
        responseNames = [activity for activity in response["liveData"]]

        for activity in responseNames:
            if activity["entityType"] == ActivityTypes.Attraction:
                self.attractions[self.attractions.names.index(activity["name"])].currentStatus = activity["status"]
            elif activity["entityType"] == ActivityTypes.Restaurant:
                self.restaurants[self.restaurants.names.index(activity["name"])].currentStatus = activity["status"]
            elif activity["entityType"] == ActivityTypes.Show:
                self.shows[self.shows.names.index(activity["name"])].currentStatus = activity["status"]

    def _additionalInfoAdd(self):
        response = web.get(URL.liveData.format(self.slug)).json()
        self._checkRideGreet(response)
        self._getWaitTimes(response)
        self._getStatus(response)
               
    def __init__(self, name:str, slug:str) -> None:
        self.name = name
        self.slug = slug

        self.attractions = ActivityList([], Attraction)
        self.shows = ActivityList([], Show)
        self.restaurants = ActivityList([], Restaurant)

        self.waitBetweenTimeChecks = 300

        if not UtilFuncs().WifiCheck():
            raise ConnectionError("Cannot connect to the wifi")
        
        asyncio.run(main=self._getParkActivitiesData())
        self._additionalInfoAdd()

        self.lastTimeCheck = datetime.datetime.now()

    def checkWaitTimes(self):
        timeBetween = datetime.datetime.now() - self.lastTimeCheck
        if timeBetween.seconds == 0:
            raise RuntimeError(f"Time Was Checked {timeBetween.seconds} seconds ago")
        
        response = web.get(URL.liveData.format(self.slug))
        self._getWaitTimes(response)      