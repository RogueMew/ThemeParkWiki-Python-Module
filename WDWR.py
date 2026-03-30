import json as jason
import requests as web

import csv
import asyncio
import datetime
import os

from typing import Iterable, Literal, TypeVar, Generic
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

class ParkSlugs(StrEnum):
    Magic = "magickingdompark"
    epcot = "epcot"
    hollywood = "disneyshollywoodstudios"
    DAK = "disneysanimalkingdomthemepark"

class UtilFuncs:
    def WifiCheck(self):
        try:
            web.get("https://www.google.com", timeout=5)
            return True
        except (web.ConnectTimeout, web.ConnectionError):
            return False
        
    def printJson(self, json):
        print(jason.dumps(json, indent=4))
        
    def hasHeaders(self, filePath):
        if not os.path.exists(filePath):
            return False
        
        if not os.path.getsize(filePath) == 0:
            return False
        
        return True

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

    properties = []
    
    def __init__(self, name: str, location: LongitudeLatitude, id:str) -> None:
        self.name = name
        self.location = location
        self.waitTime = None
        self.id = id
        self.currentStatus = None
        self.properties = ["name", "id", "waitTime", "currentStatus", "weekday", "hourOfDay", "month"]

    def __repr__(self) -> str:
        return f"{type(self)}({self.name}, {self.location}, waitTime:{self.waitTime}, status: {self.currentStatus})"

    @property
    def dict(self):
        return {
            "name" : self.name,
            "location" : self.location.toDict,
            "waitTime" : self.waitTime,
            "id" : self.id,
            "currentStatus" : self.currentStatus
        }

class Attraction(_BaseEntity):
    #distanceFromUser: float | None
    isRide: bool

    def __init__(self, name: str, location: LongitudeLatitude, id:str) -> None:
        super().__init__(name, location, id)
        self.isRide = False
        self.distanceFromUser = None
        self.properties.append("isRide")
    
    def __repr__    (self) -> str:
        return f"Attraction({self.name}, isRide: {self.isRide}, {self.location}. {self.waitTime})"
    
    def toDict(self):
        dictionary = super().dict 
        dictionary.update({"isRide" : self.isRide})
        return dictionary
        
class Restaurant(_BaseEntity):
    def __init__(self, name: str, location: LongitudeLatitude, id: str) -> None:
        super().__init__(name, location, id)

class Show(_BaseEntity):
    #nextPerformance: int | None
    isMeetGreet: bool

    def __init__(self, name: str, location: LongitudeLatitude, id) -> None:
        super().__init__(name, location, id)
        self.properties.append("isMeetGreet")

    def toDict(self):
        return super().dict.update({"nextPerformance" : self.nextPerformance, "isMeetGreet" : self.isMeetGreet})

T = TypeVar("T", bound=_BaseEntity)

class ActivityList(list[T], Generic[T]):
    activityType: type[T]

    _sortTypes = Literal["alpha", "waitTime", "distance", "currentStatus"]
    
    def __init__(self, iterable: Iterable, activityType: type[T]) -> None:
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
    def rides(self) -> list[Attraction]:
        if self.activityType is not Attraction:
            raise ValueError(f"ride cannot be called on type {self.activityType}")
        return [ride for ride in self if ride.isRide] # type: ignore[attr-defined]

    @property
    def meetGreet(self) -> list[Show]:
        if self.activityType is not Show:
            raise ValueError(f"Meet and Greets cannot be called on type {self.activityType}")
        return [meetGreet for meetGreet in self if meetGreet.isMeetGreet] # type: ignore[attr-defined]
    
    @property
    def names(self) -> list[str]:
        return [item.name for item in self]
         
class Park:
    name: str
    slug: str
    location: LongitudeLatitude
    lastTimeCheck: datetime.datetime
    attractions: ActivityList[Attraction]
    shows: ActivityList[Show]
    restaurants: ActivityList[Restaurant]
    waitBetweenTimeChecks: int

    def _categorizeActivites(self, entityType: ActivityTypes, parkData):
        return [activity for activity in parkData if activity["entityType"] == entityType]

    async def _getParkActivitiesData(self):
        response = web.get(URL.entityChildrenData.format(self.slug)).json()
        
        self._parseData(ActivityTypes.Attraction, self._categorizeActivites(ActivityTypes.Attraction, response["children"]))
        self._parseData(ActivityTypes.Show, self._categorizeActivites(ActivityTypes.Show, response["children"]))
        self._parseData(ActivityTypes.Restaurant, self._categorizeActivites(ActivityTypes.Restaurant, response["children"]))

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
            if "STANDBY" not in activity["queue"]:
                continue
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
        if timeBetween.seconds < self.waitBetweenTimeChecks:
            raise RuntimeError(f"Time Was Checked {timeBetween.seconds} seconds ago")
        
        response = web.get(URL.liveData.format(self.slug))
        self._getWaitTimes(response)     

    def toDict(self):
         return {
            "attractions" : [attraction.dict for attraction in self.attractions],
            "shows" : [show.dict for show in self.shows],
            "restaurants" : [restaurant.dict for restaurant in self.restaurants] 
        }
    
    def toCSV(self):

        activityList = {
            "Attractions" : self.attractions,
            "Shows" : self.shows,
            "Restaurants" : self.restaurants
        }

        activitiesDict = self.toDict()

        try:
            self.checkWaitTimes()
        except RuntimeError:
            print("Time was checked within the past 5 minutes")

        for activity in activityList:
            activity = activityList[activity]
            
            if activity is self.attractions:


                with open(f"{self.name}_attraction.csv", "a", newline="") as f:
                    writer = csv.DictWriter(f, Attraction("", LongitudeLatitude(1,1), "").properties)
                    
                    if UtilFuncs().hasHeaders(f"{self.name}_attraction.csv"):
                        writer.writeheader()
                    
                    for attraction in activitiesDict["attractions"]:
                        row = {
                            "name" : attraction["name"],
                            "id" : attraction["id"],
                            "waitTime" : attraction["waitTime"],
                            "currentStatus" : attraction["currentStatus"],
                            "weekday" : self.lastTimeCheck.weekday(),
                            "hourOfDay" : self.lastTimeCheck.hour,
                            "month" : self.lastTimeCheck.month 
                            }
                        
                        writer.writerow(row)
            
            elif activity is self.shows:
                with open(f"{self.name}_show.csv", "a", newline="") as f:
                    writer = csv.DictWriter(f, Show("", LongitudeLatitude(1,1), "").properties)

                    if UtilFuncs().hasHeaders(f"{self.name}_show.csv"):
                        writer.writeheader()

                    for show in activitiesDict["shows"]:
                        row = {
                            "name" : show["name"],
                            "id" : show["id"],
                            "waitTime" : show["waitTime"],
                            "currentStatus" : show["currentStatus"],
                            "weekday" : self.lastTimeCheck.weekday(),
                            "hourOfDay" : self.lastTimeCheck.hour,
                            "month" : self.lastTimeCheck.month
                        }
                        writer.writerow(row)
            
            elif activity is self.restaurants:
                with open(f"{self.name}_restaurant.csv", "a", newline="") as f:
                    writer = csv.DictWriter(f, Restaurant("", LongitudeLatitude(1,1), "").properties)
                    if UtilFuncs().hasHeaders(f"{self.name}_restaurant.csv"):
                        writer.writeheader()
                    
                    for restaurant in activitiesDict["restaurants"]:
                        row = {
                            "name" : restaurant["name"],
                            "id" : restaurant["id"],
                            "waitTime" : restaurant["waitTime"],
                            "currentStatus" : restaurant["currentStatus"],
                            "weekday" : self.lastTimeCheck.weekday(),
                            "hourOfDay" : self.lastTimeCheck.hour,
                            "month" : self.lastTimeCheck.month
                        }
                        writer.writerow(row)

class MachineLearning:
    ...