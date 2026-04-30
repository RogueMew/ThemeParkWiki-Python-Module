import json as jason
import requests as web

import csv
import asyncio
import datetime
import os
import pandas
import pytz
import pymongo
import math

from typing import Iterable, Literal, TypeVar, Generic
from enum import StrEnum
from geopy.distance import geodesic

from fuzzywuzzy import process

class Status(StrEnum):
    Operating = "OPERATING"
    Down = "DOWN"
    Closed = "CLOSED"
    Refurbishment = "REFURBISHMENT"

class URL(StrEnum):
    liveData = "https://api.themeparks.wiki/v1/entity/{}/live"
    entityChildrenData = "https://api.themeparks.wiki/v1/entity/{}/children"
    entityData = "https://api.themeparks.wiki/v1/entity/{}"
    schedule = "https://api.themeparks.wiki/v1/entity/{}/schedule" 

class ActivityTypes(StrEnum):
    Attraction = "ATTRACTION"
    Show = "SHOW"
    Restaurant = "RESTAURANT"

class ParkSlugs(StrEnum):
    magic = "magickingdompark"
    epcot = "epcot"
    hollywood = "disneyshollywoodstudios"
    dak = "disneysanimalkingdomthemepark"

class UtilFuncs:
    @staticmethod
    def WifiCheck():
        """Does a WIFI connection check and returns if connected to the internet and returns a bool

        Returns:
            bool: True if connected or False if not connected
        """
        try:
            web.get("https://www.google.com", timeout=5)
            return True
        except (web.ConnectTimeout, web.ConnectionError):
            return False
    
    @staticmethod
    def printJson(json):
        """Quick DUmb debug for Jason because im to lazy to write this out everytime

        Args:
            json (Any): The json I need to debug
        """
        print(jason.dumps(json, indent=4))

    @staticmethod    
    def hasHeaders(filePath: str):
        """A rudimentary check if the file has a header but not checking if its CSV, will need to update to do that. Will I at some point yes, but not important right now.

        Args:
            filePath (str): the filepath to the CSV file

        Returns:
            bool: True if it has data(AKA headers) and False if not or does not exist
        """
        if not os.path.exists(filePath):
            return False
        
        with open(filePath, "r") as f:
            return bool(f.readline().strip())

    @staticmethod
    def correctHeaders(filePath):
        ...

class LongitudeLatitude:
    longitude: float
    latitude: float
    
    def __init__(self, longitude: float, latitude: float) -> None:
        """Creates a new LongitudeLatitude Item

        Args:
            longitude (float): The longitude of a point
            latitude (float): The latitude of a point
        """
        self.latitude = latitude
        self.longitude = longitude

    @property
    def toTuple(self):
        """Returns the data in a tuple type

        Returns:
            tuple[float, float]: Tuple where the first object is the longitude and the second object is the latitude
        """
        return (self.longitude, self.latitude)
    
    @property
    def toDict(self):
        """Returns the data in a dictionary

        Returns:
            dict[str, float]: Returns a two item dictionary where the keys are longitude or latitude and the data is likewise
        """
        return {
            "longitude" : self.longitude,
            "latitude" : self.latitude
            }
    
    def distanceBetween(self, location: "LongitudeLatitude"):
        """Returns the distance between the current LongitudeLatitude object and another one

        Args:
            location (LongitudeLatitude): The other point that you want to calculate the distance to

        Returns:
            _type_: _description_
        """
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
        """Creates a new _BaseEntity object, this is the backbone for all the other objects such as Attractions, Shows, and Restaurants

        Args:
            name (str): The name of the Entity
            location (LongitudeLatitude): The location of the Entity in Longitude and Latitude as the custom ObjectType LongitudeLatitude
            id (str): The Id of the attraction based off the ThemeparkWiki API
        """
        self.name = name
        self.location = location
        self.waitTime = None
        self.id = id
        self.currentStatus = None
        self.properties = ["name", "waitTime", "currentStatus", "date", "time"]

    def __repr__(self) -> str:
        return f"{type(self)}({self.name}, {self.location}, waitTime:{self.waitTime}, status: {self.currentStatus})"

    @property
    def dict(self):
        """Sorts the data into a nice and neat dictionary item

        Returns:
            dict[str, Any]: All the data that is kind of important stored as a dict with str keys and data to match
        """
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
        """Creates a new Attraction Item based off the _BaseEntity Backbone

        Args:
            name (str): Name of the Attraction
            location (LongitudeLatitude): The Longitude and Latitude of an attraction in the custom datatype of LongitudeLatitude
            id (str): Id given to hte attraction by ThemeparksWiki API
        """
        super().__init__(name, location, id)
        self.isRide = False
        self.distanceFromUser = None
        self.properties.append("isRide")
    
    def __repr__    (self) -> str:
        return f"Attraction({self.name}, isRide: {self.isRide}, {self.location}. {self.waitTime})"
    
    @property
    def dict(self):
        """Returns the attraction as a dictionary object

        Returns:
            dict[str, Any]: All the data like stated before, nothing to new
        """
        dictionary = super().dict 
        dictionary.update({"isRide" : self.isRide})
        return dictionary
        
class Restaurant(_BaseEntity):
    def __init__(self, name: str, location: LongitudeLatitude, id: str) -> None:
        """I mean same as Attractions but with a different name and not much added

        Args:
            name (str): Name of the Restaurant
            location (LongitudeLatitude): Same Gimmick
            id (str): Id Assigned by ThemeParkWiki API
        """
        super().__init__(name, location, id)

class Show(_BaseEntity):
    #nextPerformance: int | None
    isMeetGreet: bool

    def __init__(self, name: str, location: LongitudeLatitude, id) -> None:
        """Creates a new... Im done retyping the same thing; please see attractions for the documentation.(I know its just me in the future looking at this because I forgot how everything worked)

        Args:
            name (str): Same Deal
            location (LongitudeLatitude): Same Deal
            id (int): Same Deal
        """
        super().__init__(name, location, id)
        self.properties.append("isMeetGreet")

    @property
    def dict(self):
        dictionary = super().dict
        dictionary.update({"isMeetGreet" : self.isMeetGreet})
        return dictionary

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
    
    def toDict(self):
        dictionary = {}
        
        for attraction in self:
            attractionDict = {
                "name" : attraction.name,
                "waitTime" : attraction.waitTime,
                "currentStatus" : attraction.currentStatus
            }
            
            if self.activityType is Attraction:
                attractionDict.update({"isRide" : attraction.isRide}) # type: ignore

            if self.activityType is Show:
                attractionDict.update({"isMeetGreet" : attraction.isMeetGreet}) # type: ignore

            dictionary.update({f"{attraction.name}" : attractionDict})

        return dictionary

    def archiveToCSV(self, parkName: str, lastTimeCheck=datetime.datetime.now(), filePath=None):

        typeDict = {
            Attraction : Attraction("",LongitudeLatitude(0,0),""),
            Show : Show("",LongitudeLatitude(0,0),""),
            Restaurant : Restaurant("",LongitudeLatitude(0,0),""),
        }

        typeDictStr = {
            Attraction : "attractions",
            Show : "shows",
            Restaurant : "restaurant"
        }

        if filePath is None:
            filePath = f"{parkName}_{typeDictStr[self.activityType]}.csv"

        with open(filePath, mode="a", newline="") as csvFile:
            writer = csv.DictWriter(csvFile, typeDict[self.activityType].properties)

            if UtilFuncs.hasHeaders(f"{parkName}_{typeDictStr[self.activityType]}.csv"):
                writer.writeheader()
            
            activities = self.toDict()
            
            for activity in activities:
                row = {
                            "name" : activities[activity]["name"],
                            "waitTime" : activities[activity]["waitTime"],
                            "currentStatus" : activities[activity]["currentStatus"],
                            "date" : lastTimeCheck.strftime("%m-%d-%Y"),
                            "time" : lastTimeCheck.strftime("%H:%M")
                            }
                
                if self.activityType is Attraction:
                    row.update({"isRide" : activities[activity]["isRide"]})
                
                if self.activityType is Show:
                    row.update({"isMeetGreet" : activities[activity]["isMeetGreet"]})
                writer.writerow(row)

class Park:
    name: str
    slug: str
    location: LongitudeLatitude
    lastTimeCheck: datetime.datetime
    attractions: ActivityList[Attraction]
    shows: ActivityList[Show]
    restaurants: ActivityList[Restaurant]
    waitBetweenTimeChecks: int
    openTime : datetime.datetime
    closeTime : datetime.datetime
    timeZone : str

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

    def _getParkSchedule(self):
        response = web.get(URL.schedule.format(self.slug)).json()
        today = [day for day in response["schedule"] if day["date"] == datetime.datetime.now(pytz.timezone(self.timeZone)).strftime("%Y-%m-%d") and "description" not in day]
        today = today[0]
        self.openTime = datetime.datetime.fromisoformat(today["openingTime"])
        self.closeTime = datetime.datetime.fromisoformat(today["closingTime"])

    def _additionalInfoAdd(self):
        response = web.get(URL.liveData.format(self.slug)).json()
        self._checkRideGreet(response)
        self._getWaitTimes(response)
        self._getStatus(response)
        self._getParkSchedule()

    def __init__(self, name:str, slug:str, timeZone="America/New_York") -> None:
        self.name = name
        self.slug = slug
        self.timeZone = timeZone
        self.attractions = ActivityList([], Attraction)
        self.shows = ActivityList([], Show)
        self.restaurants = ActivityList([], Restaurant)

        self.waitBetweenTimeChecks = 300

        if not UtilFuncs.WifiCheck():
            raise ConnectionError("Cannot connect to the wifi")
        
        asyncio.run(main=self._getParkActivitiesData())
        self._additionalInfoAdd()

        self.lastTimeCheck = datetime.datetime.now(pytz.timezone(self.timeZone))

    def isParkOpen(self):
        currentTime = datetime.datetime.now(pytz.timezone(self.timeZone))
        
        if self.openTime <= currentTime <= self.closeTime:
            return True
        else:
            return False

    def checkWaitTimes(self):
        timeBetween = datetime.datetime.now(pytz.timezone(self.timeZone)) - self.lastTimeCheck
        if timeBetween.seconds < self.waitBetweenTimeChecks:
            raise RuntimeError(f"Time Was Checked {timeBetween.seconds} seconds ago")
        
        response = web.get(URL.liveData.format(self.slug))
        self._getWaitTimes(response.json())     

    def toDict(self):
         return {
            "attractions" : [attraction.dict for attraction in self.attractions],
            "shows" : [show.dict for show in self.shows],
            "restaurants" : [restaurant.dict for restaurant in self.restaurants] 
        }

class dataCleanup:
    filePath: str
    dataFrame: pandas.DataFrame
    latestVersion: bool

    def __init__(self, filePath:str, latestVersion:bool =True, printChange: bool=True) -> None:
        self.filePath = filePath
        self.latestVersion = latestVersion
        if not os.path.exists(self.filePath):
            raise FileExistsError(f"The file {os.path.split(self.filePath)[1]} does not exist at the file path")
        elif not UtilFuncs.hasHeaders(self.filePath):
            raise ValueError("This File has no Data in it")
        
        self.dataFrame = pandas.read_csv(self.filePath)


    def _findSimilars(self, listofData: list) :
        replaceDict = {}
        i = 0
        while len(listofData) > 1:
            query = listofData.pop(0)
            fuzzyCheck = process.extractOne(query, listofData)
            if fuzzyCheck == None:
                continue
            
            if fuzzyCheck[1] > 90:
                replaceDict.update({query : fuzzyCheck[0]})
        return replaceDict
    
    def _replaceData(self, replaceDict: dict, originalList: list):
        for item in replaceDict:
            if originalList[::-1].index(item) < originalList[::-1].index(replaceDict[item]) and self.latestVersion:
                self.dataFrame.replace(replaceDict[item], item, inplace=True)
                print(f"Replacing {replaceDict[item]} with {item}")
            else:
                self.dataFrame.replace(item, replaceDict[item], inplace=True)          
                print(f"Replacing {item} with {replaceDict[item]}")
        
    def standardizeNames(self):
        originalList = self.dataFrame["name"].to_list()
        nameList = list(set(originalList))
        self._replaceData(self._findSimilars(nameList.copy()), originalList)

    def export(self, differentFilePath=None):
        if differentFilePath is None:
            differentFilePath = self.filePath
        
        self.dataFrame.to_csv(differentFilePath, index=False)