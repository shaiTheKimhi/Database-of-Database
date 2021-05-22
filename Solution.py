from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

connection = Connector.DBConnector()

def createTables():
    pass


def clearTables():
    pass


def dropTables():
    pass


def addQuery(query: Query) -> ReturnValue:
    global(connection)
    
    if type(query) is not Query:
        return ReturnValue.BAD_PARAMS
    id = query.getQueryID()
    purpose = query.getPurpose()
    size = query.getSize()
    if (type(id) is not int) or (type(purpose) is not str) or (type(size) is not int):
        return ReturnValue.BAD_PARAMS
    try:
        connection.execute(f"INSERT INTO Query (Qid, Purpose, Size) VALUES ({id}, {purpose}, {size})")
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except:
        return ReturnValue.ERROR
        
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    global(connection)
    
    values = connection.execute(f'SELECT Qid,Purpose,Size FROM Query WHERE Qid={queryID}')[1]
    if values is None:
        return Query.badQuery()
    for val in values:
        if val is None:
            return Query.badQuery()
    return Query(tuple(values))


def deleteQuery(query: Query) -> ReturnValue:  ##
    global(connection)
    
    try:
        connection.execute(f"DELETE FROM Query WHERE Qid={query.getQueryID()}")
    except:
        return ReturnValue.ERROR
       
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    

    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []
