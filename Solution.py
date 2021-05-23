from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.DBConnector import ResultSet
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


# maybe we should replace the prints at the expects
def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Queries(Qid INTEGER PRIMARY KEY,"
                     "Purpose TEXT NOT NULL,"
                     "Qsize INTEGER NOT NULL,"
                     "check(Qid>0),"
                     "check (Size>0))")
        conn.execute("CREATE TABLE Disk(Did INTEGER PRIMARY KEY,"
                     "company TEXT NOT NULL,"
                     "Speed INTEGER NOT NULL, "
                     "Dspace INTEGER NOT NULL,"
                     " Cost INTEGER NOT NULL,"
                     "check (Did>0),"
                     "check(Speed>0),"
                     "check (space>0),"
                     "check (Cost>0));")
        conn.execute("CREATE TABLE Ram(Rid INTEGER PRIMARY KEY,"
                     "Company TEXT NOT NULL, "
                     "Rspace INTEGER NOT NULL,"
                     "check(Rid>0),"
                     "check (Size>0));")
        conn.execute("CREATE TABLE QueryToDisk(Qid INTEGER PRIMARY KEY ,"
                     "Did INTEGER PRIMARY KEY ,"
                     "Qsize INTEGER NOT NULL,"
                     "Cost INTEGER NOT NULL,"
                     "FOREIGN KEY (Qid) REFERENCES Queries(Qid) ON DELETE CASCADE ,"
                     "FOREIGN KEY(Did) REFERENCES Disk(Did) ON DELETE CASCADE,"
                     "check(size>0),"
                     "check (Cost>0));")  # maybe we should creat ramToQuery also
        conn.execute("CREATE TABLE QueryToRam(Qid INTEGER PRIMARY KEY ,"
                     "Rid INTEGER PRIMARY KEY ,"
                     "Qsize INTEGER NOT NULL,"
                     "Cost INTEGER NOT NULL,"
                     "FOREIGN KEY (Qid) REFERENCES Queries(Qid) ON DELETE CASCADE ,"
                     "FOREIGN KEY(Rid) REFERENCES Ram(Rid) ON DELETE CASCADE,"
                     "check(size>0),"
                     "check (Cost>0));")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    pass


# maybe we should replace the prints at the expects
def dropTables():
    conn = Connector.DBConnector()
    try:
        conn.execute("DROP TABLE IF EXISTS Queries CASCADE")
        conn.execute("DROP TABLE IF EXISTS Disk CASCADE")
        conn.execute("DROP TABLE IF EXISTS Ram CASCADE")
        conn.execute("DROP TABLE IF EXISTS QueryToDisk CASCADE")
        conn.execute("DROP TABLE IF EXISTS QueryToRam CASCADE")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    return_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        Id = query.getQueryID()
        purpose = query.getPurpose()
        size = query.getSize()
        if (type(Id) is not int) or (type(purpose) is not str) or (type(size) is not int):
            return ReturnValue.BAD_PARAMS
        query = sql.SQL("INSERT INTO Queries(Qid, Purpose, Qsize) VALUES ({Id}, {purpose}, {Qsize})").format \
            (Qid=sql.Literal(Id), Purpose=sql.Literal(purpose), Qsize=sql.Literal(size))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return_val = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return_val = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return_val = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return_val = ReturnValue.ERROR
    except Exception:
        return_val = ReturnValue.ERROR
    finally:
        conn.close()
        return return_val


def getQueryProfile(queryID: int) -> Query:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Queries WHERE Qid={queryID}")
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Query.badQuery()
    except DatabaseException.NOT_NULL_VIOLATION:
        return Query.badQuery()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Query.badQuery()
    except DatabaseException.UNIQUE_VIOLATION:
        return Query.badQuery()
    except DatabaseException.CHECK_VIOLATION:
        return Query.badQuery()
    except Exception:
        return Query.badQuery()
    finally:
        if rows_effected is 0:
            return Query.badQuery()
        conn.close()
        return result


# need to complete and change the query
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        Id = query.getQueryID()
# after the begin and before delete we should add update to disk table ; update to ram table; after find relevant id#
        query = sql.SQL("BEGIN;DELETE FROM Queries WHERE Qid={Id};COMMIT").format(Qid=sql.Literal(Id))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    return_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        Id = disk.getDiskID()
        cost = disk.getCost()
        speed = disk.getSpeed()
        company = disk.getCompany()
        d_space = disk.getFreeSpace()
        if (type(Id) is not int) or (type(company) is not str) or (type(cost) is not int) \
                or (type(speed) is not int) or (type(d_space) is not int):
            return ReturnValue.BAD_PARAMS
        query = sql.SQL("INSERT INTO Disk(Did, Company,Speed, Dspace, Cost) VALUES ({Id},"
                        " {company},{speed},{d_space},{cost})"). \
            format(Did=sql.Literal(id), Company=sql.Literal(company),
                   Speed=sql.Literal(company), Dspace=sql.Literal(d_space), Cost=sql.Literal(cost))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:  # no need as e?
        return_val = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return_val = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return_val = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return_val = ReturnValue.ERROR
    except Exception as e:
        return_val = ReturnValue.ERROR
    finally:
        conn.close()
        return return_val


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Disk WHERE Did={diskID}")
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Disk.badDisk()
    except DatabaseException.NOT_NULL_VIOLATION:
        return Disk.badDisk()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Disk.badDisk()
    except DatabaseException.UNIQUE_VIOLATION:
        return Disk.badDisk()
    except DatabaseException.CHECK_VIOLATION:
        return Disk.badDisk()
    except Exception:
        return Disk.badDisk()
    finally:
        if rows_effected is 0:
            return Disk.badDisk()
        conn.close()
        return result


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    row_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE Did={diskID}").format(Did=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
        if row_effected is 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    return_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        Id = ram.getRamID()
        company = ram.getCompany()
        r_size = ram.getSize()
        if (type(Id) is not int) or (type(company) is not str) or (type(r_size) is not int):
            return ReturnValue.BAD_PARAMS
        query = sql.SQL("INSERT INTO Ram(Rid, Company, Rspace) VALUES ({Id}, {company}, {r_size})"). \
            format(Rid=sql.Literal(id), Company=sql.Literal(company), Rspace=sql.Literal(r_size))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return_val = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return_val = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return_val = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return_val = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return_val = ReturnValue.ERROR
    except Exception:
        return_val = ReturnValue.ERROR
    finally:
        conn.close()
        return return_val


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Ram WHERE Rid={ramID}")  # , printSchema=printSchema)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return RAM.badRAM()
    except DatabaseException.NOT_NULL_VIOLATION:
        return RAM.badRAM()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return RAM.badRAM()
    except DatabaseException.UNIQUE_VIOLATION:
        return RAM.badRAM()
    except DatabaseException.CHECK_VIOLATION:
        return RAM.badRAM()
    except Exception:
        return RAM.badRAM()
    finally:
        if rows_effected is 0:
            return RAM.badRAM()
        conn.close()
        return result


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Ram WHERE Rid={ramID}").format(Rid=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.ERROR
    except Exception:
        return ReturnValue.ERROR
    finally:
        if rows_effected is 0:
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    Id = query.getQueryID()
    purpose = query.getPurpose()
    size = query.getSize()
    if (type(Id) is not int) or (type(purpose) is not str) or (type(size) is not int):
        return ReturnValue.BAD_PARAMS
    D_id = disk.getDiskID()
    cost = disk.getCost()
    speed = disk.getSpeed()
    company = disk.getCompany()
    d_space = disk.getFreeSpace()
    if (type(D_id) is not int) or (type(company) is not str) or (type(cost) is not int) \
            or (type(speed) is not int) or (type(d_space) is not int):
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN; INSERT INTO Queries(Qid, Purpose, Qsize) VALUES ({Id}, {purpose}, {Qsize});"
                        "INSERT INTO Disk(Did, Company,Speed, Dspace, Cost) VALUES ({D_id},"
                        " {company},{speed},{d_space},{cost});COMMIT;")
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except Exception:
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
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
