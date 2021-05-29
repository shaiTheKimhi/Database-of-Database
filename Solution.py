from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.DBConnector import ResultSet
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def activate(query, kind='delete'):
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return ReturnValue.ERROR
    finally:
        if row_effected == 0:
            return ReturnValue.NOT_EXISTS if kind == 'delete' else ReturnValue.OK
        conn.close()
        return ReturnValue.OK
        
def aggregate(query):
    try:
        conn = Connector.DBConnector()
        rows_effected, output = conn.execute(query)
    except Exception:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return -1 #error value
    finally:
        if row_effected == 0:
            return 0  #default value
        conn.close()
        return output
        
        
def get_rows(query, default_value, error_value):
    try:
        conn = Connector.DBConnector()
        rows_effected, output = conn.execute(query)
    except Exception:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return error_value #error value
    finally:
        return output, rows_effected
    


# maybe we should replace the prints at the expects
def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Queries(Qid INTEGER PRIMARY KEY,\
                     Purpose TEXT NOT NULL,\
                     Qsize INTEGER NOT NULL,\
                     check(Qid>0),\
                     check (QSize>0))")
        conn.execute("CREATE TABLE Disk(Did INTEGER PRIMARY KEY,\
                     company TEXT NOT NULL,\
                     Speed INTEGER NOT NULL, \
                     Cost INTEGER NOT NULL,\
                     Dspace INTEGER NOT NULL\
                     check (Did>0),\
                     check(Speed>0),\
                     check (Cost>0));\
                     check (Dspace>=0)")
        conn.execute("CREATE TABLE Ram(Rid INTEGER PRIMARY KEY,\
                     Company TEXT NOT NULL, \
                     Rspace INTEGER NOT NULL,\
                     check(Rid>0),\
                     check (Rspace>0));") #TODO: CHECK if Did is PRIMARY KEY OR NOT
        conn.execute("CREATE TABLE QueryToDisk(Qid INTEGER PRIMARY KEY ,\
                     Did INTEGER PRIMARY KEY,\
                     Qsize INTEGER NOT NULL,\
                     FOREIGN KEY (Qid) REFERENCES Queries(Qid) ON DELETE CASCADE ,\
                     FOREIGN KEY(Did) REFERENCES Disk(Did) ON DELETE CASCADE,\
                     check(Qsize>0);")  # maybe we should creat ramToQuery also
        conn.execute("CREATE TABLE RamToDisk(Rid INTEGER PRIMARY KEY ,\
                     Did INTEGER PRIMARY KEY,\
                     Rsize INTEGER\
                     FOREIGN KEY (Did) REFERENCES Disk(Did) ON DELETE CASCADE ,\
                     FOREIGN KEY(Rid) REFERENCES Ram(Rid) ON DELETE CASCADE,\
                     check(Rsize>0);")
        conn.commit()
        conn.close()
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
        if rows_effected == 0:
            return Query.badQuery()
        conn.close()
        return Query.Query(result)
        #return result


# need to complete and change the query
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        Id = query.getQueryID()
        #Using Cascade this query should delete all appearances of Query including in 
        query = f"BEGIN;DELETE FROM Queries WHERE Qid={Id};\
        UPDATE Disk SET Disk.Dspace = Disk.Dspace - t2.QuerySize FROM (Disk INNER JOIN QueryToDisk ON Disk.Did=QueryToDisk.Did) WHERE QueryToDisk.Qid={Id};\
        COMMIT" #automatically deletes from QueryToDisk and so we only remain to add removed values to Dspace at Disk
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except Exception:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    finally:
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
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
        if rows_effected == 0:
            return Disk.badDisk()
        conn.close()
        #return result
        return Disk.Disk(result)


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
        if row_effected == 0:
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
        if rows_effected == 0:
            return RAM.badRAM()
        conn.close()
        #return result
        return RAM.RAM(result)

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
        if rows_effected == 0:
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
        query = sql.SQL(f"BEGIN; INSERT INTO Queries(Qid, Purpose, Qsize) VALUES ({Id}, {purpose}, {Qsize});"
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
    Qid = query.getQueryID()
    size = query.getSize()
    if (type(query) is not Query) or (type(Qid) is not int) or (type(size) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
    
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"BEGIN;UPDATE Disk SET Dspace=Dspace-{size} WHERE Disk.Did={diskID}\
        INSERT INTO QueryToDisk(Qid, Did, Qsize) VALUES ({Qid}, {size}, {diskID})\
        ;COMMIT;")
        
        '''SELECT (Qid, Did, Dspace, sum(Qsize) as usedSpace)\
        FROM QueryToDisk INNER JOIN Disk ON (Disk.Did = QueryToDisk.Did)\
        WHERE usedSpace+{size} <= Dspace\
        GROUP BY Did")'''
        rows_effected, _ = conn.execute(query)
        if row_effected == 0:
            return BAD_PARAMS
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


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    Qid = query.getQueryID()
    if (type(query) is not Query) or (type(Qid) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
        
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM QueryToDisk WHERE (Qid={Qid} AND  Did = {diskID})") #TODO: reduce Qsize from Disk freespace
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

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    if (type(query) is not Query) or (type(Qid) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
    return activate(f"INSERT INTO RamToDisk(Rid, Qid) VALUES ({ramID}, {diskID})")


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    if (type(query) is not Query) or (type(Qid) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
    return activate(f"DELETE FROM RamToDisk WHERE (Rid={ramID} And Did={diskID})")


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return aggregate(f"SELECT AVG(Qsize) FROM QueryToDisk WHERE (Did={diskID}) GROUP BY Did")


def diskTotalRAM(diskID: int) -> int:
    return aggregate(f"SELECT SUM(Rsize) FROM RamToDisk WHERE (Did={diskID}) GROUP BY Did")


def getCostForPurpose(purpose: str) -> int:
    return aggregate(f"SELECT SUM(money) as total_cost FROM \
    (SELECT Disk.Cost*Queries.Qsize FROM (Queries INNER JOIN QueryToDisk ON Queries.Qid=QueryToDisk.Qid) \
    INNER JOIN Disk ON QueryToDisk.Did=Disk.Did WHERE Queries.Purpose={purpose})")


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return list(get_rows(f"SELECT TOP 5 Queries.Qid FROM Queries WHERE Queries.QSize <= \
    (SELECT Disk.Dspace WHERE Disk.Did={diskID});")[0])


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return list(get_rows(f"SELECT TOP 5 Queries.Qid FROM Queries WHERE (Queries.QSize <= \
    (SELECT Disk.Dspace WHERE Disk.Did={diskID}) AND Queries.QSize <= (SELECT SUM(RamToDisk.Rsize) FROM RamToDisk WHERE RamToDisk.Did={diskID}))  ORDER BY Queries.Qid;")[0])


def isCompanyExclusive(diskID: int) -> bool: 
    query = f"SELECT * FROM ((Disk INNER JOIN RamToDisk ON Disk.Did=RamToDisk.Did) INNER JOIN RAM ON RamToDisk.Rid=RAM.Rid) WHERE RAM.Company != Disk.Company AND Disk.Did={diskID}"
    return get_rows(query)[1] == 0 #company of Disk is exlusive iff all of the RAMs attached to it are of the same company i.e there are no RAMs of different company.


def getConflictingDisks() -> List[int]:
    query = "SELECT DISTINCT Did FROM QueryToDisk WHERE Qid IN (SELECT Qid,COUNT(Qid) AS cn FROM QueryToDisk GROUP BY Qid HAVING cn > 1)"
    return list(get_rows(query)[0])


def mostAvailableDisks() -> List[int]:
    query = "SELECT Disk.Did FROM \
    (SELECT TOP 5 Disk.Did, Disk.Speed, COUNT(Qid) AS cqid FROM (Disk, Queries)\
    WHERE Queries.Qsize <= Disk.Dspace) GROUP BY Disk.Did ORDER BY cqid DESC, Disk.Speed DESC, Disk.Did"
    return list(get_rows(query)[0])


def getCloseQueries(queryID: int) -> List[int]:
    query = f"SELECT TOP 10 Qid FROM \
    (SELECT Queries.Qid, COUNT(Did) as cnt\
    FROM Queries INNER JOIN QueryToDisk ON Queries.Qid=QueryToDisk.Qid\
    WHERE Did IN (SELECT Did FROM QueryToDisk WHERE QueryToDisk.Qid={queryID})\
    AND Queries.Qid != {queryID}\
    GROUP BY Queries.Qid \
    HAVING cnt >= (SELECT Count(Did) FROM QueryToDisk WHERE QueryToDisk.Qid={queryID}))\
    ORDER BY Queries.Qid"
    return list(get_rows(query)[0])
    
    
    
    
    