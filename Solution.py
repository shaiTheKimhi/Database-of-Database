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
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ALREADY_EXISTS if kind != 'delete' else ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ERROR
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return ReturnValue.ERROR

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS if kind == 'delete' else ReturnValue.OK
    conn.commit()
    conn.close()
    return ReturnValue.OK
        
def aggregate(query):
    try:
        conn = Connector.DBConnector()
        rows_effected, output = conn.execute(query)
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return -1 #error value

    if rows_effected == 0:
        return 0  #default value
    conn.close()
    return output.rows[0][0]
        
        
def get_rows(query, default_value=[], error_value=[]):
    try:
        conn = Connector.DBConnector()
        rows_effected, output = conn.execute(query)

    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            nop = 0 #no operation here
        return error_value, -1 #error value
    if rows_effected == 0:
        return default_value, 0
    return output.rows, rows_effected
    


# maybe we should replace the prints at the expects
def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Queries(Qid INTEGER PRIMARY KEY,\
                     Purpose TEXT NOT NULL,\
                     Qsize INTEGER NOT NULL,\
                     check (Qid>0),\
                     check (QSize>=0))")
        conn.execute("CREATE TABLE Disk(Did INTEGER PRIMARY KEY,\
                     company TEXT NOT NULL,\
                     Speed INTEGER NOT NULL, \
                     Dspace INTEGER NOT NULL,\
                     Cost INTEGER NOT NULL,\
                     check (Did>0),\
                     check (Speed>0),\
                     check (Cost>0),\
                     check (Dspace>=0))")
        conn.execute("CREATE TABLE Ram(Rid INTEGER PRIMARY KEY,\
                     Company TEXT NOT NULL, \
                     Rspace INTEGER NOT NULL,\
                     check (Rid>0),\
                     check (Rspace>0))") #TODO: CHECK if Did is PRIMARY KEY OR NOT
        conn.execute("CREATE TABLE QueryToDisk(Qid INTEGER ,\
                     Did INTEGER ,\
                     Qsize INTEGER NOT NULL,\
                     CONSTRAINT Identity PRIMARY KEY (Qid, Did),\
                     FOREIGN KEY (Qid) REFERENCES Queries(Qid) ON DELETE CASCADE ,\
                     FOREIGN KEY(Did) REFERENCES Disk(Did) ON DELETE CASCADE,\
                     check (Qsize>=0))")  # maybe we should creat ramToQuery also
        conn.execute("CREATE TABLE RamToDisk(Rid INTEGER ,\
                     Did INTEGER ,\
                     CONSTRAINT descriptor PRIMARY KEY (Rid, Did), \
                     FOREIGN KEY (Did) REFERENCES Disk(Did) ON DELETE CASCADE ,\
                     FOREIGN KEY(Rid) REFERENCES Ram(Rid) ON DELETE CASCADE)")
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
    activate("BEGIN;DELETE FROM Queries; DELETE FROM Disk; DELETE FROM RAM; DELETE FROM QueryToDisk; DELETE FROM RamToDisk; COMMIT;")


# maybe we should replace the prints at the expects
def dropTables():
    conn = Connector.DBConnector()
    try:
        conn.execute("DROP TABLE IF EXISTS Queries CASCADE")
        conn.execute("DROP TABLE IF EXISTS Disk CASCADE")
        conn.execute("DROP TABLE IF EXISTS Ram CASCADE")
        conn.execute("DROP TABLE IF EXISTS QueryToDisk CASCADE")
        conn.execute("DROP TABLE IF EXISTS RamToDisk CASCADE")
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
            return_val = ReturnValue.BAD_PARAMS
            #conn.rollback()
        else:
            query = f"INSERT INTO Queries(Qid, Purpose, Qsize) VALUES ({Id}, '{purpose}', {size})"
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
    except Exception as e:
        return_val = ReturnValue.ERROR
    finally:
        conn.close()
        return return_val


def getQueryProfile(queryID: int) -> Query:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Queries WHERE Qid={queryID}")
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
        return Query(*result.rows[0])
        #return result


# need to complete and change the query
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    rows_effected = 0
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        Id = query.getQueryID()
        #Using Cascade this query should delete all appearances of Query including in 
        query = f"BEGIN;UPDATE Disk SET Dspace = Dspace + Qsize FROM QueryToDisk WHERE QueryToDisk.Qid={Id} AND Disk.Did=QueryToDisk.Did;\
        DELETE FROM Queries WHERE Qid={Id};\
        " #automatically deletes from QueryToDisk and so we only remain to add removed values to Dspace at Disk
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        conn.close()
        return_value = ReturnValue.ERROR
    finally:
        if rows_effected == 0 and return_value == ReturnValue.OK:
            return_value = ReturnValue.NOT_EXISTS
        conn.close()
        return return_value


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
            return_val = ReturnValue.BAD_PARAMS
            #conn.rollback()
        else:
            query = (f"INSERT INTO Disk(Did, Company,Speed, Dspace, Cost) VALUES ({Id},\
                             '{company}',{speed},{d_space},{cost})")
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
        rows_effected, result = conn.execute(f"SELECT * FROM Disk WHERE Did={diskID}")
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
    except Exception as e:
        return Disk.badDisk()

    if rows_effected == 0:
        return Disk.badDisk()
    conn.close()
    #return result
    return Disk(*result.rows[0])


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = f"DELETE FROM Disk WHERE Did={diskID}"
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return_value = ReturnValue.ERROR
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0 and return_value == ReturnValue.OK:
            return_value = ReturnValue.NOT_EXISTS
        return return_value


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    return_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        Id = ram.getRamID()
        company = ram.getCompany()
        r_size = ram.getSize()
        if (type(Id) is not int) or (type(company) is not str) or (type(r_size) is not int):
            return_val = ReturnValue.BAD_PARAMS
            #conn.rollback()
        else:
            query = f"INSERT INTO Ram(Rid, Company, Rspace) VALUES ({Id}, '{company}', {r_size})"
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
        rows_effected, result = conn.execute(f"SELECT * FROM Ram WHERE Rid={ramID}")  # , printSchema=printSchema)
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

    if rows_effected == 0:
        return RAM.badRAM()
    conn.close()
    #return result
    return RAM(*result.rows[0])

def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = f"DELETE FROM Ram WHERE Rid={ramID}"
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        return_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        return_value = ReturnValue.ERROR
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        if rows_effected == 0 and return_value == ReturnValue.OK:
            return_value = ReturnValue.NOT_EXISTS
        conn.close()
        return return_value


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
        query = f"BEGIN; INSERT INTO Queries(Qid, Purpose, Qsize) VALUES ({Id}, '{purpose}', {size});\
                        INSERT INTO Disk(Did, Company,Speed, Dspace, Cost) VALUES ({D_id},\
                         '{company}',{speed},{d_space},{cost});COMMIT"
        rows_effected, _ = conn.execute(query)
        #conn.commit()
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
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    Qid = query.getQueryID()
    size = query.getSize()
    if (type(query) is not Query) or (type(Qid) is not int) or (type(size) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
    #elif (Qid <= 0) or (size < 0) or ()

    try:
        conn = Connector.DBConnector()
        query = f"BEGIN;\
        INSERT INTO QueryToDisk(Qid, Did, Qsize) VALUES ({Qid}, {diskID}, {size})\
        ;UPDATE Disk SET Dspace=Dspace-{size} WHERE Disk.Did={diskID};"
        
        '''SELECT (Qid, Did, Dspace, sum(Qsize) as usedSpace)\
        FROM QueryToDisk INNER JOIN Disk ON (Disk.Did = QueryToDisk.Did)\
        WHERE usedSpace+{size} <= Dspace\
        GROUP BY Did")'''
        rows_effected, _ = conn.execute(query)
        conn.commit()
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS

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
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        conn.close()
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    Qid = query.getQueryID()
    if (type(query) is not Query) or (type(Qid) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
        
    try:
        conn = Connector.DBConnector()
        query = f"BEGIN;UPDATE Disk SET Dspace=Dspace+{query.getSize()} WHERE Did={diskID}; \
                DELETE FROM QueryToDisk WHERE (Qid={Qid} AND  Did = {diskID});"
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            conn.rollback()
            return ReturnValue.OK
        conn.commit()

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
    except Exception as e:
        conn.rollback()
        return ReturnValue.ERROR

    conn.close()
    return ReturnValue.OK

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    if (type(ramID) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
    return activate(f"INSERT INTO RamToDisk(Rid, Did) VALUES ({ramID}, {diskID})", "insert")


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    if (type(ramID) is not int) or (type(diskID) is not int):
        return ReturnValue.BAD_PARAMS
        
    return activate(f"DELETE FROM RamToDisk WHERE (Rid={ramID} And Did={diskID})")


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return aggregate(f"SELECT AVG(Qsize) FROM QueryToDisk WHERE (Did={diskID}) GROUP BY Did")


def diskTotalRAM(diskID: int) -> int:
    return aggregate(f"SELECT SUM(Rspace) FROM RamToDisk INNER JOIN RAM ON RamToDisk.Rid=RAM.Rid WHERE (Did={diskID}) GROUP BY Did")


def getCostForPurpose(purpose: str) -> int:
    return aggregate(f"SELECT COALESCE(SUM(fo.money),0) as total_cost FROM \
    (SELECT Disk.Cost*Queries.Qsize as money FROM (Queries INNER JOIN QueryToDisk ON Queries.Qid=QueryToDisk.Qid) \
    INNER JOIN Disk ON QueryToDisk.Did=Disk.Did WHERE Queries.Purpose='{purpose}') AS fo")


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return list([item[0] for item in get_rows(f"SELECT Queries.Qid FROM Queries WHERE Queries.QSize <= \
    (SELECT Dspace FROM Disk WHERE Did={diskID}) ORDER BY Queries.Qid DESC LIMIT 5;")[0]])


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return list([item[0] for item in get_rows(f"SELECT Queries.Qid FROM Queries WHERE (Queries.QSize <= \
    (SELECT Dspace FROM Disk WHERE Did={diskID}) AND Queries.QSize <= (SELECT COALESCE(SUM(Rspace),0) FROM RamToDisk INNER JOIN RAM ON RamToDisk.Rid=RAM.Rid WHERE RamToDisk.Did={diskID}))  ORDER BY Queries.Qid LIMIT 5;")[0]])


def isCompanyExclusive(diskID: int) -> bool: 
    #query = f"SELECT * FROM ((Disk INNER JOIN RamToDisk ON Disk.Did=RamToDisk.Did) INNER JOIN RAM ON RamToDisk.Rid=RAM.Rid) WHERE RAM.Company != Disk.Company AND Disk.Did={diskID}"
    query = f"SELECT * FROM Disk WHERE 0 = (SELECT count(*) FROM ((Disk INNER JOIN RamToDisk ON Disk.Did=RamToDisk.Did) INNER JOIN RAM ON RamToDisk.Rid=RAM.Rid) WHERE RAM.Company != Disk.Company AND Disk.Did={diskID})\
     AND 1 <= (SELECT count(*) FROM Disk WHERE Did={diskID});"
    return get_rows(query)[1] > 0 #company of Disk is exlusive iff all of the RAMs attached to it are of the same company i.e there are no RAMs of different company.


def getConflictingDisks() -> List[int]:
    query = "SELECT DISTINCT Did FROM QueryToDisk WHERE QueryToDisk.Qid IN (SELECT Qid FROM QueryToDisk GROUP BY Qid HAVING COUNT(Qid) > 1) ORDER BY Did"
    rows = get_rows(query)[0]
    res = list([i[0] for i in rows])
    return res


def mostAvailableDisks() -> List[int]:
    query = "SELECT id FROM (\
    SELECT COALESCE(Disk.Did,0) as id, COALESCE(Disk.Speed,0) as spd, COALESCE(ava.cqid,0) as cq \
     FROM Disk LEFT JOIN(SELECT Did, Speed, COALESCE(COUNT(Qid),0) AS cqid FROM (Disk LEFT JOIN Queries ON TRUE ) \
    WHERE Qsize <= Dspace GROUP BY Did ORDER BY cqid DESC, Speed DESC, Disk.Did)AS ava ON Disk.Did=ava.Did ORDER BY cq DESC, spd DESC, id LIMIT 5) as f2"
    return list([i[0] for i in get_rows(query)[0]])


def getCloseQueries(queryID: int) -> List[int]:
    res = get_rows(f"(SELECT Did FROM QueryToDisk WHERE QueryToDisk.Qid={queryID})")

    res = get_rows(f"(SELECT COUNT(Did) as c, Queries.Qid \
    FROM Queries,QueryToDisk \
    WHERE Queries.qid=QueryToDisk.qid AND Did IN (SELECT Did FROM QueryToDisk WHERE QueryToDisk.Qid={queryID})\
    AND Queries.Qid != {queryID}\
    GROUP BY Queries.Qid)")

    query = f"SELECT Qid FROM \
    (SELECT COUNT(Did) as c, Queries.Qid \
    FROM Queries INNER JOIN QueryToDisk ON Queries.Qid=QueryToDisk.Qid\
    WHERE Did IN (SELECT Did FROM QueryToDisk WHERE QueryToDisk.Qid={queryID})\
    AND Queries.Qid != {queryID}\
    GROUP BY Queries.Qid \
    HAVING COUNT(Did) >= (SELECT Count(Did) FROM QueryToDisk WHERE QueryToDisk.Qid={queryID})) AS clo\
    ORDER BY Qid LIMIT 10"
    return list(get_rows(query)[0])
    
    
    
    
    