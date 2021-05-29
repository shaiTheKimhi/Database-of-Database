import Solution
from Utility.ReturnValue import ReturnValue
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk


Solution.createTables()

print(Solution.addDisk(Disk(1, "DELL", 10, 10, 10))) #OK
print(Solution.addDisk(Disk(2, "DELL", 10, 10, 10))) #OK 
print(Solution.addDisk(Disk(3, "DELL", 10, 10, 10))) #OK
print(Solution.addDisk(Disk(1, "DELL", 10, 10, 10))) #ALREADY_EXIST

print(Solution.addDisk(Disk(4, "HP", 0, 10, 10))) #BAD_PARAMS
print(Solution.addDisk(Disk(0, "HP", 10, 10, 10))) #BAD_PARAMS
print(Solution.addDisk(Disk(4, "HP", 10, 10, 0)))
print(Solution.addDisk(Disk(4, "HP", 10, -1, 10)))


Solution.dropTables()