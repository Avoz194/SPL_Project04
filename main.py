import atexit
import sys
import sqlite3


def main(args):
    configPath = args[1]
    ordersPath = args[2]
    outputPath = args[3]
    repo = _Repository()
    repo.create_tables()
    configParser(configPath, repo) #parse the config file
    ordersParser(ordersPath, outputPath, repo)
    atexit.register(repo._close)  # TODO: figure out what is this line


if __name__ == '__main__':
    main(sys.argv)


def configParser(inputFile, repo):
    with open(inputFile, encoding="utf-8") as inputfile:
        for i, line in inputfile: #TODO:does i counts in this case? behave as a counter?
            lineArray = line.split(',')
            if i==0:
                endOfVac = lineArray[0] # 3 : indexes 1 2 3
                endOfSup = endOfVac+lineArray[1] # 3+1 : indexes 4
                endOfClin = endOfSup +lineArray[2] # 4+2 : indexes 5 6
            if 0 < i <= endOfVac: #vaccines
                repo.vaccines.insert(*lineArray)
            if endOfVac < i <= endOfSup: #suppliar
                repo.suppliers.insert(*lineArray)
            if endOfSup < i <= endOfClin: #clincs
                repo.clinics.insert(*lineArray)
            if i > endOfClin: #logistics
                repo.logistics.insert(*lineArray)
    inputfile.close()

def ordersParser(inputFile, outputPath, repo):
    with open(inputFile) as inputfile:
        for line in inputfile:
            lineArray = line.split(',')
            if len(lineArray) ==2:
                repo.send_Shipment(*lineArray)
            if len(lineArray) ==3:
                repo.receive_Shipment(*lineArray)
            logArray = repo.action_log()
            with open(outputPath, "w") as outputFile:
                outputFile.write(','.join(logArray))
                outputFile.close()
    inputfile.close()



# DTOs Section
class Vaccine:
    def __init__(self, id, date, supplier, quantity):
        self.id = id
        self.date = date
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name, logistic):
        self.id = id
        self.name = name
        self.logistic = logistic


class Clinic:
    def __init__(self, id, location, demand, logistic):
        self.id = id
        self.location = location
        self.demand = demand
        self.logistic = logistic


class Logistic:
    def __init__(self, id, name, count_sent, count_received):
        self.id = id
        self.name = name
        self.count_sent = count_sent
        self.count_received = count_received

# The Repository

class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE vaccines (
            id        INT         PRIMARY KEY,
            date      DATE        NOT NULL,
            supplier  INT         NOT NULL,
            quantity  INT         NOT NULL,    
            FOREIGN KEY(supplier)     REFERENCES suppliers(id)
        );
        
        CREATE TABLE suppliers (
            id        INT        PRIMARY KEY,
            name      TEXT       NOT NULL,
            logistic  INT        NOT NULL,
            FOREIGN KEY(logistic)     REFERENCES logistics(id)

        );
        
        CREATE TABLE clinics (
            id        INT      PRIMARY KEY,
            location  TEXT     NOT NULL,
            demand    INT      NOT NULL,
            logistic  INT      NOT NULL,
            FOREIGN KEY(logistic)     REFERENCES logistics(id)
        );
        
         CREATE TABLE logistics (
            id             INT     PRIMARY KEY,
            name           TEXT    NOT NULL,
            count_sent     INT     NOT NULL,
            count_received INT     NOT NULL 
        );
    """)

    def receive_Shipment(self, supname, amount, date):
        logId = self.suppliers.getLogId(supname)
        self.logistics.incCountRecived(amount, logId)
        self.vaccines.insert(Vaccine(self.vaccines.maxId()+1, date, logId, amount))

    def send_Shipment(self, location, amount):
        logId = self.clinics.getLogId(location)
        self.clinics.reduceDemend(amount, location)
        self.vaccines.removeAmount(amount)
        self.logistics.incCountSent(amount, logId)

    def action_log(self):
        totalInven = self.vaccines.total_inventory()
        totalDema = self.clinics.total_demand()
        totalRec = self.suppliers.total_recevied()
        totalSent = self.suppliers.total_sent()
        return [totalInven, totalDema, totalSent, totalRec]
# DAO

class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        self._conn.execute("""
               INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?);""", [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])


    def maxId(self):
        cursor = self._conn.execute("""
                              SELECT MAX(id) FROM vaccines """)
        return cursor.fetchone()[0]

    def removeAmount(self, amount):
        amount_clone=amount
        index = 0
        size = self.size()
        cursor = self._conn.execute("""
                      SELECT id,quantity FROM vaccines ORDER BY date """)
        while index != size and len(cursor)!=0 and index!= self.maxId()+1 :
            curr_inventory = cursor.fetchone()[index]
            if amount_clone>curr_inventory[1]:
                self._conn.execute("""
                                           DELETE from vaccine where id = curr_inventory[0] """)
                amount_clone = amount_clone - curr_inventory[1]
            else:
                update_inventory = curr_inventory - amount
                self._conn.execute(
                    """UPDATE vaccines SET quantity = %d WHERE id=curr_inventory[0] """ % update_inventory)
            index += 1
        self._conn.commit()

    def total_inventory(self):
        cursor = self._conn.execute("""SELECT SUM(quantity) FROM vaccines """)
        return cursor.fetchone()[0]




class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
                INSERT INTO suppliers (id, name, logistics) VALUES (?, ?, ?)
        """, [supplier.id, supplier.name, supplier.logistic])

    # get the logistics's id from suplier name
    def getLogId(self, name):
        cursor = self._conn.execute("""
                  SELECT logistics FROM suppliers WHERE name=%d""" % name)
        return cursor.fetchone()[0]



class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, clinic):
        self._conn.execute("""
            INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?,?)
        """, [clinic.id, clinic.location, clinic.demand, clinic.logistic])

    def getLogId(self, location):
        cursor = self._conn.execute("""
                  SELECT logistics FROM clinics WHERE location=%d""" % location)
        return cursor.fetchone()[0]

    # reduce the amount from the location demand
    def reduceDemend(self ,amount, location):
        cursor = self._conn.execute("""
                       SELECT demand FROM clinics WHERE location=%d""" % location)
        curr_inventory = cursor.fetchone()[0] -amount
        self._conn.execute("""UPDATE clinics SET demand = %s WHERE location= %d """ % (curr_inventory, location)) #TODO: cursur is the running q with the result, not an actual number
        self._conn.commit()

    def size(self):
        cursor = self._conn.execute("""
                              SELECT COUNT(*) FROM clinics """)
        return cursor.fetchone()[0]

    def total_demand(self):
        cursor = self._conn.execute("""SELECT SUM(demand) FROM clinics """)
        return cursor.fetchone()[0]

class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, logistic):
        self._conn.execute("""
            INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?)
        """, [logistic.id, logistic.name, logistic.count_sent, logistic.count_received])

    def getLogId(self,name):
        logId = self._conn.execute("""SELECT id FROM logistics WHERE name=%d""" % name)  # TODO: why ' and not """ ?
        return logId.fetchone()[0]

    #increase the count_recived/count_sent by amount
    def incCountRecived(self, amount, logId):
        cr = self._conn.execute("""SELECT count_received FROM logistics WHERE id=%d""" % logId)  #TODO: why ' and not """ ?
        logistic_id_cr = cr.fetchone()[0]
        new_logistic_id_cr = logistic_id_cr + amount
        self._conn.execute("""UPDATE logistics SET count_received = %s WHERE id= %d """ %(new_logistic_id_cr,logId))
        self._conn.commit()

    def incCountSent(self, amount, logId):
        cr = self._conn.execute("""SELECT count_sent FROM logistics WHERE id=%d""" % logId) #TODO: why ' and not """ ?
        logistic_id_cs = cr.fetchone()[0]
        new_logistic_id_cs = logistic_id_cs + amount
        self._conn.execute("""UPDATE logistics SET count_sent = %s WHERE id= %d """ % (new_logistic_id_cs, logId))
        self._conn.commit()

    def total_recevied(self):
        cursor = self._conn.execute("""SELECT SUM(count_received) FROM logistics """)
        return cursor.fetchone()[0]

    def total_sent(self):
        cursor = self._conn.execute("""SELECT SUM(count_sent) FROM logistics """)
        return cursor.fetchone()[0]
