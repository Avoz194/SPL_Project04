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
    repo._close();
    atexit.register(repo._close)  # TODO: figure out what is this line


if __name__ == '__main__':
    main(sys.argv)


def configParser(inputFile, repo):  # TODO: complete config_Parser
    with open(inputFile, encoding="utf-8" ) as inputfile:
        for i, line in inputfile: #TODO:insert new object using repository
            if i==0:
                lineArray = line.split(',')
                endOfVac = lineArray[0] # 3 : indexes 1 2 3
                endOfSup = endOfVac+lineArray[1] # 3+1 : indexes 4
                endOfClin = endOfSup +lineArray[2] # 4+2 : indexes 5 6
            if 0 < i <= endOfVac: #vaccines
                lineArrayV = line.split(',')
                repo.vaccines.insert(Vaccine(lineArrayV[0],lineArrayV[1],lineArrayV[2],lineArrayL[3]))
            if endOfVac < i <= endOfSup: #suppliar
                lineArrayS = line.split(',')
                repo.suppliers.insert(Supplier(lineArrayS[0],lineArrayS[1],lineArrayS[2]))
            if endOfSup < i <= endOfClin: #clincs
                lineArrayC = line.split(',')
                repo.clinics.insert(Clinic(lineArrayC[0],lineArrayC[1],lineArrayC[2],lineArrayC[3]))
            if i > endOfClin: #logistics
                lineArrayL = line.split(',')
                repo.logistics.insert(Logistic(lineArrayL[0],lineArrayL[1],lineArrayL[2],lineArrayL[3]))
    inputfile.close()

def ordersParser(inputFile, outputPath, repo):  # TODO: complete config_Parser
    with open(inputFile) as inputfile:
        for line in inputfile:
            lineArray = line.split(',')
            if len(lineArray) ==2:
                repo.send_Shipment(*lineArray)
            if len(lineArray) ==3:
                repo.receive_Shipment(*lineArray)
            # TODO:insert new line to the output file
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

    def receive_Shipment(self, name, amount, date):
        self.logistics.incCountRecived(amount,self.suppliers.getSupId(name))
        self.vaccines.insert(Vaccine( id,date, self.suppliers.getSupId(name), amount))

    def send_Shipment(self, location, amount):
        self.clinics.reduceDemend(amount,location)
        self.vaccines.removeAmount(amount)
        self.logistics.incCountSent(amount,self.clinics.getSupId(location))

    def action_log(self): # what is that ?


# DAO

class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        self._conn.execute("""
               INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?);""", [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])

    #recursive method if amount > from the id 1 demand than remove the demand from amount and check all again
    # if amount< the id 1 demand than remove the amount from the deman
    def removeAmount(self,amount):
        cursor = self._conn.execute("""
                      SELECT column_name =demand FROM vaccines """)
        curr_inventory = cursor.fetchone()[0]
        if curr_inventory<amount:
            self._conn.execute("""
                           DELETE from vaccine where id = 1 """)
            self.removeAmount(amount-curr_inventory)
        update_inventory = curr_inventory-amount
        self._conn.execute("""UPDATE vaccines SET demand = %d WHERE id=1 """ % update_inventory)
        self._conn.commit()



class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
                INSERT INTO suppliers (id, name, logistics) VALUES (?, ?, ?)
        """, [supplier.id, supplier.name, supplier.logistic])

    #get the suplier's name id
    def getSupId(self, name):
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

    # get the suplier's location id
    def getSupId(self,location):
        cursor = self._conn.execute("""
               SELECT logistics FROM clinics WHERE location=%d""" % location)
        return cursor.fetchone()[0]

    # reduce the amount from the location demand
    def reduceDemend(self ,amount, location):
        cursor = self._conn.execute("""
                       SELECT demand FROM clinics WHERE location=%d""" % location)
        self._conn.execute("""UPDATE clinucs SET demand = %s WHERE locatyin= %d """ % (cursor-amount, location))
        self._conn.commit()






class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, logistic):
        self._conn.execute("""
            INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?)
        """, [logistic.id, logistic.name, logistic.count_sent, logistic.count_received])

    #increase the count_recived/count_sent by amount
    def incCountRecived(self, amount, logId):
        cr = self._conn.execute('SELECT count_received FROM logistics WHERE id=%d' % logId)
        logistic_id_cr = cr.fetchone()[0]
        new_logistic_id_cr = logistic_id_cr + amount
        self._conn.execute("""UPDATE logistics SET count_received = %s WHERE id= %d """ %(new_logistic_id_cr,logId))
        self._conn.commit()
    def incCountSent(self, amount, logId):
        cr = self._conn.execute('SELECT count_sent FROM logistics WHERE id=%d' % logId)
        logistic_id_cs = cr.fetchone()[0]
        new_logistic_id_cs = logistic_id_cs + amount
        self._conn.execute("""UPDATE logistics SET count_sent = %s WHERE id= %d """ % (new_logistic_id_cs, logId))
        self._conn.commit()


