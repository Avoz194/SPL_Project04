import dao
import dto
import sqlite3

# The Repository
class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = dao._Vaccines(self._conn)
        self.suppliers = dao._Suppliers(self._conn)
        self.clinics = dao._Clinics(self._conn)
        self.logistics = dao._Logistics(self._conn)

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

    def receive_shipment(self, supname, amount, date):
        logId = self.suppliers.getLogId(supname)
        self.logistics.incCountRecived(amount, logId)
        self.vaccines.insert(dto.Vaccine(self.vaccines.maxId() + 1, date, logId, amount))

    def send_shipment(self, location, amount):
        logId = self.clinics.getLogId(location)
        self.clinics.reduceDemend(amount, location)
        self.vaccines.remove_amount(amount)
        self.logistics.incCountSent(amount, logId)

    def action_log(self):
        totalInven = self.vaccines.total_inventory()
        totalDema = self.clinics.total_demand()
        totalRec = self.suppliers.total_recevied()
        totalSent = self.suppliers.total_sent()
        return [totalInven, totalDema, totalSent, totalRec]
