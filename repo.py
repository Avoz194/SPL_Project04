import sqlite3
import atexit
import dto


# DAOs
class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        self._conn.execute("""INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?);""",
                           [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])

    def max_id(self):
        c = self._conn.cursor()
        c.execute("""SELECT MAX(id) FROM vaccines """)
        return c.fetchone()[0]

    def remove_amount(self, amount):
        amount_clone = amount
        index = 0
        size = self.size()
        c = self._conn.cursor()
        c.execute("""SELECT id,quantity FROM vaccines ORDER BY date """)
        while index != size and len(c) != 0 and index != self.max_id() + 1:
            curr_inventory = c.fetchone()[index]
            if amount_clone > curr_inventory[1]:
                c.execute("""DELETE from vaccine where id = curr_inventory[0] """)
                amount_clone = amount_clone - curr_inventory[1]
            else:
                update_inventory = curr_inventory - amount
                c.execute("""UPDATE vaccines SET quantity =? WHERE id=curr_inventory[0] """, update_inventory)
            index += 1
        c.commit()

    def total_inventory(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(quantity) FROM vaccines """)
        return c.fetchone()[0]


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""INSERT INTO suppliers (id, name, logistics) VALUES (?, ?, ?)
        """, [supplier.id, supplier.name, supplier.logistic])

    # get the logistics's id from suplier name
    def find(self, name):
        c = self._conn.cursor()
        c.execute("""SELECT logistics FROM suppliers WHERE name=?""", name)
        return dto.Supplier(*c.fetchone())


class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, clinic):
        self._conn.execute("""INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?,?)
        """, [clinic.id, clinic.location, clinic.demand, clinic.logistic])

    def find(self, location):
        c = self._conn.cursor()
        c.execute("""SELECT logistics FROM clinics WHERE location=?""", location)
        return dto.Clinic(*c.fetchone())

    # reduce the amount from the location demand
    def reduce_demand(self, amount, location):
        c = self._conn.cursor()
        c.execute("""SELECT demand FROM clinics WHERE location=?""", location)
        curr_inventory = c.fetchone()[0] - amount
        c.execute("""UPDATE clinics SET demand =? WHERE location=?""", curr_inventory, location)
        c.commit()

    def size(self):
        c = self._conn.cursor()
        c.execute("""SELECT COUNT(*) FROM clinics """)
        return c.fetchone()[0]

    def total_demand(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(demand) FROM clinics """)
        return c.fetchone()[0]


class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, logistic):
        self._conn.execute("""INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?)
        """, [logistic.id, logistic.name, logistic.count_sent, logistic.count_received])

    # increase the count_recived/count_sent by amount
    def inc_count_received(self, amount, logId):
        c = self._conn.cursor()
        c.execute("""SELECT count_received FROM logistics WHERE id=?""", logId)
        logistic_id_cr = c.fetchone()[0]
        new_logistic_id_cr = logistic_id_cr + amount
        c.execute("""UPDATE logistics SET count_received =? WHERE id=?""", new_logistic_id_cr, logId)
        c.commit()

    def inc_count_sent(self, amount, logId):
        c = self._conn.cursor()
        c.execute("""SELECT count_sent FROM logistics WHERE id=?""", logId)
        logistic_id_cs = c.fetchone()[0]
        new_logistic_id_cs = logistic_id_cs + amount
        c.execute("""UPDATE logistics SET count_sent =? WHERE id=?""", new_logistic_id_cs, logId)
        c.commit()

    def total_received(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(count_received) FROM logistics """)
        return c.fetchone()[0]

    def total_sent(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(count_sent) FROM logistics """)
        return c.fetchone()[0]


###############################
# The Repository
class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)

    def close(self):
        self._close()

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
        logId = self.suppliers.find(supname).id
        self.logistics.inc_count_received(amount, logId)
        self.vaccines.insert(dto.Vaccine(self.vaccines.maxId() + 1, date, logId, amount))

    def send_shipment(self, location, amount):
        logId = self.clinics.find(location).id
        self.clinics.reduce_demand(amount, location)
        self.vaccines.remove_amount(amount)
        self.logistics.inc_count_sent(amount, logId)

    def action_log(self):
        total_inven = self.vaccines.total_inventory()
        total_dema = self.clinics.total_demand()
        total_sent = self.suppliers.total_sent()
        total_rec = self.suppliers.total_received()
        return [total_inven, total_dema, total_sent, total_rec]


repo = _Repository()
atexit.register(repo.close)
