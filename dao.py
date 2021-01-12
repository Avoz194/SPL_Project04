import dto
class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        self._conn.execute("""INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?);""", [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])


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
        while index != size and len(c) != 0 and index != self.max_id()+1 :
            curr_inventory = c.fetchone()[index]
            if amount_clone>curr_inventory[1]:
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
    def getLogId(self, name):
        c = self._conn.cursor()
        c.execute("""SELECT logistics FROM suppliers WHERE name=?""", name)
        return c.fetchone()[0]



class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, clinic):
        self._conn.execute("""INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?,?)
        """, [clinic.id, clinic.location, clinic.demand, clinic.logistic])

    def getLogId(self, location):
        c = self._conn.cursor()
        c.execute("""SELECT logistics FROM clinics WHERE location=?""", location)
        return c.fetchone()[0]

    # reduce the amount from the location demand
    def reduceDemend(self ,amount, location):
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

    def getLogId(self, name):
        c = self._conn.cursor()
        c.execute("""SELECT id FROM logistics WHERE name=?""", name)
        return c.fetchone()[0]

    #increase the count_recived/count_sent by amount
    def incCountRecived(self, amount, logId):
        c = self._conn.cursor()
        c.execute("""SELECT count_received FROM logistics WHERE id=?""",  logId)
        logistic_id_cr = c.fetchone()[0]
        new_logistic_id_cr = logistic_id_cr + amount
        c.execute("""UPDATE logistics SET count_received =? WHERE id=?""", new_logistic_id_cr, logId)
        c.commit()

    def incCountSent(self, amount, logId):
        c = self._conn.cursor()
        c.execute("""SELECT count_sent FROM logistics WHERE id=?""", logId)
        logistic_id_cs = c.fetchone()[0]
        new_logistic_id_cs = logistic_id_cs + amount
        c.execute("""UPDATE logistics SET count_sent =? WHERE id=?""", new_logistic_id_cs, logId)
        c.commit()

    def total_recevied(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(count_received) FROM logistics """)
        return c.fetchone()[0]

    def total_sent(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM(count_sent) FROM logistics """)
        return c.fetchone()[0]
