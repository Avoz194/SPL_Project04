import dto
class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        self._conn.execute("""
               INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?);""", [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])


    def max_id(self):
        cursor = self._conn.execute("""
                              SELECT MAX(id) FROM vaccines """)
        return cursor.fetchone()[0]

    def remove_amount(self, amount):
        amount_clone=amount
        index = 0
        size = self.size()
        cursor = self._conn.execute("""
                      SELECT id,quantity FROM vaccines ORDER BY date """)
        while index != size and len(cursor)!=0 and index!= self.max_id()+1 :
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
        self._conn.execute("""UPDATE clinics SET demand = %s WHERE location= %d """ % (curr_inventory, location))
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
        logId = self._conn.execute("""SELECT id FROM logistics WHERE name=%d""" % name)
        return logId.fetchone()[0]

    #increase the count_recived/count_sent by amount
    def incCountRecived(self, amount, logId):
        cr = self._conn.execute("""SELECT count_received FROM logistics WHERE id=%d""" % logId)
        logistic_id_cr = cr.fetchone()[0]
        new_logistic_id_cr = logistic_id_cr + amount
        self._conn.execute("""UPDATE logistics SET count_received = %s WHERE id= %d """ %(new_logistic_id_cr, logId))
        self._conn.commit()

    def incCountSent(self, amount, logId):
        cr = self._conn.execute("""SELECT count_sent FROM logistics WHERE id=%d""" % logId)
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
