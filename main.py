import sys
import sqlite3


def main(args):
    configPath = args[1]
    ordersPath = args[2]
    outputPath = args[3]
    configParser(configPath)
    repo = _Repository()
    repo.create_tables()

    repo._close();
    atexit.register(repo._close)  # TODO: figure out what is this line


if __name__ == '__main__':
    main(sys.argv)


def configParser(inputFile):  # TODO: complete config_Parser
    with open(inputFile) as inputfile:
        for line in inputfile:
            print("Fill me in")


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


# DAO

class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, student):
        self._conn.execute("""
               INSERT INTO students (id, name) VALUES (?, ?)
           """, [student.id, student.name])

    def find(self, student_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, name FROM students WHERE id = ?
        """, [student_id])

        return Student(*c.fetchone())


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, assignment):
        self._conn.execute("""
                INSERT INTO assignments (num, expected_output) VALUES (?, ?)
        """, [assignment.num, assignment.expected_output])

    def find(self, num):
        c = self._conn.cursor()
        c.execute("""
                SELECT num,expected_output FROM assignments WHERE num = ?
            """, [num])

        return Assignment(*c.fetchone())


class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, grade):
        self._conn.execute("""
            INSERT INTO grades (student_id, assignment_num, grade) VALUES (?, ?, ?)
        """, [grade.student_id, grade.assignment_num, grade.grade])

    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""
            SELECT student_id, assignment_num, grade FROM grades
        """).fetchall()

        return [Grade(*row) for row in all]

class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, grade):
        self._conn.execute("""
            INSERT INTO grades (student_id, assignment_num, grade) VALUES (?, ?, ?)
        """, [grade.student_id, grade.assignment_num, grade.grade])

    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""
            SELECT student_id, assignment_num, grade FROM grades
        """).fetchall()

        return [Grade(*row) for row in all]