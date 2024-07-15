import cx_Oracle

class OracleDB:
    def __init__(self):
        self.user = "NEXUS_GIS"
        self.password = "nexus_gis"
        self.host = "ltronexusbdqa01.pro.edenor"
        self.port = 1530
        self.service = "GISQA02"
        self.dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service)
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = cx_Oracle.connect(self.user, self.password, self.dsn)

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, params=None):
        cursor = self.connection.cursor()
        cursor.execute(query, params or {})
        columns = [col[0].lower() for col in cursor.description]  # Convertir a minúsculas
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]
