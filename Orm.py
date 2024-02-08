import psycopg2

def check_dc(columns, data):
    if len(columns) == len(data):
        return
    if len(columns) > len(data) or len(columns) < len(data):
        raise ValueError("Error in columns setter")

class Orm:
    def __init__(self,database,password,table_name, columns, data):
        self.con = psycopg2.connect(database='postgres', user='postgres', password="postgres", host="127.0.0.1", port="5432")
        self.table_name = table_name
        self.columns = columns
        self.data = data

        try:
            check_dc(self.columns, self.data)
        except ValueError:
            print("Error in columns setter")
            exit()
        else:
            print('Database opened successfully')

    def create_table(self):
        columns = list(zip(self.columns, self.data))
        print(columns)
        sql = f"CREATE TABLE IF NOT EXISTS {self.table_name}("
        for i, (column, datatype) in enumerate(columns):
            print((column, datatype))
            print(i)
            if i !=  0:
                sql += ", "
            sql += f"{column} {datatype}"
        sql += ");"
        print(sql)
        self.cur = self.con.cursor()
        self.cur.execute(sql)
        self.con.commit()
    def insert(self,table_name,columns,value):
        check_dc(columns, value)
        placeholders = ', '.join(['%s'] * len(value))
        print(value)
        sql = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
        self.cur.execute(sql, value)
        self.con.commit()

    def retrieving(self,table_name,columns):
        sql = f"SELECT {','.join(columns)} FROM {table_name}"
        print(sql)
        self.cur.execute(sql)
        self.rows = self.cur.fetchall()

    def update(self, table_name, columns, values,condition):
        check_dc(columns, values)
        updates = ', '.join([f"{column} = %s" for column in columns])
        sql = f"UPDATE {table_name} SET {updates} WHERE {condition};"
        print(sql)
        self.cur.execute(sql, values)
        self.con.commit()
    def delete(self,table_name,condition):
        sql = f"DELETE FROM {table_name} WHERE {condition};"
        self.cur.execute(sql)
        self.con.commit()
a = Orm("postgres", "postgres", 'persons', ['first_name', 'last_name'], ['VARCHAR(255)', 'INT'])
a.create_table()
a.insert('persons',['first_name', 'last_name'],['armin',12])
a.retrieving('persons', ['first_name', 'last_name'])
rows = a.rows
for row in rows:
    print('firstname =',row[0])
    print('lastname =',row[1])
a.update("persons", ['first_name', 'last_name'], ['armin', 12],'last_name = 13')
# a.delete('persons','last_name = 12')