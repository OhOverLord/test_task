import tornado.web
import tornado.ioloop
import base64
import sqlite3


conn = sqlite3.connect('test.db')
c = conn.cursor()

try:
    c.execute('''CREATE TABLE request (key text, value text, duplicates int)''')
except sqlite3.OperationalError:
    pass

class addRequestHandler(tornado.web.RequestHandler):
    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        try:
            c.execute("insert into request values (?, ?, ?)", (base64.b64encode(f'{data["key"] + data["value"] }').encode('ascii'), data['value'], 0))
            conn.commit()
        except sqlite3.Error as error:
            self.write("Failed to insert record to the table")


class getValueRequestHandler(tornado.web.RequestHandler):
    def get(self):
        key = self.get_arguments('key')
        try:
            c.execute("select * from request where key=?", (key[0],))
            res = c.fetchall()
            c.execute("""
                SELECT value, COUNT(*) - 1 as duplicates
                FROM request
                WHERE value=?
                GROUP BY value
            """, (res[0][1],))
            duplicates = c.fetchone()[1]
            c.execute("""
                update request
                set duplicates=?
                where key=?
            """, (duplicates, key[0],))
            self.write(str(res))
            conn.commit()
        except sqlite3.Error as error:
            self.write("Failed to get record from the table")


class deleteValueRequestHandler(tornado.web.RequestHandler):
    def delete(self):
        key = self.get_arguments('key')
        try:
            c.execute(f"delete from request where key=?", (key[0],))
            conn.commit()
            self.write('Deleted successfuly')
        except sqlite3.Error as error:
            self.write("Failed to delete record from the table")


class updateValueRequestHandler(tornado.web.RequestHandler):
    def put(self):
        data = tornado.escape.json_decode(self.request.body)
        key = data['key']
        try:
            c.execute(f"update request set key=?, value=?, duplicates=? where key=?", (data['key'], data['value'], 0, key[0],))
            conn.commit()
            self.write('Updated successfuly')
        except sqlite3.Error as error:
            self.write("Failed to update record from the table")


class statisticRequestHandler(tornado.web.RequestHandler):
    def get(self):
        c.execute("""
            select sum(duplicates), COUNT(*), value
            from request
            group by value
        """)
        res = c.fetchall()
        resultat = []
        for el in res:
            resultat.append(int((el[0] / el[1]) * 100))
        self.write(str(sum(resultat)))

if __name__ == "__main__":
    app = tornado.web.Application([
        (r"/api/add", addRequestHandler),
        (r"/api/get", getValueRequestHandler),
        (r"/api/remove", deleteValueRequestHandler),
        (r"/api/update", updateValueRequestHandler),
        (r"/api/statistic", statisticRequestHandler),
    ])

    app.listen(8881)
    print("I'm listening on port 8881")
    tornado.ioloop.IOLoop.current().start()