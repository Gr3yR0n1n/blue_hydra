import json
import socket
import threading
import requests
import pymysql.cursors
import MySQLdb

class ThreadedServer(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.listenToClient,args = (client,address)).start()

    def listenToClient(self, client, address):
        size = 1024
        while True:
            try:
				data = client.recv(size)
				sql = ""
				if data:
					# Set the response to echo back the recieved data
					response = data

					file = open("./pulse.log","a")
					file.write(response)
					file.close()

					print response

					r = json.loads(response)

					# Connect to the database
					connection = pymysql.connect(
							host='localhost',
							user='<user>', password='<password>',
							db='pulse',
							charset='utf8mb4',
							cursorclass=pymysql.cursors.DictCursor
					)

					try:

						data = MySQLdb.escape_string(response)
						rtype = -1
						try: rtype = r['type']
						except: rtype = -1

						source = -1
						try: source= r['source']
						except: source = -1

						version = -1
						try: version = r['version']
						except: version = -1

						sync_id = -1
						try: sync_id = r['data']['sync_id']
						except: sync_id = -1

						status = -1
						try: status = r['data']['status']
						except: status = -1

						sync_version = -1
						try: sync_version = r['data']['sync_version']
						except: sync_version = -1

						le_company_data = -1
						try: le_company_data = r['data']['le_company_data']
						except: le_company_data = -1

						company = -1
						try: company = r['data']['company']
						except: company = -1

						address = -1
						try: address = r['data']['address']
						except: address = -1

						vendor = -1
						try: vendor = r['data']['vendor']
						except: vendor = -1

						company_type = -1
						try: company_type = r['data']['company_type']
						except: company_type = -1

						classic_mode = 0
						try: classic_mode = r['data']['classic_mode']
						except: classic_mode = 0

						le_mode = 0
						try: le_mode = r['data']['le_mode']
						except: le_mode = 0

						le_address_type = -1
						try: le_address_type = r['data']['le_address_type']
						except: le_address_type = -1

						last_seen = -1
						try: last_seen = r['data']['last_seen']
						except: last_seen = -1

						le_flags = -1
						try: le_flags = r['data']['le_flags']
						except: le_flags = -1

						le_rssi = -1
						try: le_rssi = r['data']['le_rssi']
						except: le_rssi = -1

						with connection.cursor() as cursor:
							# Create a new record
							sql = "INSERT INTO log ("\
								"type, "\
								"source, "\
								"version, "\
								"sync_id, "\
								"status, "\
								"sync_version, "\
								"le_company_data, "\
								"company, "\
								"address, "\
								"vendor, "\
								"company_type, "\
								"classic_mode, "\
								"le_mode, "\
								"le_address_type, "\
								"last_seen, "\
								"le_flags, "\
								"le_rssi "\
								") VALUES ("\
								"\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%d,%d,\"%s\",%d,\"%s\",\"%s\""\
								");" % ( \
								rtype, \
								source, \
								version, \
								sync_id, \
								status, \
								sync_version, \
								le_company_data, \
								company, \
								address, \
								vendor, \
								company_type, \
								classic_mode, \
								le_mode, \
								le_address_type, \
								last_seen, \
								le_flags, \
								le_rssi \
								)

							print sql
							cursor.execute(sql)

						# connection is not autocommit by default. So you must commit to save
						# your changes.
						connection.commit()
					except Exception as inst:
						file = open("./error.log","a")
						file.write(r"------------------ Error ---------------------")
						file.write(inst)
						file.write("")
						file.close()

						print("------------------ Error ---------------------")
						print(inst)

					finally:
						connection.close()


				else:
					raise error('Client disconnected')
            except:
                client.close()
                return False

if __name__ == "__main__":
    while True:
        port_num = input("Port? ")
        try:
            port_num = int(port_num)
            break
        except ValueError:
            pass

    ThreadedServer('',port_num).listen()

