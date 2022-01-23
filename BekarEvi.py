import socket
import threading
import time
import queue
import uuid
import json
from sys import getsizeof
import geopy.distance

#En yakin peerleri bulma bug fix :: Kendini listeye eklemiyor
def closest_peer(lal,long,num,geoloc_fihrist):
    geo_dict = {}
    restrainer = int(num)
    for uuid,coor in geoloc_fihrist.items():
        if restrainer != 0:
            coor_div = coor.split(",")
            coords_1 = (float(lal), float(long))
            coords_2 = (float(coor_div[0]), float(coor_div[1]))
            distance = geopy.distance.geodesic(coords_1, coords_2).km
            if distance != 0:
                geo_dict[uuid] = distance
                restrainer -=1
        else:
            break

    dict(sorted(geo_dict.items(), key=lambda item: item[1]))
    return geo_dict

class Write_Thread(threading.Thread):
    def __init__(self, name, client_socket, client_address, client_queue):
        threading.Thread.__init__(self)
        self.name = name
        self.client_socket = client_socket
        self.client_address = client_address
        self.client_queue = client_queue

    def run(self):
        print(f"{self.name} starting.")
        self.client_socket.send(b'Welcome.\n> ')

        while True:
            response = self.client_queue.get()
            self.client_socket.send(self.format_message(response).encode())

            if (response == "BYE"):
                time.sleep(.2)
                self.client_socket.close()
                break

        print(f"{self.name} ending.")
    def format_message(self, data):
        return (data + "\n> ")


class Read_Thread(threading.Thread):
    def __init__(self, name, client_socket, client_address, client_queue, fihrist, users,uuids,uye,geoloc_fihrist):
        threading.Thread.__init__(self)
        self.name = name
        self.client_socket = client_socket
        self.client_address = client_address
        self.client_queue = client_queue
        self.fihrist = fihrist
        self.users = users
        self.uuids = uuids
        self.uye = uye
        self.geoloc_fihrist = geoloc_fihrist
        self.username = None
    def run(self):
        print(f"{self.name} starting.")
        uuid_id = uuid.uuid1()
        uuid_id = uuid_id.int
        while uuid_id in self.uuids:
            uuid_id = uuid.uuid1()
            uuid_id = uuid_id.int
        self.uuids.append(uuid_id)
        self.uuid_id = uuid_id
        #Baglanti kurulmasi
        responsehel = ("HE::" + str(uuid_id))
        self.client_queue.put(responsehel)
        while True:
            data = self.client_socket.recv(1024).decode().strip()        
            return_value = self.incoming_parser(data)

            if return_value == 1:
                break

        print(f"{self.name} ending.")
    def incoming_parser(self, data):
        f = open('market.json', 'r+')
        gelen = json.load(f)

        ret = 0
        if(data[:4] == "IG::"):
            input = data.split("::")
            uuid = input[1]
            if(int(self.uuid_id==int(uuid))):
                response = "OG::"+uuid
            else:
                #baglanti testi basarisiz uuid ler uyusmamakta
                response = "NG"
        elif (data[:4] == "RG::"):
            #2048 byte kurali
            input = data[4:]  
            if getsizeof(input) < 2048:   
                b = input.split("::")
                if len(b) == 6:
                    uuid_id = b[0]
                    if (int(uuid_id) == int(self.uuid_id)) and (uuid_id not in self.fihrist.keys()):
                        ipadres = b[1]
                        portno = b[2]
                        geoloc = b[3]
                        sys_type = b[4]
                        keyword = b[5]
                        key = uuid_id
                        values = ipadres + "::"+ portno + "::"+ geoloc + "::" + sys_type + "::" + keyword
                        self.fihrist[key] = values
                        self.users[key]=self.client_queue
                        self.username = key
                        response = "RO"
                    else:
                       response = "RN"     
                else:
                    response  = "ER"   
            else:
                response = "RN"
        elif(data[:4] == "SB::"):
            input = data[4:]
            if(self.username == None):
                response = "RN"
            else:
                if(input == "T"):
                    self.uye[self.username]="T"
                    response = "SO"
                elif(input=="F" and self.uye[self.username] == "T"):
                    self.uye.popitem(self.username)
                    response = "SO"
                else:
                    response = "RN"
        elif (data[:4] =="CS::"):
            #Bunu sorgulayan sistem kayitli mi kontrolu
            if  str(self.uuid_id) in self.fihrist.keys():
                response = "CO::BEGIN \n"
                input = data[4:]
                b = input.split("::")
                geolocinput = b[0]
                closestno = b[1]
                coordination = geolocinput.split(",")
                laltitude = coordination[0]
                longitude = coordination[1]
                sorted_uuids = closest_peer(laltitude,longitude,closestno,self.geoloc_fihrist)
                for uuid_idx,distance in sorted_uuids.items():      
                    response += "CO::" + str(uuid_idx) + "::" + self.fihrist.get(uuid_idx) + "\n"
                response += "CO::END"
            else:        
                response = "RN"
        elif (data[:4] == "OF::"):
            input = data[4:]
            b = input.split("::")
            NorK = b[0]
            i = 0
            y = ''
            if(NorK == "N"):
                num = int(b[1])
                totalarz = ''
                for i in gelen['offers']:
                    if(num == 0): break
                    num -=1
                    liste = list(i.values())
                    totalarz += "\nOO"
                    for y in liste:
                        totalarz = totalarz+ "::" + str(y)
                    totalarz += "\nOO::END"

                response = totalarz
            elif(NorK == "K"):
                keywords = b[2]
            else:
                response = "RN"
        elif (data[:4] == "DM::"):
            input = data[4:]
            b = input.split("::")
            NorK = b[0]
            i = 0
            y = ''
            if(NorK == "N"):
                num = int(b[1])
                totalarz = ''
                for i in gelen['demands']:
                    if(num == 0): break
                    num -=1
                    liste = list(i.values())
                    totalarz += "\nOO"
                    for y in liste:
                        totalarz = totalarz+ "::" + str(y)
                    totalarz += "\nOO::END"

                response = totalarz
            elif(NorK == "K"):
                keywords = b[2]
            else:
                response = "RN"
        elif (data[:4] == "AT::"):
            if (self.username == list(self.fihrist)[0]):
                totalarz = ''
                totalarz += "\nUB::BEGIN"
                for i in gelen['offers']:
                    liste = list(i.values())
                    totalarz += "\nUB::A"
                    for y in liste:
                        totalarz = totalarz+ "::" + str(y)
                totalarz += "\nUB::END"

                totalarz += "\nUB::BEGIN"
                for i in gelen['demands']:
                    liste = list(i.values())

                    totalarz += "\nUB::T"
                    for y in liste:
                        totalarz = totalarz+ "::" + str(y)
                totalarz += "\nUB::END"
                for target_user in self.users.keys():
                    self.users[target_user].put(f"{totalarz}\n")
                response = "OK"
            else:
                response ="RN"

        elif (data[:4] == "TR::"):
            if (self.username == None and self.uye[self.username]=="T"):
                response = "RN"
            else:
                splitted_data = data[4:].split("::")
                print(splitted_data)
                if(len(splitted_data) == 5):
                    uid = splitted_data[1]
                    name = splitted_data[2]
                    unit = splitted_data[3]
                    payment = int(splitted_data[4])
                    response = "RN"
                    cumle = ""
                    if splitted_data[0] == "O":
                        #Transaction offer
                        for i in gelen['offers']:
                            liste = list(i.values())
                            if ((uid or name or unit) in liste) and int(payment) >= int(liste[-1]):
                                i['quantity'] -= 1
                                liste[3]-=1
                                json.dump(gelen, f, indent = 2)
                                for target_user in self.users.keys():
                                    for eleman in liste:
                                        cumle+="::"+eleman
                                    self.users[target_user].put(f"NW{cumle}\n")
                                response = "TO"   
                       #Transaction demand              
                    elif splitted_data[0] == "D":
                        for i in gelen['demands']:
                            liste = list(i.values())
                            if ((uid or name or unit) in liste) and int(payment) >= int(liste[-1]): 
                                response = "TO"
                                i['quantitiy'] += 1
                                json.dump(gelen, f, indent = 2)
                                for target_user in self.users.keys():
                                    for eleman in liste:
                                        cumle+="::"+eleman
                                    self.users[target_user].put(f"NW{cumle}\n")

                    else:
                        response = "TN"
                else:
                    response = "RN"
        elif data[:4] == "MS::":
            if (self.username == None):
                response = "RN"
            else:
                message = data[4:]       
                target_user = list(self.fihrist)[0]
                print(target_user)
                response = "MO"
                self.users[target_user].put(f"MO::{self.username}:{message}")

    

        elif (data == "QU"):
            response = "BY"
            ret = 1
        else:
            response = "ER"

        self.client_queue.put(response)
        
        return ret

def main():
    port = 12349
    host = "localhost"
    thread_counter = 0

    listener_socket = socket.socket()
    listener_socket.bind((host, port))

    listener_socket.listen(0)

    fihrist = {}
    users = {}
    uye = {}
    uuids = []
    geoloc_fihrist = {}    
 
    print("Server is starting.")
    while True:
        client_socket, client_address = listener_socket.accept()
        print("A new client has connected: ", client_address)
        message_queue = queue.Queue()

        write_thread = Write_Thread("WriteThread-" + str(thread_counter), client_socket, client_address, message_queue)
        read_thread = Read_Thread("ReadThread-" + str(thread_counter), client_socket, client_address, message_queue, fihrist, users,uuids, uye,geoloc_fihrist)
        read_thread.start()
        write_thread.start()
        print(users)
        thread_counter += 1

    print("Server has closed.")

if __name__ == "__main__":
    main()