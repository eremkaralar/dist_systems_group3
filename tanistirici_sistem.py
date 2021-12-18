import socket
import threading
import time
import queue
import uuid
from sys import getsizeof

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

            if (response == "BY"):
                time.sleep(.2)
                self.client_socket.close()
                break

        print(f"{self.name} ending.")
    def format_message(self, data):
        return (data + "\n> ")


class Read_Thread(threading.Thread):
    def __init__(self, name, client_socket, client_address, client_queue, fihrist,uuids):
        threading.Thread.__init__(self)
        self.name = name
        self.client_socket = client_socket
        self.client_address = client_address
        self.client_queue = client_queue
        self.fihrist = fihrist
        self.uuid_id = None
        self.uuids = uuids
    def run(self):
        print(f"{self.name} starting.")
        #UUID olusturma ve int seklinde kullanma
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
        ret = 0
        #PING-PONG
        #baglanti kontrolu
        if(data[:4] == "IG::"):
            input = data.split("::")
            uuid = input[1]
            if(int(self.uuid_id==int(uuid))):
                response = "OG::"+uuid
            else:
                #baglanti testi basarisiz uuid ler uyusmamakta
                response = "NG"

        else:
            response = "ER"

        self.client_queue.put(response)
        
        return ret

def main():
    port = 12345
    host = "0.0.0.0"
    thread_counter = 0


    listener_socket = socket.socket()
    listener_socket.bind((host, port))

    listener_socket.listen(0)

    fihrist = {}
    uuids = []

 
    print("Server is starting.")
    while True:
        client_socket, client_address = listener_socket.accept()
        print("A new client has connected: ", client_address)
        message_queue = queue.Queue()

        write_thread = Write_Thread("WriteThread-" + str(thread_counter), client_socket, client_address, message_queue)
        read_thread = Read_Thread("ReadThread-" + str(thread_counter), client_socket, client_address, message_queue, fihrist,uuids)

        read_thread.start()
        write_thread.start()
        thread_counter += 1

    print("Server has closed.")

if __name__ == "__main__":
    main()
