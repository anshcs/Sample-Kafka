import socket
import threading
from time import sleep
import subprocess

HEADER = 1000
PORT = 5053
# PORT2 = 5051
# SERVER2 = '192.168.29.241'
SERVER = socket.gethostbyname(socket.gethostname())
# SERVER = '192.168.29.241'
# print(SERVER)
ADDR = (SERVER, PORT)  # Binding server and port
# ADDR2 = (SERVER, PORT2)
DISCONNECT_MSG = "DISCONECTED!"
zookeeper = socket.socket(
    socket.AF_INET, socket.SOCK_STREAM)  # selct what socket you want and select stream for streaming data \

zookeeper.bind(ADDR)  # WE HAVE BOUND THE SOCKET TO THE ADDRESS
brokers = []
broker_dict = {}
socket_dict = {}
brok_addr = []
producers = []
consumers = []

# -----------------------------------------------------------------------------------------------------------------------------------------
# handels heart beat from brokers of leader


def recv_heartbeat(conn, addr):
    # heartbeat_length = conn.recv(HEADER).decode('utf-8')
    # heartbeat_length = int(heartbeat_length)
    connected = True

    print('recv broker')

    while connected:
        try:
            sleep(5)
            port_length = conn.recv(HEADER).decode('utf-8')
            port_length = int(port_length)
            address = conn.recv(port_length).decode('utf-8')
            if address not in brok_addr:
                brok_addr.append(address)
            print(f'brokers : {brok_addr}; leader:{address}')
        except:
            print('leader algo takng place')
            leader_algo()
            break

        # print(f'brokers : {brok_addr}; leader:{address}')

    # handel_consumer()
    print('disconected')
    # brokers.pop(brokers[0])

    # print(f"disconnected prot : {addr}")

# -----------------------------------------------------------------------------------------------------------------------------------------
# encodes message for sending


def send_encoding_msg(msg):
    message = msg.encode('utf-8')  # we encode the message
    msg_length = len(message)  # we get length of the message
    # firsst message should always be length
    send_length = str(msg_length).encode('utf-8')
    # therefre we pad it to legth of 64
    send_length += b' '*(HEADER-len(send_length))
    return send_length, message
# -----------------------------------------------------------------------------------------------------------------------------------------
# START BROKER FUNCTION

    # def start_otherbroker():
    #     nstart = True
    #     while nstart:
    #         try:
    #             subprocess.call('python broker.py',
    #                             creationflags=subprocess.CREATE_NEW_CONSOLE)
    #             nstart = False
    #         except:
    #             try:
    #                 subprocess.call('python broker2.py',
    #                                 creationflags=subprocess.CREATE_NEW_CONSOLE)
    #                 nstart = False
    #             except:
    #                 try:
    #                     subprocess.call('python broker2.py',
    #                                     creationflags=subprocess.CREATE_NEW_CONSOLE)
    #                     nstart = False
    #                 except:
    #                     pass

# -----------------------------------------------------------------------------------------------------------------------------------------
# leader algo which pop from list of brokers and appoints new broker


def leader_algo():
    brokers.pop(0)
    leader = brokers[0]
    # socket_dict.get(leader)
    port = brok_addr.pop(0)
    port = int(port)
    thread = threading.Thread(
        target=recv_heartbeat, args=(socket_dict.get(leader), leader))
    thread.start()
    # starBroke = threading.Thread(target=start_otherbroker)
    # starBroke.start()
    nstart = True
    print(port, type(port))
    while nstart:
        
        if port == 5050:
            try:

                subprocess.call('python broker.py',
                            creationflags=subprocess.CREATE_NEW_CONSOLE)
                nstart = False
            except:
                pass

        elif port == 5051:
            try:

                subprocess.call('python broker2.py',
                                creationflags=subprocess.CREATE_NEW_CONSOLE)
                nstart = False
            except:
                pass
        elif port == 5052:
            try:

                subprocess.call('python broker3.py',
                                creationflags=subprocess.CREATE_NEW_CONSOLE)
                nstart = False
            except:
                pass

    handel_prod()
    handel_consumer()


# ______________________________________________________________________________________________________________________________________________
# hadels heart beat from broker if drop is true then executes the message passsing to prod to change prot to new server


def handel_prod():
    for i in producers:
        l, m = send_encoding_msg(f'{brok_addr[0]}')
        i.send(l)
        i.send(m)

# -----------------------------------------------------------------------------------------------------------------------------------------
# hadels heart beat from broker if drop is true then executes the message passsing to consumer to change prot to new server


def handel_consumer():
    for i in consumers:
        l, m = send_encoding_msg(f'{brok_addr[0]}')
        i.send(l)
        i.send(m)


def start():
    zookeeper.listen()
    print(f"zookeeper is listening .... on :{ADDR}")
    # broker_consumer.listen()
    while True:
        conn, addr = zookeeper.accept()  # stores address of where connection came
        # create threads so that it exec each thread without completing the prev thread
        # conn2, addr2 = broker_consumer.accept()
        # thread = threading.Thread(target=recv_heartbeat, args=(conn, addr))
        # thread.start()
        # thread2 = threading.Thread(target=handel_consumer, args=(conn2, addr))
        # thread2.start()
        # show number of active connection to server
        identify_length = conn.recv(HEADER).decode('utf-8')
        identify_length = int(identify_length)
        identify = conn.recv(identify_length).decode('utf-8')
        if identify == 'b':
            if addr not in brokers:
                brokers.append(addr)
                socket_dict[addr] = conn
            leader = brokers[0]

            if(addr == leader):
                broker_heartbeat = threading.Thread(
                    target=recv_heartbeat, args=(conn, leader))
                broker_heartbeat.start()
            else:
                try:
                    port_length = conn.recv(HEADER).decode('utf-8')
                    port_length = int(port_length)
                    address = conn.recv(port_length).decode('utf-8')
                    brok_addr.append(address)
                except:
                    pass

        elif identify == 'p':
            # prod_heartbeat = threading.Thread(
            #     target=handel_prod, args=(conn, addr))
            # prod_heartbeat.start()
            producers.append(conn)
        elif identify == 'c':
            # consumer_heartbeat = threading.Thread(
            #     target=handel_consumer, args=(conn, addr))
            # consumer_heartbeat.start()
            consumers.append(conn)

        print(f"active connections : {threading.activeCount()-1}")
        print(
            f'producers and consumers and broekrs : {producers};{brokers}')
        # print(f'brokers : {brokers}; leader:{leader}')


start()
