import socket
import threading
import sys
import os
import subprocess
# sys.setrecursionlimit(200000)
HEADER = 1000
PORT = 5052
HEARTBEAT_PORT = 5053
# PORT2 = 5051
# SERVER2 = '192.168.29.241'
SERVER = socket.gethostbyname(socket.gethostname())
# SERVER = '192.168.197.1'
# print(SERVER)
ADDR = (SERVER, PORT)  # Binding server and port
ADDR_HEARTBEAT = (SERVER, HEARTBEAT_PORT)
# ADDR2 = (SERVER, PORT2)
DISCONNECT_MSG = "DISCONECTED!"
broker = socket.socket(
    socket.AF_INET, socket.SOCK_STREAM)  # selct what socket you want and select stream for streaming data \

broker.bind(ADDR)  # WE HAVE BOUND THE SOCKET TO THE ADDRESS

# broker_consumer = socket.socket(
#     socket.AF_INET, socket.SOCK_STREAM)
# broker_consumer.bind(ADDR2)

no_of_partition = 3
no_offset = 3
topicName = []  # list of topics

producers = []
consumers = []

# ______________________________________________________________________________________________________________________________________________
# Handels the connection sent by prod and cosnumer


def handel_connection(conn, addr):
    print(f"NEW connnection {addr} connected")
    connected = True
    while connected:
        # first message is always 64 bytes to tell how long the actual message is
        identify_length = conn.recv(HEADER).decode('utf-8')
        # identify_length = int(identify_length)
        identify = conn.recv(1).decode('utf-8')
        if identify == "p":  # TO CHECK IF IT IS PRODUCER
            msg_length = conn.recv(HEADER).decode('utf-8')
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode('utf-8')

            top_length = conn.recv(HEADER).decode('utf-8')
            top_length = int(top_length)
            top = conn.recv(top_length).decode('utf-8')
            producers.append(addr)

            # print(
            #     f'message is : {msg};\nmessage length = {msg_length};\nopic is : {top};\nopic length = {top_length} ')
            if top not in topicName:  # CHECKS IF TOPIC ALREADY EXISTS
                topicName.append(top)  # APPENDS TO TOPIC LIST
                parent = r'D:\BD_final_project\topics'
                direct = f'{top}'
                mode = 0o666
                path = os.path.join(parent, direct)
                os.mkdir(path, mode=mode)
                parent = path
                for i in range(no_of_partition):
                    direct = f'{i}'
                    path = os.path.join(parent, direct)
                    os.mkdir(path, mode=mode)
                with open(f"D:\BD_final_project\Topics\{top}\{0}\{0}.txt", "w") as f:

                    f.write(f"{msg}\n")
                f.close
                # brokerComm = threading.Thread(target=send_metadata)
                # brokerComm.start()

            else:
                parent = r'D:\BD_final_project\topics'
                direct = f'{top}'

                count = 0
                i = 0
                while i < no_of_partition:
                    pathf = os.path.join(parent, direct)
                    count = 0
                    part = f'{i}'
                    pathf = os.path.join(pathf, part)
                    for p in os.listdir(pathf):
                        if os.path.isfile(os.path.join(pathf, p)):
                            count += 1
                    if count == no_offset:
                        i += 1
                    else:
                        newoff = f"{count}.txt"
                        pathf = os.path.join(pathf, newoff)
                        with open(pathf, "a") as f:
                            # f.write("This text is written with Python.")
                            # APPENDS TP EXISTING FILE
                            f.write(f"{msg}")
                        f.close()
                        i = no_of_partition + 2
                # f.write(f"{msg}\n")
            # print(
                # f"topicName list is : {topicName};\ntopic dict : {topic_dict}")
        elif identify == "c":  # IDENTIFIES THE CONSUMER
            top_length = conn.recv(HEADER).decode('utf-8')
            top_length = int(top_length)
            top = conn.recv(top_length).decode('utf-8')
            parent = r'D:\BD_final_project\topics'

            direct = f'{top}'
            # READS FILE AND SENDS BACK TO CONSUMER
            count = 0
            partition = 0
            while partition < no_of_partition:
                pathf = os.path.join(parent, direct)
                count = 0
                part = f'{partition}'
                pathf = os.path.join(pathf, part)
                for p in os.listdir(pathf):
                    # if os.path.isfile(os.path.join(pathf, p)):
                    readpath = os.path.join(pathf, p)
                    with open(readpath, "r+") as f:
                        # f.write("This text is written with Python.")
                        for msg in f.readlines():
                            l, m = send_encoding_msg(msg)
                            conn.send(l)
                            conn.send(m)
                partition += 1

            consumers.append(addr)
        elif identify == 'b':
            top_length = conn.recv(HEADER).decode('utf-8')
            top_length = int(top_length)
            top = conn.recv(top_length).decode('utf-8')
            topicName.append(top)
            print(topicName)
            # brokerComm = threading.Thread(target=send_metadata)
            # brokerComm.start()
            connected = False

    conn.close()

# ______________________________________________________________________________________________________________________________________________
# MESSAGE ENCODING


def send_encoding_msg(msg):
    message = msg.encode('utf-8')  # we encode the message
    msg_length = len(message)  # we get length of the message
    # firsst message should always be length
    send_length = str(msg_length).encode('utf-8')
    # therefre we pad it to legth of 64
    send_length += b' '*(HEADER-len(send_length))
    return send_length, message


    # producers.send(send_length)
    # producers.send(message)
# _________________________________________________________________________________________________________________________________
# HEART BEAT CODE
heart_beat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
heart_beat.connect(ADDR_HEARTBEAT)


def send_heartbeat():
    b = 'b'
    b_length, b = send_encoding_msg(b)
    heart_beat.send(b_length)
    heart_beat.send(b)
    while True:
        b_length, beat_msg = send_encoding_msg(f'{PORT}')
        heart_beat.send(b_length)
        heart_beat.send(beat_msg)

    # ______________________________________________________________________________________________________________________________________________
# START BROKER FUNCTION




def start_broker():
    broker.listen()
    print(f"broker is listening .... on :{ADDR}")
    # broker_consumer.listen()
    # heartBeat = threading.Thread(target=send_heartbeat, args=())
    # heartBeat.start()
    while True:
        # b_length, beat = send_encoding_msg(beat)
        # heart_beat.send(b_length)
        # heart_beat.send(beat)
        conn, addr = broker.accept()  # stores address of where connection came
        # create threads so that it exec each thread without completing the prev thread
        # conn2, addr2 = broker_consumer.accept()

        thread = threading.Thread(target=handel_connection, args=(conn, addr))
        thread.start()
        # thread2 = threading.Thread(target=handel_consumer, args=(conn2, addr))
        # thread2.start()
        # show number of active connection to server
        print(f"active connections : {threading.activeCount()-1}")


# ______________________________________________________________________________________________________________________________________________
# STARTING EVERYTHING
print("**** broker is starting ****")

brokerstart = threading.Thread(target=start_broker, args=())
brokerstart.start()
# while True:
#     # print('t')
#     heart_beat.send('.'.encode('utf-8'))

# beat = 'true'
# b_length, beat = send_encoding_msg(beat)
# heart_beat.send(b_length)
# heart_beat.send(beat)


# start_broker()

beat = 'ping'
heartBeat = threading.Thread(target=send_heartbeat)
heartBeat.start()
