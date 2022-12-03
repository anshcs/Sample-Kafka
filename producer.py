import socket
import websocket
import json
import threading
from time import sleep
# PROD_PORT = 5054
HEADER_MSG = 1000
HEADER_TOPIC = 1000
PORT = 5050
# PORT2 = 5051
# PORT3 = 5052
# SERVER = '192.168.29.240'
SERVER = socket.gethostbyname(socket.gethostname())
# print(SERVER)
ADDR = (SERVER, PORT)  # Binding server and port
DISCONNECT_MSG = "DISCONECTED!"
HEARTBEAT_PORT = 5053
ADDR_HEARTBEAT = (SERVER, HEARTBEAT_PORT)
# ADDR2 = (SERVER, PORT2)
# ADDR3 = (SERVER, PORT3)
# no__of_prod = 0
# producers = socket.socket(
#     socket.AF_INET, socket.SOCK_STREAM)

# producers.connect(ADDR)

producer = {}  # keeps track of all producers

# -----------------------------------------------------------------------------------------------------------------------------
# function to create producer


def create_prod(address):
    connected = False
    prod = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while not connected:
        try:
            prod.connect(address)
            connected = True
        except socket.error:
            sleep(3)
            prod.connect(NEWADDR)
            connected = True

    return prod
# -----------------------------------------------------------------------------------------------------------------------------
# sends messages to broker


def send_message(topic, m, my_prod):

    topic_length, topic_msg = send_encoding_msg(topic)
    message_length, message = send_encoding_topic(m)
    p = 'p'

    p_length, p = send_encoding_msg(p)

    # print(topic_length, topic_msg, message_length, message)
    try:
        my_prod.send(p_length)
        my_prod.send(p)
        my_prod.send(message_length)
        my_prod.send(message)
        my_prod.send(topic_length)
        my_prod.send(topic_msg)
    except:
        change_port(topic, m)
# -----------------------------------------------------------------------------------------------------------------------------
# sends messages encoding


def send_encoding_msg(msg):
    message = msg.encode('utf-8')  # we encode the message
    msg_length = len(message)  # we get length of the message
    # firsst message should always be length
    send_length = str(msg_length).encode('utf-8')
    # therefre we pad it to legth of 64
    send_length += b' '*(HEADER_MSG-len(send_length))
    return send_length, message
    # producers.send(send_length)
    # producers.send(message)


def send_encoding_topic(topic):
    message = topic.encode('utf-8')  # we encode the message
    msg_length = len(message)  # we get length of the message
    # firsst message should always be length
    send_length = str(msg_length).encode('utf-8')
    # therefre we pad it to legth of 64
    send_length += b' '*(HEADER_TOPIC-len(send_length))
    return send_length, message

# -----------------------------------------------------------------------------------------------------------------------------
# change port connection


def change_port(topic, m):

    prod = create_prod(NEWADDR)
    producer[topic] = prod
    print(f'changed connections to new addr : {NEWPORT}; {producer}')
    print(f'producer dict : {producer}')

    send_message(topic, m, prod)
    # print(f'changed connections to new addr : {NEWPORT}')


# -----------------------------------------------------------------------------------------------------------------------------
# sends heartbeat to zookeeper
heart_beat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
heart_beat.connect(ADDR_HEARTBEAT)


def send_heartbeat():
    p = 'p'

    p_length, p = send_encoding_msg(p)
    # try:
    #     heart_beat.send(p_length)
    # except:
    heart_beat.send(p_length)
    heart_beat.send(p)
    while True:
        # beat = 'true'
        # beat.encode('utf-8')
        # heart_beat.send(beat)
        beat = 'ping'
        b_length, beat_msg = send_encoding_msg(beat)
        heart_beat.send(b_length)
        try:
            val_length = heart_beat.recv(HEADER_MSG).decode('utf-8')
            val_length = int(val_length)
            port = heart_beat.recv(val_length).decode('utf-8')

            global NEWPORT
            NEWPORT = int(port)
            global NEWADDR
            NEWADDR = (SERVER, NEWPORT)

            print(f'port has to be changed to{NEWPORT}; type: {type(NEWPORT)}')

            # change_port(port)
        except:
            pass
        # reciveport = threading.Thread(target=recive_port)
        # reciveport.start()


# --------------------------------------------------------------------------------------------------------------------
# start heart beat to zookeepr
heartBeat = threading.Thread(target=send_heartbeat)
heartBeat.start()
# --------------------------------------------------------------------------------------------------------------------
# reciving msage from user and sending to broker


inp = 1
while True:
    print(f"1. Send message \n2. terminate ")
    inp = input()
    inp = int(inp)
    if inp == 1:
        print("what is the message you would like to write to broker")
        msg = input()
        print("enter topicName")
        topicName = input()
        # cryptoName = topicName
        # web_socket = f'wss://stream.binance.com:9443/ws/{cryptoName}t@kline_1m'
        # ws = websocket.WebSocketApp(
        # web_socket, on_message=on_message, on_close=on_close)
        # ws.run_forever()
        if topicName in producer.keys():
            my_prod = producer.get(topicName)
            send_message(topicName, msg, my_prod)
        else:
            producer[topicName] = create_prod(ADDR)
            my_prod = producer.get(topicName)
            send_message(topicName, msg, my_prod)
        print(f"producer dict is : {producer}")

    else:
        send_message('DISCONECTED', DISCONNECT_MSG)
