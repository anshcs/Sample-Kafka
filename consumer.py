from time import sleep
import socket
import threading
HEADER_MSG = 1000
HEADER_TOPIC = 1000
PORT = 5050
# SERVER = '192.168.29.240'
SERVER = socket.gethostbyname(socket.gethostname())
# print(SERVER)
ADDR = (SERVER, PORT)  # Binding server and port
DISCONNECT_MSG = "DISCONECTED!"
HEARTBEAT_PORT = 5053
ADDR_HEARTBEAT = (SERVER, HEARTBEAT_PORT)

# no__of_prod = 0
# consumers = socket.socket(
#     socket.AF_INET, socket.SOCK_STREAM)

# consumers.connect(ADDR)

consumer = {}  # keeps track of all consumers


# function to create consumer
def create_con(address):
    connected = False
    con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while not connected:
        try:
            con.connect(address)
            connected = True
        except socket.error:
            sleep(3)
            con.connect(NEWADDR)
            connected = True

    return con


def send_message(topic):
    my_con = consumer.get(topic)
    topic_length, topic_msg = send_encoding_msg(topic)
    # message_length, message = send_encoding_topic(m)
    # print(topic_length, topic_msg, message_length, message)
    c = 'c'

    c_length, c = send_encoding_msg(c)
    try:
        # print(topic_length, topic_msg, message_length, message)
        my_con.send(c_length)
        my_con.send(c)
        my_con.send(topic_length)
        my_con.send(topic_msg)
    except:
        change_port(topic)
    # msg = consumer[topic].recv(HEADER_TOPIC)
    # print(f"messages from subscribed topic is : {msg}")

    val1 = recive_msg(my_con)
    print(f"message : {val1}")

    # my_prod.send(message_length)
    # my_prod.send(message)


def send_encoding_msg(msg):
    message = msg.encode('utf-8')  # we encode the message
    msg_length = len(message)  # we get length of the message
    # firsst message should always be length
    send_length = str(msg_length).encode('utf-8')
    # therefre we pad it to legth of 64
    send_length += b' '*(HEADER_MSG-len(send_length))
    return [send_length, message]
    # consumers.send(send_length)
    # consumers.send(message)


def recive_msg(conn):
    val_length = conn.recv(HEADER_MSG).decode('utf-8')
    val_length = int(val_length)
    val = conn.recv(val_length).decode('utf-8')

    # val_length2 = conn.recv(HEADER_MSG).decode('utf-8')
    # val_length2 = int(val_length2)
    # val2 = conn.recv(val_length2).decode('utf-8')
    # val  = conn.recv()
    return val


# -----------------------------------------------------------------------------------------------------------------------------
# change port connection

def change_port(topic):

    cons = create_con(NEWADDR)
    consumer[topic] = cons
    print(f'changed connections to new addr : {NEWPORT}; {consumer}')
    print(f'producer dict : {consumer}')

    send_message(topic)


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

        except:
            pass
        # reciveport = threading.Thread(target=recive_port)
        # reciveport.start()


# --------------------------------------------------------------------------------------------------------------------
# start heart beat to zookeepr
heartBeat = threading.Thread(target=send_heartbeat)
heartBeat.start()


# def send_encoding_topic(topic):
#     message = topic.encode('utf-8')  # we encode the message
#     msg_length = len(message)  # we get length of the message
#     # firsst message should always be length
#     send_length = str(msg_length).encode('utf-8')
#     # therefre we pad it to legth of 64
#     send_length += b' '*(HEADER_TOPIC-len(send_length))
#     return [send_length, message]

inp = 1
while True:
    print(f"1. Request data \n2. terminate ")
    inp = input()
    inp = int(inp)
    if inp == 1:
        # print("what is the message you would like to write to broker")
        # msg = input()
        print("enter topicName")
        topicName = input()
        if topicName in consumer.keys():
            send_message(topicName)
        else:
            consumer[topicName] = create_con(ADDR)
            send_message(topicName)
        print(f"consumer dict is : {consumer}")
    else:
        break
