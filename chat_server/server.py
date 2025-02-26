import socket, sys, datetime, threading, time, json, select, errno, queue
import signal
from absl import app, flags
import message_pb2 as pb

HOST = '127.0.0.1'
FLAGS = flags.FLAGS
SHUTDOWN = 4

flags.DEFINE_integer(name='workers', default=None, required=True, help='서버 Worker 개수')
flags.DEFINE_enum(name='format', default='json', enum_values=['json', 'protobuf'], help='메시지 포맷')
flags.DEFINE_integer(name='port', default='10123', required=False , help='포트 번호')


is_stopping:bool = False
sockets_list:list[socket.socket] = []
clientsocks = []
#clients = []
rooms = {}
roomcount = 0
workers:list[threading.Thread] = []
active_socket = []
message_queue = queue.Queue()
consum_condition = threading.Condition()
socket_condition = threading.Condition()
rooms_lock = threading.Lock()


class SocketClosed(RuntimeError):
    pass

class NoTypeFieldInMessage(RuntimeError):
    pass

class UnknownTypeInMessage(RuntimeError):
    def __self__(self, _type):
        self.type = _type

    def __str__(self):
        return str(self.type)

#클라이언트 정보를 담는 객체
class ClientSock:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.msg_len = None
        self.recv_buffer = None
        self.current_protobuf_type = None
        self.room = None
        self.name = addr
    
    def set_clientname(self, name):
        self.name = name

    def join_room(self, id):
        self.room = id
    
    def leave_room(self):
        self.room = None

    def clear_buffer(self):
        self.msg_len = None
        self.recv_buffer = None
        self.current_protobuf_type = None



# room의 client목록, id, name의 정보를 담고 있는 객체
class Room:
    def __init__(self, client, id, name):
        self.clients = [client]
        self.id = id
        self.name = name

    def add_client(self, client):
        self.clients.append(client)
    
    def rm_client(self, client):
        self.clients.remove(client)

    def format_json(self):
        members = []
        for client in self.clients:
            members.append(client.name)
        format_data = {
            'roomId' : self.id,
            'title' : self.name,
            'members' : members
        }
        return format_data

    def format_proto(self, format_data):
        members = []
        for client in self.clients:
            members.append(client.name)

        format_data.roomId = self.id
        format_data.title = self.name
        format_data.members = members
        return format_data
    
    def get_members(self):
        members = []
        for client in self.clients:
            members.append(str(client.name))
        return members



def finalize():
    global workers
    global is_stopping
    global sockets_list
    is_stopping = True
    for i in range(FLAGS.workers):
        workers[i].join()
        print(f"'thread join' num : {i}\n")
    
    for index, sock in enumerate(sockets_list):
        print(f"socket {index} close\n")
        sock.close()

    print("server down")


def handle_sigint(signum, frame):
    print("\n강제 종료 신호(SIGINT)를 받았습니다.\n")
    print("자원 정리 중...\n")
    finalize()

    print("프로그램 종료.\n")
    sys.exit(0)  # 안전하게 종료


def sc_chat(msg):
    global rooms
    messages = []
    if FLAGS.format == 'json':
        if msg[0].room == None:
            return sc_chat_sys(msg)

        send_message = {
            'type' : 'SCChat',
            'text' : msg[1]['text'],
            'member' : msg[0].name
        } 

        messages.append(send_message)

        with rooms_lock:
            room = rooms[msg[0].room]
            for s in room.clients:
                if not s == msg[0]:
                    send_messages_to_client(s.sock, messages)
        
    else:
        if msg[0].room == None:
            return sc_chat_sys(msg)
        
        send_message = pb.Type()
        send_message.type = pb.Type.MessageType.SC_CHAT
        messages.append(send_message)

        send_message = pb.SCChat()
        send_message.text = msg[1].text
        send_message.member = str(msg[0].name)
        messages.append(send_message)

        with rooms_lock:
            room = rooms[msg[0].room]
            for s in room.clients:
                if not s == msg[0]:
                    send_messages_to_client(s.sock, messages)

def sc_chat_sys(msg):
    messages = []
    if FLAGS.format == 'json':
        
        send_message = {
            'type' : 'SCSystemMessage',
            'text' : '현재 대화방에 들어가 있지 않습니다.'
        } 

        messages.append(send_message)
        send_messages_to_client(msg[0].sock, messages)
        
    else:
        send_message_type = pb.Type()
        send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
        messages.append(send_message_type)

        send_message = pb.SCSystemMessage()
        send_message.text = '현재 대화방에 들어가 있지 않습니다.'
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)

def sc_name(msg):
    global rooms
    messages = []
    if FLAGS.format == 'json':
        old_name = msg[0].name
        new_name = msg[1]['name']
        msg[0].set_clientname(new_name)
        send_message = {
            'type' : "SCSystemMessage",
            'text' : f"{old_name}의 이름이 {new_name}으로 변경되었습니다."
        }
        messages.append(send_message)

        if msg[0].room:
            with rooms_lock:
                room = rooms[msg[0].room]
                for s in room.clients:
                    send_messages_to_client(s.sock, messages)
        else:
            send_messages_to_client(msg[0].sock, messages)
    else:
        old_name = msg[0].name
        new_name = msg[1].name
        msg[0].set_clientname(msg[1].name)
        send_message_type = pb.Type()
        send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
        messages.append(send_message_type)

        send_message = pb.SCSystemMessage()
        send_message.text = f"{old_name}의 이름이 {new_name}으로 변경되었습니다."
        messages.append(send_message)
        
        if msg[0].room:
            with rooms_lock:
                room = rooms[msg[0].room]
                for s in room.clients:
                    send_messages_to_client(s.sock, messages)
        else:
            send_messages_to_client(msg[0].sock, messages)

def sc_rooms(msg):
    global rooms
    messages = []
    if FLAGS.format == 'json':
        send_rooms:list[dict] = []
        with rooms_lock:
            
            for room in rooms:
                send_rooms.append(rooms[room].format_json())

        send_message = {
            'type' : 'SCRoomsResult',
            'rooms': send_rooms
        }

        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)
    
    else:

        with rooms_lock:
            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_ROOMS_RESULT
            messages.append(send_message_type)

            send_message = pb.SCRoomsResult()
            for room in rooms:
                add_room = send_message.rooms.add()
                add_room.roomId = rooms[room].id
                add_room.title = str(rooms[room].name)
                add_room.members.extend(rooms[room].get_members())   
                
            messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)

def sc_create_room(msg):
    global rooms
    global roomcount
    roomcount += 1
    
    messages = []
    if msg[0].room:
        sc_already_join(msg)
        return
    
    if FLAGS.format == 'json':
        with rooms_lock:
            room = Room(msg[0], roomcount, msg[1]['title'])
            msg[0].room = room.id
            rooms[roomcount] = room

            send_message = {
                'type' : "SCSystemMessage",
                'text' : f"{room.name} 방에 입장했습니다."
            }
            messages.append(send_message)
            send_messages_to_client(msg[0].sock, messages)
    else:
        with rooms_lock:
            room = Room(msg[0], roomcount, msg[1].title)
            msg[0].room = room.id
            rooms[roomcount] = room

            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
            messages.append(send_message_type)

            send_message = pb.SCSystemMessage()
            send_message.text = f"{room.name} 방에 입장했습니다."
            messages.append(send_message)

            send_messages_to_client(msg[0].sock, messages)

def sc_join_room(msg):
    global rooms
    messages = []
    if msg[0].room:
        sc_already_join(msg)
        return
    
    if FLAGS.format == 'json':
        if not msg[1]['roomId'] in rooms:
            sc_join_none_room(msg)
            return
        
        with rooms_lock:
            id = msg[1]['roomId']
            msg[0].join_room(id)
            room = rooms[id]
            room.add_client(msg[0])

            send_message = {
                'type' : "SCSystemMessage",
                'text' : f"{room.name} 방에 입장했습니다."
            }
            messages.append(send_message)

            send_messages_to_client(msg[0].sock, messages)

    else:
        if not msg[1].roomId in rooms:
            sc_join_none_room(msg)
            return

        with rooms_lock:
            id = msg[1].roomId
            msg[0].join_room(id)
            room = rooms[id]
            room.add_client(msg[0])

            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
            messages.append(send_message_type)
            
            send_message = pb.SCSystemMessage()
            send_message.text = f"{room.name} 방에 입장했습니다."
            messages.append(send_message)

            send_messages_to_client(msg[0].sock, messages)


def sc_join_none_room(msg):
    messages = []
    if FLAGS.format == 'json':
        send_message = {
            'type' : "SCSystemMessage",
            'text' : "대화방이 존재하지 않습니다."
        }
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)
    else:
        send_message_type = pb.Type()
        send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
        messages.append(send_message_type)
        
        send_message = pb.SCSystemMessage()
        send_message.text = "대화방이 존재하지 않습니다."
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)
        

def sc_already_join(msg):
    messages = []
    if FLAGS.format == 'json':
        send_message = {
            'type' : "SCSystemMessage",
            'text' : "대화 방에 있을 때는 다른 방에 들어갈 수 없습니다."
        }
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)
    else:
        send_message_type = pb.Type()
        send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
        messages.append(send_message_type)
        
        send_message = pb.SCSystemMessage()
        send_message.text = "대화 방에 있을 때는 다른 방에 들어갈 수 없습니다"
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)

def sc_join_broadcast(msg):
    global rooms
    messages = []
    if FLAGS.format == 'json':
        if not msg[1]['roomId'] in rooms:
            return
        with rooms_lock:
            send_message = {
                'type' : "SCSystemMessage",
                'text' : f"{msg[0].name} 님이 입장했습니다."
            }
            messages.append(send_message)

            room = rooms[msg[0].room]
            for s in room.clients:
                if not s == msg[0]:
                    send_messages_to_client(s.sock, messages)

    else:
        if not msg[1].roomId in rooms:
            return
        
        with rooms_lock:
            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
            messages.append(send_message_type)
            
            send_message = pb.SCSystemMessage()
            send_message.text = f"{msg[0].name} 님이 입장했습니다."
            messages.append(send_message)

            room = rooms[msg[0].room]
            for s in room.clients:
                if not s == msg[0]:
                    send_messages_to_client(s.sock, messages)

def sc_leave_room(msg):
    global rooms
    messages = []

    if not msg[0].room:
        sc_leave_none_room(msg)
        return
    
    if FLAGS.format == 'json':
        with rooms_lock:
            id = msg[0].room
            rooms[id].rm_client(msg[0])
            name = rooms[id].name
            msg[0].leave_room()

            if not rooms[id].clients:
                del rooms[id]
            
            send_message = {
                'type' : "SCSystemMessage",
                'text' : f"{name} 방에서 퇴장했습니다."
            }
            messages.append(send_message)

            send_messages_to_client(msg[0].sock, messages)  

            send_message = {
                'type' : "SCSystemMessage",
                'text' : f"{msg[0].name} 님이 퇴장했습니다."
            }
            messages.append(send_message)

            if not id in rooms:
                return
            
            for s in rooms[id].clients:
                send_messages_to_client(s.sock, messages)   

    else:
        with rooms_lock:
            id = msg[0].room
            rooms[id].rm_client(msg[0])
            name = rooms[id].name
            msg[0].leave_room()

            if not rooms[id].clients:
                del rooms[id]

            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
            messages.append(send_message_type)
            
            send_message = pb.SCSystemMessage()
            send_message.text = f"{name} 방에서 퇴장했습니다."
            messages.append(send_message)

            send_messages_to_client(msg[0].sock, messages)

            send_message_type = pb.Type()
            send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
            messages.append(send_message_type)
            
            send_message = pb.SCSystemMessage()
            send_message.text = f"{msg[0].name} 님이 퇴장했습니다."
            messages.append(send_message)
            
            if not id in rooms:
                return

            for s in rooms[id].clients:
                send_messages_to_client(s.sock, messages)

def sc_leave_none_room(msg):
    messages = []
    if FLAGS.format == 'json':
        send_message = {
            'type' : "SCSystemMessage",
            'text' : "현재 대화방에 들어가 있지 않습니다."
        }
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)
    else:
        send_message_type = pb.Type()
        send_message_type.type = pb.Type.MessageType.SC_SYSTEM_MESSAGE
        messages.append(send_message_type)
        
        send_message = pb.SCSystemMessage()
        send_message.text = "현재 대화방에 들어가 있지 않습니다."
        messages.append(send_message)

        send_messages_to_client(msg[0].sock, messages)



json_message_handlers = {
    'CSChat': [sc_chat],
    'CSName': [sc_name],
    'CSRooms': [sc_rooms], 
    'CSCreateRoom': [sc_create_room],
    'CSJoinRoom': [sc_join_room, sc_join_broadcast],
    'CSLeaveRoom': [sc_leave_room],
    #'CSShutdown': sc_shutdown
}

protobuf_message_handlers = {
    pb.Type.MessageType.CS_CHAT: [sc_chat],
    pb.Type.MessageType.CS_NAME : [sc_name],
    pb.Type.MessageType.CS_ROOMS: [sc_rooms],
    pb.Type.MessageType.CS_CREATE_ROOM : [sc_create_room],
    pb.Type.MessageType.CS_JOIN_ROOM : [sc_join_room, sc_join_broadcast],
    pb.Type.MessageType.CS_LEAVE_ROOM : [sc_leave_room],
    pb.Type.MessageType.CS_SHUTDOWN : [],
}

protobuf_message_parsers = {
    pb.Type.MessageType.CS_CHAT: pb.CSChat.FromString,
    pb.Type.MessageType.CS_NAME: pb.CSName.FromString,
    pb.Type.MessageType.CS_ROOMS: pb.CSRooms.FromString,
    pb.Type.MessageType.CS_CREATE_ROOM: pb.CSCreateRoom.FromString,
    pb.Type.MessageType.CS_JOIN_ROOM: pb.CSJoinRoom.FromString,
    pb.Type.MessageType.CS_LEAVE_ROOM: pb.CSLeaveRoom.FromString,
    pb.Type.MessageType.CS_SHUTDOWN: pb.CSShutdown.FromString,
}

def send_messages_to_client(sock, messages):
  '''
  TCP socket 상으로 message 를 전송한다.
  앞에 길이에 해당하는 2 byte 를 network byte order 로 전송한다.

  :param sock: 서버와 맺은 TCP socket
  :param messages: 전송할 message list.각 메시지는 dict type 이거나 protobuf type 이어야 한다.
  '''
  assert isinstance(messages, list)

  for msg in messages:
    msg_str = None
    if FLAGS.format == 'json':
      serialized = bytes(json.dumps(msg), encoding='utf-8')
      msg_str = json.dumps(msg)
    else:
      serialized = msg.SerializeToString()
      msg_str = str(msg).strip()

    # TCP 에서 send() 함수는 일부만 전송될 수도 있다.
    # 따라서 보내려는 데이터를 다 못 보낸 경우 재시도 해야된다.
    to_send = len(serialized)
    to_send_big_endian = int.to_bytes(to_send, byteorder='big', length=2)

    # 받는 쪽에서 어디까지 읽어야 되는지 message boundary 를 알 수 있게끔 2byte 길이를 추가한다.
    serialized = to_send_big_endian + serialized

    offset = 0
    attempt = 0
    while offset < len(serialized):
      num_sent = sock.send(serialized[offset:])
      if num_sent <= 0:
        raise RuntimeError('Send failed')
      offset += num_sent



def message_producer(client:ClientSock):
    global socket_condition
    global consum_condition
    global active_socket
    message = client.sock.recv(65535)
    if not message:
        return None
    
    if not client.recv_buffer:
        client.recv_buffer = message
    else:
        client.recv_buffer += message
    
    while True:
        if client.msg_len is None:
            if len(client.recv_buffer) < 2:
                return 1

            client.msg_len = int.from_bytes(client.recv_buffer[0:2], byteorder='big')
            client.recv_buffer = client.recv_buffer[2:]


        if len(client.recv_buffer) < client.msg_len:
            return 1
        

        serialized = client.recv_buffer[:client.msg_len]
        client.recv_buffer = client.recv_buffer[client.msg_len:]
        client.msg_len = None
        

        # JSON 은 그 자체로 바로 처리 가능하다.
        if FLAGS.format == 'json':
            msg = json.loads(serialized)
            # 'type' 이라는 field 가 있어야 한다.
            msg_type = msg.get('type', None)
            if not msg_type:
                raise NoTypeFieldInMessage()
            
            if msg_type == 'CSShutdown':
                return '/shutdown'
            message = (client, msg)
            with socket_condition:
                while client.sock in active_socket:
                    socket_condition.wait()
                message_queue.put(message)
                active_socket.append(client.sock)
                with consum_condition:
                    consum_condition.notify()
            return msg
        
        else:
            # 현재 type 을 모르는 상태다. 먼저 TypeMessage 를 복구한다.
            if client.current_protobuf_type is None:
                msg = pb.Type.FromString(serialized)
                if msg.type in protobuf_message_parsers and msg.type in protobuf_message_handlers:
                    client.current_protobuf_type = msg.type
                else:
                    raise UnknownTypeInMessage(msg.type)
            
            else:
            # type 을 알고 있으므로 parser 를 이용해서 메시지를 복구한다.
                msg = protobuf_message_parsers[client.current_protobuf_type](serialized)
                if client.current_protobuf_type == pb.Type.MessageType.CS_SHUTDOWN:
                    return '/shutdown'
                
            
                message = (client, msg)
                with socket_condition:
                    while client.sock in active_socket:
                        socket_condition.wait()
                    message_queue.put(message)
                    active_socket.append(client.sock)
                    with consum_condition:
                        consum_condition.notify()
                return msg


    
def message_consumer():
    global is_stopping
    global consum_condition
    global socket_condition
    global message_queue
    global sockets_list
    while not is_stopping:
        with consum_condition:
            while not is_stopping and message_queue.empty():
                consum_condition.wait(0.1)
            if is_stopping: break

            message = message_queue.get()
            if FLAGS.format == 'json':
                for sc in json_message_handlers[message[1].get('type', None)]:
                    sc(message)

                with socket_condition:
                    message[0].clear_buffer()
                    active_socket.remove(message[0].sock)
                    socket_condition.notify()
            else:
                for sc in protobuf_message_handlers[message[0].current_protobuf_type]:
                    sc(message)
                
                with socket_condition:
                    message[0].clear_buffer()
                    active_socket.remove(message[0].sock)
                    socket_condition.notify()



def main(argv):

    if not FLAGS.workers:
        print('서버의 thread 개수를 지정해야 됩니다.\n')
        sys.exit(1)

    if not FLAGS.format:
        print('서버의 format 번호를 지정해야 됩니다.\n')
        sys.exit(2)

    global sockets_list
    #global clients
    global clientsocks
    global workers
    kill_flag = 0

    for i in range(FLAGS.workers):
        t = threading.Thread(target=message_consumer)
        t.start()
        workers.append(t)
        print(f"'new thread start' num : {i}\n")

    
    #passive 소켓 생성
    passive_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    passive_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    passive_sock.bind((HOST,FLAGS.port))
    passive_sock.setblocking(False)
    print(f"server bind : HOST '{HOST}' | PORT {FLAGS.port}\n")
    
    passive_sock.listen(10)
    
    sockets_list = [passive_sock]

    while True:
        read_sockets, write_socket, exception_sockets = select.select(sockets_list, [], sockets_list, 0.1)
        for notified_socket in read_sockets:
        # 새로운 연결 수락
            if notified_socket == passive_sock:
                try:
                    client_socket, client_address = passive_sock.accept()
                    sockets_list.append(client_socket)
                    clientsock = ClientSock(client_socket,client_address)
                    clientsocks.append(clientsock)
                    print(f"new accept: {client_address}\n")
                except Exception as e:
                    print(f"accept error: {e}\n")
            else:
                for client in clientsocks:
                    try:
                        if client.sock == notified_socket:
                            msg = message_producer(client)
                            if msg == None:
                                print(f"연결 종료: {client.addr}\n")
                                client.sock.close()
                                sockets_list.remove(client.sock)

                            if msg == '/shutdown':
                                kill_flag = 1
                                break

                    except ConnectionResetError:
                        # 예기치 않은 연결 종료 시
                        print(f"Client {client.sock.getpeername()} disconnected unexpectedly.\n")
                        client.sock.close()
                        sockets_list.remove(client.sock)
            
        
        if kill_flag == 1:
            break

    finalize()


    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_sigint)
    app.run(main)
