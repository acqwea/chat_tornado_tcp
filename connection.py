__author__ = 'Administrator'

import json
import time
import tornado.gen
import tornadoredis
from tornado import stack_context

redisClient = tornadoredis.Client()
redisClient.connect()

allSession = dict() #世界频道 , 所有session, 可用来服务器主推送消息

# TODO +屏蔽敏感词

class Connection(object):
    def __init__(self, stream, address, io_loop):
        # allSession[roleid] = self # 可用来做服务器主推， 无需进程间通信 ， 直接在其他python文件调用

        self.io_loop = io_loop
        self._stream = stream
        self._address = address
        self._roleid = '0'
        self._name = 'null'
        self._gangid = '0'
        self._viplevel = '0'
        self._gangname = 'null'
        self._battlepower = '0'
        self._roletype = '1'
        self._shutup = False

        # 返回确认建立链接消息
        print(">>>>>>>>>---Enter: ", address)
        backData = dict()
        backData['msgid'] = 'ACK_msg'
        jsonData = dict()
        jsonData = json.dumps(backData)
        self.send_message_test(jsonData) # 调试信息
        # self.send_message(jsonData)

        self.chatWorld = tornadoredis.Client()
        self.chatWorld.connect()

        self.chatGang = tornadoredis.Client()
        self.chatGang.connect()

        self.chatWhisper = tornadoredis.Client()
        self.chatWhisper.connect()

        self._stream.set_close_callback(self.on_close)
        self.distribute_message = stack_context.wrap(self._on_message)
        self.read_message()
        return

    def check_pushMessage(self, dataPush):
        if self.request.connection.stream.closed():
            id = str(dataPush['roleid'])
            pushmessage.delRegister(id)
            # 放进redis 防止错失消息
            if dataPush['kind'] != 'kick':
                pushId = 'pushId' + id
                value = int(redis_conn.redis_conn.incr(pushId))
                if value == 11:
                    value = 1
                    redis_conn.redis_conn.set(pushId, value)
                hashKey = pushId + str(value)
                redis_conn.redis_conn.hmset(hashKey, dataPush)
                listKey = pushId + 'list'
                redis_conn.redis_conn.lpush(listKey, hashKey)
                return

        data = dict()
        data['msgid'] = 'push_message'
        data['kind'] = dataPush['kind']

        ret_str = json.dumps(data)
        self.send_message(ret_str)

        print('Push message----------------------')
        print(data)
        print('Push message end..................')



    def on_timeout(self):

        # backData = dict()
        # backData['msgid'] = 'talk'
        # backData['name'] = self._name
        # backData['roleid'] = self._roleid
        # backData['channel'] = 'world'
        # backData['msg'] = '下线！'
        #
        # jsonData = dict()
        # jsonData = json.dumps(backData)
        # self.send_message(jsonData)

        print('测试时间线！', self._roleid)

        # self.remove_dead_session()
        return

    def read_message(self):
        print('read______messge!!!')

        try:
            self._stream.read_until(bytes('\n','utf-8'), self.distribute_message)
        except StreamClosedError:
            print('read err: stream closed.')
            pass

        return

    def unsubscribe(self):
        if (self.chatWorld.subscribed):
            self.chatWorld.unsubscribe('chat_msg_world')

        if self.chatWorld.connection.connected():
            self.chatWorld.connection.disconnect()

        if (self.chatGang.subscribed):
            self.chatGang.unsubscribe('chat_msg_gang')

        if self.chatGang.connection.connected():
            self.chatGang.connection.disconnect()

        if (self.chatWhisper.subscribed):
            self.chatWhisper.unsubscribe('chat_msg_whisper')

        if self.chatWhisper.connection.connected():
            self.chatWhisper.connection.disconnect()
        return

    def remove_dead_session(self):
        print('del----------------------------------------------')
        if self._roleid in allSession:
            print('del:+++++++++++++++++++++++++++++++ ', self._roleid)
            del(allSession[self._roleid])

        self.unsubscribe()
        self.close()
        return

    def on_close(self):
        print("<<<<<<---Left: ", self._roleid)
        print('del**************************************')
        if self._roleid in allSession:
            print('del:==================== ', self._roleid)
            del(allSession[self._roleid])

        self.unsubscribe()
        return

    def close(self):
        if not self._stream.closed():
            self._stream.close()
        return

    def send_message_test(self, data):
        if not self._stream.closed():
            # if data['msgid'] != 'heartbeat_fivesecond':
            t = json.loads(data)
            print('send msg: ========')
            print(t)

            dataE = data.encode()
            try:
                self._stream.write(dataE)
            except:
                self.remove_dead_session()
                print('connection is bad, be offline!')
        else:
            self.remove_dead_session()
        return

    def send_message(self, data):
        if not self._stream.closed():
            dataE = data.encode()
            try:
                self._stream.write(dataE)
            except:
                self.remove_dead_session()
                print('connection is bad, be offline!')
        else:
            self.remove_dead_session()
        return

    def chat_msg_s2c(self, data):
        if data.kind == 'message':
            self.send_message_test(str(data.body)) # 调试信息

        # ---------------------------------------------------------------------------------
        # elif data.kind == 'unsubscribe':
        #     self.chatWorld.disconnect()
        # elif data.kind == 'disconnect':
        #     self.write_message('The connection terminated due to a Redis server error.')
        #     self.close()

        return

    # @tornado.gen.coroutine
    @tornado.gen.engine
    def subscribe(self):
        yield tornado.gen.Task(self.chatWorld.subscribe, 'chat_msg_world')
        self.chatWorld.listen(self.chat_msg_s2c)

        yield tornado.gen.Task(self.chatGang.subscribe, 'chat_msg_gang'+str(self._gangid))
        self.chatGang.listen(self.chat_msg_s2c)

        yield tornado.gen.Task(self.chatWhisper.subscribe, 'chat_msg_whisper'+str(self._roleid))
        self.chatWhisper.listen(self.chat_msg_s2c)

    def roleinfo_handle(self, data):
        roleid = str(data['roleid'])
        self._roleid = roleid
        self._name = data['name']
        self._gangid = data['gangid']
        self._viplevel = data['viplevel']
        self._gangname = data['gangname']
        self._battlepower = data['battlepower']
        self._roletype = data['roletype']

        allSession[roleid] = self # 可用来做服务器主推， 无需进程间通信 ， 直接在其他python文件调用
        print("connection num is:", len(allSession))

        self.subscribe() # 订阅三个频道 世界， 所属帮派， 自己的私聊频道

        # test-----------------------------
        # backData = dict()
        # backData['msgid'] = 'talk'
        # backData['name'] = self._name
        # backData['roleid'] = self._roleid
        # backData['channel'] = 'world'
        # backData['msg'] = str(self._name) + '上线了！'

        # jsonData = dict()
        # jsonData = json.dumps(backData)
        # redisClient.publish('chat_msg_world', jsonData)

        return

    def talk_andle(self, data):
        if self._shutup:
            send_shutup()
            return

        backData = dict()
        backData['msgid'] = 'talk'
        backData['name'] = self._name
        backData['roleid'] = self._roleid
        backData['channel'] = data['channel']
        backData['msg'] = data['msg'] #45个字符控制
        backData['time'] = time.strftime("%H:%M:%S", time.localtime())
        backData['viplevel'] = self._viplevel
        backData['gangname'] = self._gangname
        backData['battlepower'] = self._battlepower
        backData['roletype'] = self._roletype

        jsonData = dict()
        jsonData = json.dumps(backData)

        if data['channel'] == 'world':
            redisClient.publish('chat_msg_world', jsonData)
            return
        elif data['channel'] == 'gang':
            redisClient.publish('chat_msg_gang'+str(data['para']), jsonData)
            return
        elif data['channel'] == 'whisper':
            redisClient.publish('chat_msg_whisper'+str(data['para']), jsonData)
            self.send_message(jsonData) # 给自己发
            return
        else:
            return


    def send_shutup(self):
        backData = dict()
        backData['msgid'] = 'shutup'

        jsonData = dict()
        jsonData = json.dumps(backData)
        self.send_message(jsonData)
        return

    def heartbeat_fivesecond_handle(self, data):
        backData = dict()
        backData['msgid'] = 'heartbeat_fivesecond'

        jsonData = dict()
        jsonData = json.dumps(backData)
        self.send_message(jsonData)
        return

    # 消息对应处理函数
    switch = {
    'roleinfo': roleinfo_handle,
    'talk': talk_andle,
    'heartbeat_fivesecond': heartbeat_fivesecond_handle,
    }

    def _on_message(self, data):
        try:
            timeout = 16

            jsonData = data.decode()
            paraData = json.loads(jsonData)
            msgid = paraData['msgid']
            self.switch[msgid](self, paraData)

            self.io_loop.add_timeout(self.io_loop.time() + timeout, self.on_timeout)
        except Exception as ex:
            print(str(ex))


        self.read_message()
        return

    # def broadcast_messages(self, data):
    #     # print(repr(data))
    #     for conn in allSession:
    #         conn.send_message(data)
    #     return

    def set_player_shutup(self, action):
        self._shutup = action
        return