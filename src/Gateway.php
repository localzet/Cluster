<?php

declare(strict_types=1);

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 * 
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright (c) 2018-2024 Localzet Group
 * @license     https://www.gnu.org/licenses/agpl AGPL-3.0 license
 * 
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as
 *              published by the Free Software Foundation, either version 3 of the
 *              License, or (at your option) any later version.
 *              
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *              GNU Affero General Public License for more details.
 *              
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

namespace localzet\Cluster;

/** Localzet Server */

use localzet\Server\Timer;
use localzet\Server;
use localzet\Server\Connection\TcpConnection;
use localzet\Server\Connection\AsyncTcpConnection;

/** Localzet Cluster */

use localzet\Cluster\Lib\Context;
use localzet\Cluster\Protocols\Federation;

/**
 * Шлюз
 * Является подклассом класса Server и отвечает за управление шлюзом в Localzet Cluster.
 */
class Gateway extends Server
{
    /**
     * IP-адрес локальной машины.
     * При развертывании на одной машине используется значение по умолчанию 127.0.0.1.
     * Если развертывание выполняется в распределенной среде, необходимо установить IP-адрес локальной машины.
     *
     * @var string
     */
    public $lanIp = '127.0.0.1';

    /**
     * Если хост-машина имеет IP-адрес 192.168.1.2, а шлюз находится в контейнере Docker (172.25.0.2),
     * то при $lanIp = 192.68.1.2 Клиент может подключиться, но $this->_innerTcpServer stream_socket_server(): Unable to connect to tcp://192.168.1.2:2901 (Address not available) in.
     * В то же время, при lanIp=172.25.0.2 Клиент stream_socket_server(): Unable to connect to tcp://172.25.0.2:2901 (Address not available), а $this->_innerTcpServer нормально прослушивает.
     *
     * Решение:
     * $gateway->lanIp = '192.168.1.2';
     * $gateway->innerTcpServerListen = '172.25.0.2'; // или '0.0.0.0'
     *
     * GatewayClientSDK подключается к 192.168.1.2:lanPort,
     * а $this->_innerTcpServer прослушивает $gateway->innerTcpServerListen:lanPort.
     */
    public $innerTcpServerListen = '';

    /**
     * Локальный порт.
     *
     * @var string
     */
    public $lanPort = 0;

    /**
     * Начальный порт внутреннего обмена данными между шлюзом и бизнес-сервером.
     * Каждый экземпляр шлюза должен использовать уникальный порт с шагом 1000.
     *
     * @var int
     */
    public $startPort = 2000;

    /**
     * Адрес Регистратора, используемого для регистрации Шлюза и обеспечения связи.
     *
     * @var string|array
     */
    public $registerAddress = '127.0.0.1:1236';

    /**
     * Интервал между отправкой пинговых сообщений (время жизни соединения).
     *
     * @var int
     */
    public $pingInterval = 0;

    /**
     * $pingNotResponseLimit * $pingInterval 时间内，客户端未发送任何数据，断开客户端连接
     *
     * @var int
     */
    public $pingNotResponseLimit = 0;

    /**
     * 服务端向客户端发送的心跳数据
     *
     * @var string
     */
    public $pingData = '';

    /**
     * 秘钥
     *
     * @var string
     */
    public $secretKey = '';

    /**
     * 路由函数
     *
     * @var callable|null
     */
    public $router = null;


    /**
     * gateway进程转发给businessServer进程的发送缓冲区大小
     *
     * @var int
     */
    public $sendToServerBufferSize = 10240000;

    /**
     * gateway进程将数据发给客户端时每个客户端发送缓冲区大小
     *
     * @var int
     */
    public $sendToClientBufferSize = 1024000;

    /**
     * 协议加速
     *
     * @var bool
     */
    public $protocolAccelerate = false;

    /**
     * Business 连接成功之后触发
     *
     * @var callable|null
     */
    public $onBusinessConnected = null;

    /**
     * Business 关闭时触发
     *
     * @var callable|null
     */
    public $onBusinessClose = null;

    /**
     * 保存客户端的所有 connection 对象
     *
     * @var array
     */
    protected $_clientConnections = array();

    /**
     * uid 到 connection 的映射，一对多关系
     */
    protected $_uidConnections = array();

    /**
     * group 到 connection 的映射，一对多关系
     *
     * @var array
     */
    protected $_groupConnections = array();

    /**
     * 保存所有 server 的内部连接的 connection 对象
     *
     * @var array
     */
    protected $_serverConnections = array();

    /**
     * gateway 内部监听 server 内部连接的 server
     *
     * @var Server
     */
    protected $_innerTcpServer = null;

    /**
     * 当 server 启动时
     *
     * @var callable|null
     */
    protected $_onServerStart = null;

    /**
     * 当有客户端连接时
     *
     * @var callable|null
     */
    protected $_onConnect = null;

    /**
     * 当客户端发来消息时
     *
     * @var callable|null
     */
    protected $_onMessage = null;

    /**
     * 当客户端连接关闭时
     *
     * @var callable|null
     */
    protected $_onClose = null;

    /**
     * 当 server 停止时
     *
     * @var callable|null
     */
    protected $_onServerStop = null;

    /**
     * 进程启动时间
     *
     * @var int
     */
    protected $_startTime = 0;

    /**
     * gateway 监听的端口
     *
     * @var int
     */
    protected $_gatewayPort = 0;

    /**
     * connectionId 记录器
     * @var int
     */
    protected static $_connectionIdRecorder = 0;

    /**
     * 用于保持长连接的心跳时间间隔
     *
     * @var int
     */
    const PERSISTENCE_CONNECTION_PING_INTERVAL = 25;

    /**
     * 构造函数
     *
     * @param string $socket_name
     * @param array  $context_option
     */
    public function __construct($socket_name, $context_option = array())
    {
        parent::__construct($socket_name, $context_option);
        $this->reloadable = false;
        $this->_gatewayPort = substr(strrchr($socket_name, ':'), 1);
        $this->router = array("\\localzet\Cluster\\Gateway", 'routerBind');
    }

    /**
     * {@inheritdoc}
     */
    public function run(): void
    {
        // 保存用户的回调，当对应的事件发生时触发
        $this->_onServerStart = $this->onServerStart;
        $this->onServerStart  = array($this, 'onServerStart');
        // 保存用户的回调，当对应的事件发生时触发
        $this->_onConnect = $this->onConnect;
        $this->onConnect  = array($this, 'onClientConnect');

        // onMessage禁止用户设置回调
        $this->onMessage = array($this, 'onClientMessage');

        // 保存用户的回调，当对应的事件发生时触发
        $this->_onClose = $this->onClose;
        $this->onClose  = array($this, 'onClientClose');
        // 保存用户的回调，当对应的事件发生时触发
        $this->_onServerStop = $this->onServerStop;
        $this->onServerStop  = array($this, 'onServerStop');

        if (!is_array($this->registerAddress)) {
            $this->registerAddress = array($this->registerAddress);
        }

        // 记录进程启动的时间
        $this->_startTime = time();
        // 运行父方法
        parent::run();
    }

    /**
     * 当客户端发来数据时，转发给server处理
     *
     * @param TcpConnection $connection
     * @param mixed         $data
     */
    public function onClientMessage($connection, $data)
    {
        $connection->pingNotResponseCount = -1;
        $this->sendToServer(Federation::CMD_ON_MESSAGE, $connection, $data);
    }

    /**
     * 当客户端连接上来时，初始化一些客户端的数据
     * 包括全局唯一的client_id、初始化session等
     *
     * @param TcpConnection $connection
     */
    public function onClientConnect($connection)
    {
        $connection->id = self::generateConnectionId();
        // 保存该连接的内部通讯的数据包报头，避免每次重新初始化
        $connection->gatewayHeader = array(
            'local_ip'      => ip2long($this->lanIp),
            'local_port'    => $this->lanPort,
            'client_ip'     => ip2long($connection->getRemoteIp()),
            'client_port'   => $connection->getRemotePort(),
            'gateway_port'  => $this->_gatewayPort,
            'connection_id' => $connection->id,
            'flag'          => 0,
        );
        // 连接的 session
        $connection->session                       = '';
        // 该连接的心跳参数
        $connection->pingNotResponseCount          = -1;
        // 该链接发送缓冲区大小
        $connection->maxSendBufferSize             = $this->sendToClientBufferSize;
        // 保存客户端连接 connection 对象
        $this->_clientConnections[$connection->id] = $connection;

        // 如果用户有自定义 onConnect 回调，则执行
        if ($this->_onConnect) {
            call_user_func($this->_onConnect, $connection);
            if (isset($connection->onWebSocketConnect)) {
                $connection->_onWebSocketConnect = $connection->onWebSocketConnect;
            }
        }
        if ($connection->protocol === '\localzet\Server\Protocols\Websocket' || $connection->protocol === 'localzet\Server\Protocols\Websocket') {
            $connection->onWebSocketConnect = array($this, 'onWebsocketConnect');
        }

        $this->sendToServer(Federation::CMD_ON_CONNECT, $connection);
    }

    /**
     * websocket握手时触发
     *
     * @param $connection
     * @param $request
     */
    public function onWebsocketConnect($connection, $request)
    {
        if (isset($connection->_onWebSocketConnect)) {
            call_user_func($connection->_onWebSocketConnect, $connection, $request);
            unset($connection->_onWebSocketConnect);
        }
        if (is_object($request)) {
            $server = [
                'QUERY_STRING' => $request->queryString(),
                'REQUEST_METHOD' => $request->method(),
                'REQUEST_URI' => $request->uri(),
                'SERVER_PROTOCOL' => "HTTP/" . $request->protocolVersion(),
                'SERVER_NAME' => $request->host(false),
                'CONTENT_TYPE' => $request->header('content-type'),
                'REMOTE_ADDR' => $connection->getRemoteIp(),
                'REMOTE_PORT' => $connection->getRemotePort(),
                'SERVER_PORT' => $connection->getLocalPort(),
            ];
            foreach ($request->header() as $key => $header) {
                $key = str_replace('-', '_', strtoupper($key));
                $server["HTTP_$key"] = $header;
            }
            $data = array('get' => $request->get(), 'server' => $server, 'cookie' => $request->cookie());
        } else {
            $data = array('get' => $_GET, 'server' => $_SERVER, 'cookie' => $_COOKIE);
        }
        $this->sendToServer(Federation::CMD_ON_WEBSOCKET_CONNECT, $connection, $data);
    }

    /**
     * 生成connection id
     * @return int
     */
    protected function generateConnectionId()
    {
        $max_unsigned_int = 4294967295;
        if (self::$_connectionIdRecorder >= $max_unsigned_int) {
            self::$_connectionIdRecorder = 0;
        }
        while (++self::$_connectionIdRecorder <= $max_unsigned_int) {
            if (!isset($this->_clientConnections[self::$_connectionIdRecorder])) {
                break;
            }
        }
        return self::$_connectionIdRecorder;
    }

    /**
     * 发送数据给 server 进程
     *
     * @param int           $cmd
     * @param TcpConnection $connection
     * @param mixed         $body
     * @return bool
     */
    protected function sendToServer($cmd, $connection, $body = '')
    {
        $gateway_data             = $connection->gatewayHeader;
        $gateway_data['cmd']      = $cmd;
        $gateway_data['body']     = $body;
        $gateway_data['ext_data'] = $connection->session;
        if ($this->_serverConnections) {
            // 调用路由函数，选择一个server把请求转发给它
            /** @var TcpConnection $server_connection */
            $server_connection = call_user_func($this->router, $this->_serverConnections, $connection, $cmd, $body);
            if (false === $server_connection->send($gateway_data)) {
                $msg = "SendBufferToServer fail. May be the send buffer are overflow. See http://doc2.workerman.net/send-buffer-overflow.html";
                static::log($msg);
                return false;
            }
        } // 没有可用的 server
        else {
            // gateway 启动后 1-2 秒内 SendBufferToServer fail 是正常现象，因为与 server 的连接还没建立起来，
            // 所以不记录日志，只是关闭连接
            $time_diff = 2;
            if (time() - $this->_startTime >= $time_diff) {
                $msg = 'SendBufferToServer fail. The connections between Gateway and Business are not ready. See http://doc2.workerman.net/send-buffer-to-worker-fail.html';
                static::log($msg);
            }
            $connection->destroy();
            return false;
        }
        return true;
    }

    /**
     * 随机路由，返回 server connection 对象
     *
     * @param array         $server_connections
     * @param TcpConnection $client_connection
     * @param int           $cmd
     * @param mixed         $buffer
     * @return TcpConnection
     */
    public static function routerRand($server_connections, $client_connection, $cmd, $buffer)
    {
        return $server_connections[array_rand($server_connections)];
    }

    /**
     * client_id 与 server 绑定
     *
     * @param array         $server_connections
     * @param TcpConnection $client_connection
     * @param int           $cmd
     * @param mixed         $buffer
     * @return TcpConnection
     */
    public static function routerBind($server_connections, $client_connection, $cmd, $buffer)
    {
        if (!isset($client_connection->businessserver_address) || !isset($server_connections[$client_connection->businessserver_address])) {
            $client_connection->businessserver_address = array_rand($server_connections);
        }
        return $server_connections[$client_connection->businessserver_address];
    }

    /**
     * 当客户端关闭时
     *
     * @param TcpConnection $connection
     */
    public function onClientClose($connection)
    {
        // 尝试通知 server，触发 Event::onClose
        $this->sendToServer(Federation::CMD_ON_CLOSE, $connection);
        unset($this->_clientConnections[$connection->id]);
        // 清理 uid 数据
        if (!empty($connection->uid)) {
            $uid = $connection->uid;
            unset($this->_uidConnections[$uid][$connection->id]);
            if (empty($this->_uidConnections[$uid])) {
                unset($this->_uidConnections[$uid]);
            }
        }
        // 清理 group 数据
        if (!empty($connection->groups)) {
            foreach ($connection->groups as $group) {
                unset($this->_groupConnections[$group][$connection->id]);
                if (empty($this->_groupConnections[$group])) {
                    unset($this->_groupConnections[$group]);
                }
            }
        }
        // 触发 onClose
        if ($this->_onClose) {
            call_user_func($this->_onClose, $connection);
        }
    }

    /**
     * 当 Gateway 启动的时候触发的回调函数
     *
     * @return void
     */
    public function onServerStart()
    {
        // 分配一个内部通讯端口
        $this->lanPort = $this->startPort + $this->id;

        // 如果有设置心跳，则定时执行
        if ($this->pingInterval > 0) {
            $timer_interval = $this->pingNotResponseLimit > 0 ? $this->pingInterval / 2 : $this->pingInterval;
            Timer::add($timer_interval, array($this, 'ping'));
        }

        // 如果Business ip不是127.0.0.1，则需要加gateway到Business的心跳
        if ($this->lanIp !== '127.0.0.1') {
            Timer::add(self::PERSISTENCE_CONNECTION_PING_INTERVAL, array($this, 'pingBusiness'));
        }

        if (!class_exists('\Protocols\Federation')) {
            class_alias('localzet\Cluster\Protocols\Federation', 'Protocols\Federation');
        }

        //如为公网IP监听，直接换成0.0.0.0 ，否则用内网IP
        $listen_ip = filter_var($this->lanIp, FILTER_VALIDATE_IP, FILTER_FLAG_NO_PRIV_RANGE | FILTER_FLAG_NO_RES_RANGE) ? '0.0.0.0' : $this->lanIp;

        //Use scenario to see line 64
        if ($this->innerTcpServerListen != '') {
            $listen_ip = $this->innerTcpServerListen;
        }

        // 初始化 gateway 内部的监听，用于监听 server 的连接已经连接上发来的数据
        $this->_innerTcpServer = new Server("Federation://{$listen_ip}:{$this->lanPort}");
        $this->_innerTcpServer->reusePort = false;
        $this->_innerTcpServer->listen();
        $this->_innerTcpServer->name = 'GatewayInnerServer';

        // 设置内部监听的相关回调
        $this->_innerTcpServer->onMessage = array($this, 'onServerMessage');

        $this->_innerTcpServer->onConnect = array($this, 'onServerConnect');
        $this->_innerTcpServer->onClose   = array($this, 'onServerClose');

        // 注册 gateway 的内部通讯地址，server 去连这个地址，以便 gateway 与 server 之间建立起 TCP 长连接
        $this->registerAddress();

        if ($this->_onServerStart) {
            call_user_func($this->_onServerStart, $this);
        }
    }


    /**
     * 当 server 通过内部通讯端口连接到 gateway 时
     *
     * @param TcpConnection $connection
     */
    public function onServerConnect($connection)
    {
        $connection->maxSendBufferSize = $this->sendToServerBufferSize;
        $connection->authorized = $this->secretKey ? false : true;
    }

    /**
     * 当 server 发来数据时
     *
     * @param TcpConnection $connection
     * @param mixed         $data
     * @throws \Exception
     *
     * @return void
     */
    public function onServerMessage($connection, $data)
    {
        $cmd = $data['cmd'];
        if (empty($connection->authorized) && $cmd !== Federation::CMD_SERVER_CONNECT && $cmd !== Federation::CMD_GATEWAY_CLIENT_CONNECT) {
            self::log("Unauthorized request from " . $connection->getRemoteIp() . ":" . $connection->getRemotePort());
            $connection->close();
            return;
        }
        switch ($cmd) {
                // Business连接Gateway
            case Federation::CMD_SERVER_CONNECT:
                $server_info = json_decode($data['body'], true);
                if ($server_info['secret_key'] !== $this->secretKey) {
                    self::log("Gateway: Server key does not match " . var_export($this->secretKey, true) . " !== " . var_export($this->secretKey));
                    $connection->close();
                    return;
                }
                $key = $connection->getRemoteIp() . ':' . $server_info['server_key'];
                // 在一台服务器上businessServer->name不能相同
                if (isset($this->_serverConnections[$key])) {
                    self::log("Gateway: Server->name conflict. Key:{$key}");
                    $connection->close();
                    return;
                }
                $connection->key = $key;
                $this->_serverConnections[$key] = $connection;
                $connection->authorized = true;
                if ($this->onBusinessConnected) {
                    call_user_func($this->onBusinessConnected, $connection);
                }
                return;
                // GatewayClient连接Gateway
            case Federation::CMD_GATEWAY_CLIENT_CONNECT:
                $server_info = json_decode($data['body'], true);
                if ($server_info['secret_key'] !== $this->secretKey) {
                    self::log("Gateway: GatewayClient key does not match " . var_export($this->secretKey, true) . " !== " . var_export($this->secretKey, true));
                    $connection->close();
                    return;
                }
                $connection->authorized = true;
                return;
                // 向某客户端发送数据，Gateway::sendToClient($client_id, $message);
            case Federation::CMD_SEND_TO_ONE:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $raw = (bool)($data['flag'] & Federation::FLAG_NOT_CALL_ENCODE);
                    $body = $data['body'];
                    if (!$raw && $this->protocolAccelerate && $this->protocol) {
                        $body = $this->preEncodeForClient($body);
                        $raw = true;
                    }
                    $this->_clientConnections[$data['connection_id']]->send($body, $raw);
                }
                return;
                // 踢出用户，Gateway::closeClient($client_id, $message);
            case Federation::CMD_KICK:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->close($data['body']);
                }
                return;
                // 立即销毁用户连接, Gateway::destroyClient($client_id);
            case Federation::CMD_DESTROY:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->destroy();
                }
                return;
                // 广播, Gateway::sendToAll($message, $client_id_array)
            case Federation::CMD_SEND_TO_ALL:
                $raw = (bool)($data['flag'] & Federation::FLAG_NOT_CALL_ENCODE);
                $body = $data['body'];
                if (!$raw && $this->protocolAccelerate && $this->protocol) {
                    $body = $this->preEncodeForClient($body);
                    $raw = true;
                }
                $ext_data = $data['ext_data'] ? json_decode($data['ext_data'], true) : '';
                // $client_id_array 不为空时，只广播给 $client_id_array 指定的客户端
                if (isset($ext_data['connections'])) {
                    foreach ($ext_data['connections'] as $connection_id) {
                        if (isset($this->_clientConnections[$connection_id])) {
                            $this->_clientConnections[$connection_id]->send($body, $raw);
                        }
                    }
                } // $client_id_array 为空时，广播给所有在线客户端
                else {
                    $exclude_connection_id = !empty($ext_data['exclude']) ? $ext_data['exclude'] : null;
                    foreach ($this->_clientConnections as $client_connection) {
                        if (!isset($exclude_connection_id[$client_connection->id])) {
                            $client_connection->send($body, $raw);
                        }
                    }
                }
                return;
            case Federation::CMD_SELECT:
                $client_info_array = array();
                $ext_data = json_decode($data['ext_data'], true);
                if (!$ext_data) {
                    echo 'CMD_SELECT ext_data=' . var_export($data['ext_data'], true) . '\r\n';
                    $buffer = serialize($client_info_array);
                    $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                    return;
                }
                $fields = $ext_data['fields'];
                $where  = $ext_data['where'];
                if ($where) {
                    $connection_box_map = array(
                        'groups'        => $this->_groupConnections,
                        'uid'           => $this->_uidConnections
                    );
                    // $where = ['groups'=>[x,x..], 'uid'=>[x,x..], 'connection_id'=>[x,x..]]
                    foreach ($where as $key => $items) {
                        if ($key !== 'connection_id') {
                            $connections_box = $connection_box_map[$key];
                            foreach ($items as $item) {
                                if (isset($connections_box[$item])) {
                                    foreach ($connections_box[$item] as $connection_id => $client_connection) {
                                        if (!isset($client_info_array[$connection_id])) {
                                            $client_info_array[$connection_id] = array();
                                            // $fields = ['groups', 'uid', 'session']
                                            foreach ($fields as $field) {
                                                $client_info_array[$connection_id][$field] = isset($client_connection->$field) ? $client_connection->$field : null;
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            foreach ($items as $connection_id) {
                                if (isset($this->_clientConnections[$connection_id])) {
                                    $client_connection = $this->_clientConnections[$connection_id];
                                    $client_info_array[$connection_id] = array();
                                    // $fields = ['groups', 'uid', 'session']
                                    foreach ($fields as $field) {
                                        $client_info_array[$connection_id][$field] = isset($client_connection->$field) ? $client_connection->$field : null;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    foreach ($this->_clientConnections as $connection_id => $client_connection) {
                        foreach ($fields as $field) {
                            $client_info_array[$connection_id][$field] = isset($client_connection->$field) ? $client_connection->$field : null;
                        }
                    }
                }
                $buffer = serialize($client_info_array);
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 获取在线群组列表
            case Federation::CMD_GET_GROUP_ID_LIST:
                $buffer = serialize(array_keys($this->_groupConnections));
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 重新赋值 session
            case Federation::CMD_SET_SESSION:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->session = $data['ext_data'];
                }
                return;
                // session合并
            case Federation::CMD_UPDATE_SESSION:
                if (!isset($this->_clientConnections[$data['connection_id']])) {
                    return;
                } else {
                    if (!$this->_clientConnections[$data['connection_id']]->session) {
                        $this->_clientConnections[$data['connection_id']]->session = $data['ext_data'];
                        return;
                    }
                    $session = Context::sessionDecode($this->_clientConnections[$data['connection_id']]->session);
                    $session_for_merge = Context::sessionDecode($data['ext_data']);
                    $session = array_replace_recursive($session, $session_for_merge);
                    $this->_clientConnections[$data['connection_id']]->session = Context::sessionEncode($session);
                }
                return;
            case Federation::CMD_GET_SESSION_BY_CLIENT_ID:
                if (!isset($this->_clientConnections[$data['connection_id']])) {
                    $session = serialize(null);
                } else {
                    if (!$this->_clientConnections[$data['connection_id']]->session) {
                        $session = serialize(array());
                    } else {
                        $session = $this->_clientConnections[$data['connection_id']]->session;
                    }
                }
                $connection->send(pack('N', strlen($session)) . $session, true);
                return;
                // 获得客户端sessions
            case Federation::CMD_GET_ALL_CLIENT_SESSIONS:
                $client_info_array = array();
                foreach ($this->_clientConnections as $connection_id => $client_connection) {
                    $client_info_array[$connection_id] = $client_connection->session;
                }
                $buffer = serialize($client_info_array);
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 判断某个 client_id 是否在线 Gateway::isOnline($client_id)
            case Federation::CMD_IS_ONLINE:
                $buffer = serialize((int)isset($this->_clientConnections[$data['connection_id']]));
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 将 client_id 与 uid 绑定
            case Federation::CMD_BIND_UID:
                $uid = $data['ext_data'];
                if (empty($uid)) {
                    echo "bindUid(client_id, uid) uid empty, uid=" . var_export($uid, true);
                    return;
                }
                $connection_id = $data['connection_id'];
                if (!isset($this->_clientConnections[$connection_id])) {
                    return;
                }
                $client_connection = $this->_clientConnections[$connection_id];
                if (isset($client_connection->uid)) {
                    $current_uid = $client_connection->uid;
                    unset($this->_uidConnections[$current_uid][$connection_id]);
                    if (empty($this->_uidConnections[$current_uid])) {
                        unset($this->_uidConnections[$current_uid]);
                    }
                }
                $client_connection->uid                      = $uid;
                $this->_uidConnections[$uid][$connection_id] = $client_connection;
                return;
                // client_id 与 uid 解绑 Gateway::unbindUid($client_id, $uid);
            case Federation::CMD_UNBIND_UID:
                $connection_id = $data['connection_id'];
                if (!isset($this->_clientConnections[$connection_id])) {
                    return;
                }
                $client_connection = $this->_clientConnections[$connection_id];
                if (isset($client_connection->uid)) {
                    $current_uid = $client_connection->uid;
                    unset($this->_uidConnections[$current_uid][$connection_id]);
                    if (empty($this->_uidConnections[$current_uid])) {
                        unset($this->_uidConnections[$current_uid]);
                    }
                    $client_connection->uid_info = '';
                    $client_connection->uid      = null;
                }
                return;
                // 发送数据给 uid Gateway::sendToUid($uid, $msg);
            case Federation::CMD_SEND_TO_UID:
                $raw = (bool)($data['flag'] & Federation::FLAG_NOT_CALL_ENCODE);
                $body = $data['body'];
                if (!$raw && $this->protocolAccelerate && $this->protocol) {
                    $body = $this->preEncodeForClient($body);
                    $raw = true;
                }
                $uid_array = json_decode($data['ext_data'], true);
                foreach ($uid_array as $uid) {
                    if (!empty($this->_uidConnections[$uid])) {
                        foreach ($this->_uidConnections[$uid] as $connection) {
                            /** @var TcpConnection $connection */
                            $connection->send($body, $raw);
                        }
                    }
                }
                return;
                // 将 $client_id 加入用户组 Gateway::joinGroup($client_id, $group);
            case Federation::CMD_JOIN_GROUP:
                $group = $data['ext_data'];
                if (empty($group)) {
                    echo "join(group) group empty, group=" . var_export($group, true);
                    return;
                }
                $connection_id = $data['connection_id'];
                if (!isset($this->_clientConnections[$connection_id])) {
                    return;
                }
                $client_connection = $this->_clientConnections[$connection_id];
                if (!isset($client_connection->groups)) {
                    $client_connection->groups = array();
                }
                $client_connection->groups[$group]               = $group;
                $this->_groupConnections[$group][$connection_id] = $client_connection;
                return;
                // 将 $client_id 从某个用户组中移除 Gateway::leaveGroup($client_id, $group);
            case Federation::CMD_LEAVE_GROUP:
                $group = $data['ext_data'];
                if (empty($group)) {
                    echo "leave(group) group empty, group=" . var_export($group, true);
                    return;
                }
                $connection_id = $data['connection_id'];
                if (!isset($this->_clientConnections[$connection_id])) {
                    return;
                }
                $client_connection = $this->_clientConnections[$connection_id];
                if (!isset($client_connection->groups[$group])) {
                    return;
                }
                unset($client_connection->groups[$group], $this->_groupConnections[$group][$connection_id]);
                if (empty($this->_groupConnections[$group])) {
                    unset($this->_groupConnections[$group]);
                }
                return;
                // 解散分组
            case Federation::CMD_UNGROUP:
                $group = $data['ext_data'];
                if (empty($group)) {
                    echo "leave(group) group empty, group=" . var_export($group, true);
                    return;
                }
                if (empty($this->_groupConnections[$group])) {
                    return;
                }
                foreach ($this->_groupConnections[$group] as $client_connection) {
                    unset($client_connection->groups[$group]);
                }
                unset($this->_groupConnections[$group]);
                return;
                // 向某个用户组发送消息 Gateway::sendToGroup($group, $msg);
            case Federation::CMD_SEND_TO_GROUP:
                $raw = (bool)($data['flag'] & Federation::FLAG_NOT_CALL_ENCODE);
                $body = $data['body'];
                if (!$raw && $this->protocolAccelerate && $this->protocol) {
                    $body = $this->preEncodeForClient($body);
                    $raw = true;
                }
                $ext_data = json_decode($data['ext_data'], true);
                $group_array = $ext_data['group'];
                $exclude_connection_id = $ext_data['exclude'];

                foreach ($group_array as $group) {
                    if (!empty($this->_groupConnections[$group])) {
                        foreach ($this->_groupConnections[$group] as $connection) {
                            if (!isset($exclude_connection_id[$connection->id])) {
                                /** @var TcpConnection $connection */
                                $connection->send($body, $raw);
                            }
                        }
                    }
                }
                return;
                // 获取某用户组成员信息 Gateway::getClientSessionsByGroup($group);
            case Federation::CMD_GET_CLIENT_SESSIONS_BY_GROUP:
                $group = $data['ext_data'];
                if (!isset($this->_groupConnections[$group])) {
                    $buffer = serialize(array());
                    $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                    return;
                }
                $client_info_array = array();
                foreach ($this->_groupConnections[$group] as $connection_id => $client_connection) {
                    $client_info_array[$connection_id] = $client_connection->session;
                }
                $buffer = serialize($client_info_array);
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 获取用户组成员数 Gateway::getClientCountByGroup($group);
            case Federation::CMD_GET_CLIENT_COUNT_BY_GROUP:
                $group = $data['ext_data'];
                $count = 0;
                if ($group !== '') {
                    if (isset($this->_groupConnections[$group])) {
                        $count = count($this->_groupConnections[$group]);
                    }
                } else {
                    $count = count($this->_clientConnections);
                }
                $buffer = serialize($count);
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
                // 获取与某个 uid 绑定的所有 client_id Gateway::getClientIdByUid($uid);
            case Federation::CMD_GET_CLIENT_ID_BY_UID:
                $uid = $data['ext_data'];
                if (empty($this->_uidConnections[$uid])) {
                    $buffer = serialize(array());
                } else {
                    $buffer = serialize(array_keys($this->_uidConnections[$uid]));
                }
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
            default:
                $err_msg = "gateway inner pack err cmd=$cmd";
                echo $err_msg;
        }
    }


    /**
     * 当server连接关闭时
     *
     * @param TcpConnection $connection
     */
    public function onServerClose($connection)
    {
        if (isset($connection->key)) {
            unset($this->_serverConnections[$connection->key]);
            if ($this->onBusinessClose) {
                call_user_func($this->onBusinessClose, $connection);
            }
        }
    }

    /**
     * 存储当前 Gateway 的内部通信地址
     *
     * @return bool
     */
    public function registerAddress()
    {
        $address = $this->lanIp . ':' . $this->lanPort;
        foreach ($this->registerAddress as $register_address) {
            $register_connection = new AsyncTcpConnection("text://{$register_address}");
            $secret_key = $this->secretKey;
            $register_connection->onConnect = function ($register_connection) use ($address, $secret_key, $register_address) {
                $register_connection->send('{"event":"gateway_connect", "address":"' . $address . '", "secret_key":"' . $secret_key . '"}');
                // 如果Register服务器不在本地服务器，则需要保持心跳
                if (strpos($register_address, '127.0.0.1') !== 0) {
                    $register_connection->ping_timer = Timer::add(self::PERSISTENCE_CONNECTION_PING_INTERVAL, function () use ($register_connection) {
                        $register_connection->send('{"event":"ping"}');
                    });
                }
            };
            $register_connection->onClose = function ($register_connection) {
                if (!empty($register_connection->ping_timer)) {
                    Timer::del($register_connection->ping_timer);
                }
                $register_connection->reconnect(1);
            };
            $register_connection->connect();
        }
    }


    /**
     * 心跳逻辑
     *
     * @return void
     */
    public function ping()
    {
        $ping_data = $this->pingData ? (string)$this->pingData : null;
        $raw = false;
        if ($this->protocolAccelerate && $ping_data && $this->protocol) {
            $ping_data = $this->preEncodeForClient($ping_data);
            $raw = true;
        }
        // 遍历所有客户端连接
        foreach ($this->_clientConnections as $connection) {
            // 上次发送的心跳还没有回复次数大于限定值就断开
            if (
                $this->pingNotResponseLimit > 0 &&
                $connection->pingNotResponseCount >= $this->pingNotResponseLimit * 2
            ) {
                $connection->destroy();
                continue;
            }
            // $connection->pingNotResponseCount 为 -1 说明最近客户端有发来消息，则不给客户端发送心跳
            $connection->pingNotResponseCount++;
            if ($ping_data) {
                if (
                    $connection->pingNotResponseCount === 0 ||
                    ($this->pingNotResponseLimit > 0 && $connection->pingNotResponseCount % 2 === 1)
                ) {
                    continue;
                }
                $connection->send($ping_data, $raw);
            }
        }
    }

    /**
     * 向 Business 发送心跳数据，用于保持长连接
     *
     * @return void
     */
    public function pingBusiness()
    {
        $gateway_data        = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_PING;
        foreach ($this->_serverConnections as $connection) {
            $connection->send($gateway_data);
        }
    }

    /**
     * @param mixed $data
     *
     * @return string
     */
    protected function preEncodeForClient($data)
    {
        foreach ($this->_clientConnections as $client_connection) {
            return call_user_func(array($client_connection->protocol, 'encode'), $data, $client_connection);
        }
    }

    /**
     * 当 gateway 关闭时触发，清理数据
     *
     * @return void
     */
    public function onServerStop()
    {
        // 尝试触发用户设置的回调
        if ($this->_onServerStop) {
            call_user_func($this->_onServerStop, $this);
        }
    }

    /**
     * Log.
     * @param string $msg
     */
    public static function log($msg): void
    {
        Timer::add(1, function () use ($msg) {
            Server::log($msg);
        }, null, false);
    }
}
