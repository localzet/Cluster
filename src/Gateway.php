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

use localzet\Cluster\Gateway\ClientTrait;
use localzet\Cluster\Gateway\ServerTrait;
use localzet\Cluster\Lib\Context;
use localzet\Cluster\Protocols\Cluster;
use localzet\Server;
use localzet\Server\Connection\AsyncTcpConnection;
use localzet\Server\Connection\TcpConnection;
use localzet\Timer;
use Protocols\Federation;

/** Localzet Cluster */

/**
 * Шлюз
 * Является подклассом класса Server и отвечает за управление шлюзом в Localzet Cluster.
 */
class Gateway extends Server
{
    use ClientTrait, ServerTrait;

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
     * Лимит неотвеченных ping перед отключением клиента
     * Если клиент не отвечает на ping $pingNotResponseLimit раз, соединение закрывается
     *
     * @var int
     */
    public $pingNotResponseLimit = 0;

    /**
     * Данные для отправки ping клиентам
     *
     * @var string
     */
    public $pingData = '';

    /**
     * Функция маршрутизации сообщений к Business Worker
     *
     * @var callable|null
     */
    public $router = null;

    /**
     * Размер буфера отправки данных от Gateway к Business Worker
     *
     * @var int
     */
    public $sendToServerBufferSize = 10240000;

    /**
     * Размер буфера отправки данных каждому клиенту
     *
     * @var int
     */
    public $sendToClientBufferSize = 1024000;

    /**
     * Ускорение протокола
     *
     * @var bool
     */
    public $protocolAccelerate = false;

    /**
     * Callback при успешном подключении Business Worker
     *
     * @var callable|null
     */
    public $onBusinessConnected = null;

    /**
     * Callback при отключении Business Worker
     *
     * @var callable|null
     */
    public $onBusinessClose = null;

    /**
     * Все клиентские соединения (connection_id => TcpConnection)
     *
     * @var array
     */
    protected $_clientConnections = array();

    /**
     * Маппинг UID к соединениям (uid => [connection_id1, connection_id2, ...])
     *
     * @var array
     */
    protected $_uidConnections = array();

    /**
     * Маппинг группы к соединениям (group => [connection_id1, connection_id2, ...])
     *
     * @var array
     */
    protected $_groupConnections = array();

    /**
     * Внутренние соединения с Business Worker (key => TcpConnection)
     *
     * @var array
     */
    protected $_serverConnections = array();

    /**
     * Внутренний TCP сервер для приема соединений от Business Worker
     *
     * @var Server
     */
    protected $_innerTcpServer = null;

    /**
     * Callback при запуске сервера
     *
     * @var callable|null
     */
    protected $_onServerStart = null;

    /**
     * Callback при подключении клиента
     *
     * @var callable|null
     */
    protected $_onConnect = null;

    /**
     * Callback при получении сообщения от клиента
     *
     * @var callable|null
     */
    protected $_onMessage = null;

    /**
     * Callback при отключении клиента
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

    public bool $reloadable = false;

    /**
     * 用于保持长连接的心跳时间间隔
     *
     * @var int
     */
    const PERSISTENCE_CONNECTION_PING_INTERVAL = 25;

    /**
     * 构造函数
     *
     * @param string|null $socketName
     * @param array $socketContext
     * @param string|null $secretKey
     */
    public function __construct(?string $socketName = null, array $socketContext = [], public ?string $secretKey = null)
    {
        parent::__construct($socketName, $socketContext);

        // Extract port from socket name (e.g., "websocket://0.0.0.0:7273" -> "7273")
        if ($socketName && strpos($socketName, ':') !== false) {
            $portPart = substr(strrchr($socketName, ':'), 1);
            $this->_gatewayPort = is_numeric($portPart) ? (int)$portPart : 0;
        } else {
            $this->_gatewayPort = 0;
        }
        $this->router = [Gateway::class, 'routerBind'];
    }

    /**
     * {@inheritdoc}
     */
    public function run(): void
    {
        $this->onServerStart = array($this, 'onServerStart');
        $this->onServerStop = array($this, 'onServerStop');

        $this->onConnect = [$this, 'onClientConnect'];
        $this->onMessage = [$this, 'onClientMessage'];
        $this->onClose = [$this, 'onClientClose'];

        if (!is_array($this->registerAddress)) {
            $this->registerAddress = [$this->registerAddress];
        }
        $this->_startTime = time();

        parent::run();
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
        $this->sendToServer(Cluster::CMD_ON_WEBSOCKET_CONNECT, $connection, $data);
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
     * 随机路由，返回 server connection 对象
     *
     * @param array $server_connections
     * @param TcpConnection $client_connection
     * @param int $cmd
     * @param mixed $buffer
     * @return TcpConnection
     */
    public static function routerRand($server_connections, $client_connection, $cmd, $buffer)
    {
        return $server_connections[array_rand($server_connections)];
    }

    /**
     * client_id 与 server 绑定
     *
     * @param array $server_connections
     * @param TcpConnection $client_connection
     * @param int $cmd
     * @param mixed $buffer
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
     * 当 Gateway 启动的时候触发的回调函数
     *
     * @return void
     */
    public function onServerStart()
    {
        $this->lanPort = $this->startPort + $this->id;

        if ($this->pingInterval > 0) {
            $timer_interval = $this->pingNotResponseLimit > 0 ? $this->pingInterval / 2 : $this->pingInterval;
            Timer::add($timer_interval, [$this, 'ping']);
        }

        if ($this->lanIp !== '127.0.0.1') {
            Timer::add(self::PERSISTENCE_CONNECTION_PING_INTERVAL, [$this, 'pingBusiness']);
        }

        if (!class_exists(Federation::class)) {
            class_alias(Cluster::class, Federation::class);
        }

        // Если IP публичный, слушаем на 0.0.0.0, иначе используем внутренний IP
        $listen_ip = filter_var($this->lanIp, FILTER_VALIDATE_IP, FILTER_FLAG_NO_PRIV_RANGE | FILTER_FLAG_NO_RES_RANGE) ? '0.0.0.0' : $this->lanIp;

        // Если указан специальный IP для внутреннего сервера, используем его
        if ($this->innerTcpServerListen != '') {
            $listen_ip = $this->innerTcpServerListen;
        }

        // Инициализация внутреннего TCP сервера для приема соединений от Business Worker
        $this->_innerTcpServer = new Server("Federation://{$listen_ip}:{$this->lanPort}");
        $this->_innerTcpServer->reusePort = false;
        $this->_innerTcpServer->listen();
        $this->_innerTcpServer->name = 'GatewayInnerServer';

        // Настройка обработчиков внутреннего сервера
        $this->_innerTcpServer->onMessage = array($this, 'onServerMessage');
        $this->_innerTcpServer->onConnect = array($this, 'onServerConnect');
        $this->_innerTcpServer->onClose = array($this, 'onServerClose');

        // Регистрация адреса Gateway в Register для обнаружения Business Worker
        $this->registerAddress();
    }


    /**
     * Обработчик подключения Business Worker через внутренний порт
     *
     * @param TcpConnection $connection
     */
    public function onServerConnect($connection)
    {
        $connection->maxSendBufferSize = $this->sendToServerBufferSize;
        $connection->authorized = $this->secretKey ? false : true;
    }

    /**
     * Обработчик сообщений от Business Worker
     *
     * @param TcpConnection $connection
     * @param mixed $data
     * @return void
     * @throws \Exception
     */
    public function onServerMessage($connection, $data)
    {
        $cmd = $data['cmd'];
        if (empty($connection->authorized) && $cmd !== Cluster::CMD_SERVER_CONNECT && $cmd !== Cluster::CMD_GATEWAY_CLIENT_CONNECT) {
            self::log("Unauthorized request from " . $connection->getRemoteIp() . ":" . $connection->getRemotePort());
            $connection->close();
            return;
        }
        switch ($cmd) {
            // Подключение Business Worker к Gateway
            case Cluster::CMD_SERVER_CONNECT:
                $server_info = json_decode($data['body'], true);
                if (!isset($server_info['secret_key']) || $server_info['secret_key'] !== $this->secretKey) {
                    self::log("Gateway: Server key does not match or missing. Expected: " . var_export($this->secretKey, true));
                    $connection->close();
                    return;
                }
                $key = $connection->getRemoteIp() . ':' . $server_info['server_key'];
                // Проверка конфликта имени Business Worker (на одном сервере имена должны быть уникальными)
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
            // Подключение Gateway Client к Gateway
            case Cluster::CMD_GATEWAY_CLIENT_CONNECT:
                $server_info = json_decode($data['body'], true);
                if (!isset($server_info['secret_key']) || $server_info['secret_key'] !== $this->secretKey) {
                    self::log("Gateway: GatewayClient key does not match or missing. Expected: " . var_export($this->secretKey, true));
                    $connection->close();
                    return;
                }
                $connection->authorized = true;
                return;
            // Отправка данных конкретному клиенту
            case Cluster::CMD_SEND_TO_ONE:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $raw = (bool)($data['flag'] & Cluster::FLAG_NOT_CALL_ENCODE);
                    $body = $data['body'];
                    if (!$raw && $this->protocolAccelerate && $this->protocol) {
                        $body = $this->preEncodeForClient($body);
                        $raw = true;
                    }
                    $this->_clientConnections[$data['connection_id']]->send($body, $raw);
                }
                return;
            // Закрытие соединения клиента с сообщением
            case Cluster::CMD_KICK:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->close($data['body']);
                }
                return;
            // Немедленное уничтожение соединения клиента
            case Cluster::CMD_DESTROY:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->destroy();
                }
                return;
            // Рассылка сообщений всем клиентам или списку клиентов
            case Cluster::CMD_SEND_TO_ALL:
                $raw = (bool)($data['flag'] & Cluster::FLAG_NOT_CALL_ENCODE);
                $body = $data['body'];
                if (!$raw && $this->protocolAccelerate && $this->protocol) {
                    $body = $this->preEncodeForClient($body);
                    $raw = true;
                }
                $ext_data = $data['ext_data'] ? json_decode($data['ext_data'], true) : '';
                // Если указан список клиентов, отправляем только им
                if (isset($ext_data['connections'])) {
                    foreach ($ext_data['connections'] as $connection_id) {
                        if (isset($this->_clientConnections[$connection_id])) {
                            $this->_clientConnections[$connection_id]->send($body, $raw);
                        }
                    }
                }
                // Иначе отправляем всем онлайн клиентам
                else {
                    $exclude_connection_id = !empty($ext_data['exclude']) ? $ext_data['exclude'] : null;
                    foreach ($this->_clientConnections as $client_connection) {
                        if (!isset($exclude_connection_id[$client_connection->id])) {
                            $client_connection->send($body, $raw);
                        }
                    }
                }
                return;
            case Cluster::CMD_SELECT:
                $client_info_array = array();
                $ext_data = json_decode($data['ext_data'], true);
                if (!$ext_data) {
                    echo 'CMD_SELECT ext_data=' . var_export($data['ext_data'], true) . '\r\n';
                    $buffer = serialize($client_info_array);
                    $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                    return;
                }
                $fields = $ext_data['fields'];
                $where = $ext_data['where'];
                if ($where) {
                    $connection_box_map = array(
                        'groups' => $this->_groupConnections,
                        'uid' => $this->_uidConnections
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
            // Получение списка онлайн групп
            case Cluster::CMD_GET_GROUP_ID_LIST:
                $buffer = serialize(array_keys($this->_groupConnections));
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
            // Установка сессии (полная замена)
            case Cluster::CMD_SET_SESSION:
                if (isset($this->_clientConnections[$data['connection_id']])) {
                    $this->_clientConnections[$data['connection_id']]->session = $data['ext_data'];
                }
                return;
            // Обновление сессии (слияние)
            case Cluster::CMD_UPDATE_SESSION:
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
            case Cluster::CMD_GET_SESSION_BY_CLIENT_ID:
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
            // Получение всех клиентских сессий
            case Cluster::CMD_GET_ALL_CLIENT_SESSIONS:
                $client_info_array = array();
                foreach ($this->_clientConnections as $connection_id => $client_connection) {
                    $client_info_array[$connection_id] = $client_connection->session;
                }
                $buffer = serialize($client_info_array);
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
            // Проверка онлайн статуса клиента
            case Cluster::CMD_IS_ONLINE:
                $buffer = serialize((int)isset($this->_clientConnections[$data['connection_id']]));
                $connection->send(pack('N', strlen($buffer)) . $buffer, true);
                return;
            // Привязка client_id к UID
            case Cluster::CMD_BIND_UID:
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
                $client_connection->uid = $uid;
                $this->_uidConnections[$uid][$connection_id] = $client_connection;
                return;
            // client_id 与 uid 解绑 Gateway::unbindUid($client_id, $uid);
            case Cluster::CMD_UNBIND_UID:
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
                    $client_connection->uid = null;
                }
                return;
            // Отправка данных по UID
            case Cluster::CMD_SEND_TO_UID:
                $raw = (bool)($data['flag'] & Cluster::FLAG_NOT_CALL_ENCODE);
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
            // Добавление клиента в группу
            case Cluster::CMD_JOIN_GROUP:
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
                $client_connection->groups[$group] = $group;
                $this->_groupConnections[$group][$connection_id] = $client_connection;
                return;
            // 将 $client_id 从某个用户组中移除 Gateway::leaveGroup($client_id, $group);
            case Cluster::CMD_LEAVE_GROUP:
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
            // Распустить группу
            case Cluster::CMD_UNGROUP:
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
            case Cluster::CMD_SEND_TO_GROUP:
                $raw = (bool)($data['flag'] & Cluster::FLAG_NOT_CALL_ENCODE);
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
            // Получение информации о членах группы
            case Cluster::CMD_GET_CLIENT_SESSIONS_BY_GROUP:
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
            // Получение количества членов группы
            case Cluster::CMD_GET_CLIENT_COUNT_BY_GROUP:
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
            // Получение всех client_id, привязанных к UID
            case Cluster::CMD_GET_CLIENT_ID_BY_UID:
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
     * Обработчик закрытия соединения с Business Worker
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
     * Регистрация внутреннего адреса Gateway в Register
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
     * Обработка ping для всех клиентских соединений
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
        $gateway_data = Cluster::$empty;
        $gateway_data['cmd'] = Cluster::CMD_PING;
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
