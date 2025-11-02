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

/** localzet Server */

use localzet\Timer;
use localzet\Server;
use localzet\Server\Connection\TcpConnection;
use localzet\Server\Connection\AsyncTcpConnection;

/** RootX Cluster */

use localzet\Cluster\Lib\Context;
use localzet\Cluster\Protocols\Cluster;

/**
 *
 * Business 用于处理Gateway转发来的数据
 *
 *
 */
class Business extends Server
{
    /**
     * 保存与 gateway 的连接 connection 对象
     *
     * @var array
     */
    public $gatewayConnections = array();

    /**
     * 注册中心地址
     *
     * @var string|array
     */
    public $registerAddress = '127.0.0.1:1236';

    /**
     * 事件处理类，默认是 Event 类
     *
     * @var string
     */
    public $eventHandler = 'Events';

    /**
     * 秘钥
     *
     * @var string
     */
    public $secretKey = '';

    /**
     * businessServer进程将消息转发给gateway进程的发送缓冲区大小
     *
     * @var int
     */
    public $sendToGatewayBufferSize = 10240000;

    /**
     * 保存用户设置的 server 启动回调
     *
     * @var callable|null
     */
    protected $_onServerStart = null;

    /**
     * 保存用户设置的 serverReload 回调
     *
     * @var callable|null
     */
    protected $_onServerReload = null;

    /**
     * 保存用户设置的 serverStop 回调
     *
     * @var callable|null
     */
    protected $_onServerStop = null;

    /**
     * 到注册中心的连接
     *
     * @var AsyncTcpConnection
     */
    protected $_registerConnection = null;

    /**
     * 处于连接状态的 gateway 通讯地址
     *
     * @var array
     */
    protected $_connectingGatewayAddresses = array();

    /**
     * 所有 geteway 内部通讯地址
     *
     * @var array
     */
    protected $_gatewayAddresses = array();

    /**
     * 等待连接个 gateway 地址
     *
     * @var array
     */
    protected $_waitingConnectGatewayAddresses = array();

    /**
     * Event::onConnect 回调
     *
     * @var callable|null
     */
    protected $_eventOnConnect = null;

    /**
     * Event::onMessage 回调
     *
     * @var callable|null
     */
    protected $_eventOnMessage = null;

    /**
     * Event::onClose 回调
     *
     * @var callable|null
     */
    protected $_eventOnClose = null;

    /**
     * websocket回调
     *
     * @var null
     */
    protected $_eventOnWebSocketConnect = null;

    /**
     * SESSION 版本缓存
     *
     * @var array
     */
    protected $_sessionVersion = array();

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
    public function __construct($socket_name = '', $context_option = array())
    {
        parent::__construct($socket_name, $context_option);
        $backrace                = debug_backtrace();
        $this->_autoloadRootPath = dirname($backrace[0]['file']);
    }

    /**
     * {@inheritdoc}
     */
    public function run(): void
    {
        $this->_onServerStart  = $this->onServerStart;
        $this->_onServerReload = $this->onServerReload;
        $this->_onServerStop = $this->onServerStop;
        $this->onServerStop   = array($this, 'onServerStop');
        $this->onServerStart   = array($this, 'onServerStart');
        $this->onServerReload  = array($this, 'onServerReload');
        parent::run();
    }

    /**
     * 当进程启动时一些初始化工作
     *
     * @return void
     */
    protected function onServerStart()
    {
        if (function_exists('opcache_reset')) {
            opcache_reset();
        }

        if (!class_exists('\Protocols\Federation')) {
            class_alias('localzet\Cluster\Protocols\Cluster', 'Protocols\Federation');
        }

        // Backward compatibility alias
        if (!class_exists('localzet\Cluster\BusinessServer')) {
            class_alias(Business::class, 'localzet\Cluster\BusinessServer');
        }

        if (!is_array($this->registerAddress)) {
            $this->registerAddress = array($this->registerAddress);
        }
        $this->connectToRegister();

        \localzet\Cluster\Lib\Gateway::setBusiness($this);
        \localzet\Cluster\Lib\Gateway::$secretKey = $this->secretKey;
        if ($this->_onServerStart) {
            call_user_func($this->_onServerStart, $this);
        }

        if (is_callable($this->eventHandler . '::onServerStart')) {
            call_user_func($this->eventHandler . '::onServerStart', $this);
        }

        // 设置回调
        if (is_callable($this->eventHandler . '::onConnect')) {
            $this->_eventOnConnect = $this->eventHandler . '::onConnect';
        }

        if (is_callable($this->eventHandler . '::onMessage')) {
            $this->_eventOnMessage = $this->eventHandler . '::onMessage';
        } else {
            echo "Waring: {$this->eventHandler}::onMessage is not callable\n";
        }

        if (is_callable($this->eventHandler . '::onClose')) {
            $this->_eventOnClose = $this->eventHandler . '::onClose';
        }

        if (is_callable($this->eventHandler . '::onWebSocketConnect')) {
            $this->_eventOnWebSocketConnect = $this->eventHandler . '::onWebSocketConnect';
        }
    }

    /**
     * onServerReload 回调
     *
     * @param Server $server
     */
    protected function onServerReload($server)
    {
        // 防止进程立刻退出
        $server->reloadable = false;
        // 延迟 0.05 秒退出，避免 Business 瞬间全部退出导致没有可用的 Business 进程
        Timer::add(0.05, array('localzet\Server', 'stopAll'));
        // 执行用户定义的 onServerReload 回调
        if ($this->_onServerReload) {
            call_user_func($this->_onServerReload, $this);
        }
    }

    /**
     * 当进程关闭时一些清理工作
     *
     * @return void
     */
    protected function onServerStop()
    {
        if ($this->_onServerStop) {
            call_user_func($this->_onServerStop, $this);
        }
        if (is_callable($this->eventHandler . '::onServerStop')) {
            call_user_func($this->eventHandler . '::onServerStop', $this);
        }
    }

    /**
     * 连接服务注册中心
     * 
     * @return void
     */
    public function connectToRegister()
    {
        foreach ($this->registerAddress as $register_address) {
            $register_connection = new AsyncTcpConnection("text://{$register_address}");
            $secret_key = $this->secretKey;
            $register_connection->onConnect = function () use ($register_connection, $secret_key, $register_address) {
                $register_connection->send('{"event":"server_connect","secret_key":"' . $secret_key . '"}');
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
            $register_connection->onMessage = array($this, 'onRegisterConnectionMessage');
            $register_connection->connect();
        }
    }


    /**
     * 当注册中心发来消息时
     *
     * @return void
     */
    public function onRegisterConnectionMessage($register_connection, $data)
    {
        $data = json_decode($data, true);
        if (!isset($data['event'])) {
            echo "Received bad data from Register\n";
            return;
        }
        $event = $data['event'];
        switch ($event) {
            case 'broadcast_addresses':
                if (!is_array($data['addresses'])) {
                    echo "Received bad data from Register. Addresses empty\n";
                    return;
                }
                $addresses               = $data['addresses'];
                $this->_gatewayAddresses = array();
                foreach ($addresses as $addr) {
                    $this->_gatewayAddresses[$addr] = $addr;
                }
                $this->checkGatewayConnections($addresses);
                break;
            default:
                echo "Receive bad event:$event from Register.\n";
        }
    }

    /**
     * 当 gateway 转发来数据时
     *
     * @param TcpConnection $connection
     * @param mixed         $data
     */
    public function onGatewayMessage($connection, $data)
    {
        $cmd = $data['cmd'];
        if ($cmd === Cluster::CMD_PING) {
            return;
        }
        // 上下文数据
        Context::$client_ip     = $data['client_ip'];
        Context::$client_port   = $data['client_port'];
        Context::$local_ip      = $data['local_ip'];
        Context::$local_port    = $data['local_port'];
        Context::$connection_id = $data['connection_id'];
        Context::$client_id     = Context::addressToClientId(
            $data['local_ip'],
            $data['local_port'],
            $data['connection_id']
        );
        // $_SERVER 变量
        $_SERVER = array(
            'REMOTE_ADDR'       => long2ip($data['client_ip']),
            'REMOTE_PORT'       => $data['client_port'],
            'GATEWAY_ADDR'      => long2ip($data['local_ip']),
            'GATEWAY_PORT'      => $data['gateway_port'],
            'GATEWAY_CLIENT_ID' => Context::$client_id,
        );
        // 检查session版本，如果是过期的session数据则拉取最新的数据
        if ($cmd !== Cluster::CMD_ON_CLOSE && isset($this->_sessionVersion[Context::$client_id]) && $this->_sessionVersion[Context::$client_id] !== crc32($data['ext_data'])) {
            $_SESSION = Context::$old_session = \localzet\Cluster\Lib\Gateway::getSession(Context::$client_id);
            $this->_sessionVersion[Context::$client_id] = crc32($data['ext_data']);
        } else {
            if (!isset($this->_sessionVersion[Context::$client_id])) {
                $this->_sessionVersion[Context::$client_id] = crc32($data['ext_data']);
            }
            // 尝试解析 session
            if ($data['ext_data'] != '') {
                Context::$old_session = $_SESSION = Context::sessionDecode($data['ext_data']);
            } else {
                Context::$old_session = $_SESSION = null;
            }
        }

        // 尝试执行 Event::onConnection、Event::onMessage、Event::onClose
        switch ($cmd) {
            case Cluster::CMD_ON_CONNECT:
                if ($this->_eventOnConnect) {
                    call_user_func($this->_eventOnConnect, Context::$client_id);
                }
                break;
            case Cluster::CMD_ON_MESSAGE:
                if ($this->_eventOnMessage) {
                    call_user_func($this->_eventOnMessage, Context::$client_id, $data['body']);
                }
                break;
            case Cluster::CMD_ON_CLOSE:
                unset($this->_sessionVersion[Context::$client_id]);
                if ($this->_eventOnClose) {
                    call_user_func($this->_eventOnClose, Context::$client_id);
                }
                break;
            case Cluster::CMD_ON_WEBSOCKET_CONNECT:
                if ($this->_eventOnWebSocketConnect) {
                    call_user_func($this->_eventOnWebSocketConnect, Context::$client_id, $data['body']);
                }
                break;
        }

        // session 必须是数组
        if ($_SESSION !== null && !is_array($_SESSION)) {
            throw new \Exception('$_SESSION must be an array. But $_SESSION=' . var_export($_SESSION, true) . ' is not array.');
        }

        // 判断 session 是否被更改
        if ($_SESSION !== Context::$old_session && $cmd !== Cluster::CMD_ON_CLOSE) {
            $session_str_now = $_SESSION !== null ? Context::sessionEncode($_SESSION) : '';
            \localzet\Cluster\Lib\Gateway::setSocketSession(Context::$client_id, $session_str_now);
            $this->_sessionVersion[Context::$client_id] = crc32($session_str_now);
        }

        Context::clear();
    }

    /**
     * 当与 Gateway 的连接断开时触发
     *
     * @param TcpConnection $connection
     * @return  void
     */
    public function onGatewayClose($connection)
    {
        $addr = $connection->remoteAddr;
        unset($this->gatewayConnections[$addr], $this->_connectingGatewayAddresses[$addr]);
        if (isset($this->_gatewayAddresses[$addr]) && !isset($this->_waitingConnectGatewayAddresses[$addr])) {
            Timer::add(1, array($this, 'tryToConnectGateway'), array($addr), false);
            $this->_waitingConnectGatewayAddresses[$addr] = $addr;
        }
    }

    /**
     * 尝试连接 Gateway 内部通讯地址
     *
     * @param string $addr
     */
    public function tryToConnectGateway($addr)
    {
        if (!isset($this->gatewayConnections[$addr]) && !isset($this->_connectingGatewayAddresses[$addr]) && isset($this->_gatewayAddresses[$addr])) {
            $gateway_connection                    = new AsyncTcpConnection("Federation://$addr");
            $gateway_connection->remoteAddr        = $addr;
            $gateway_connection->onConnect         = array($this, 'onConnectGateway');
            $gateway_connection->onMessage         = array($this, 'onGatewayMessage');
            $gateway_connection->onClose           = array($this, 'onGatewayClose');
            $gateway_connection->onError           = array($this, 'onGatewayError');
            $gateway_connection->maxSendBufferSize = $this->sendToGatewayBufferSize;
            if (TcpConnection::$defaultMaxSendBufferSize == $gateway_connection->maxSendBufferSize) {
                $gateway_connection->maxSendBufferSize = 50 * 1024 * 1024;
            }
            $gateway_data         = Cluster::$empty;
            $gateway_data['cmd']  = Cluster::CMD_SERVER_CONNECT;
            $gateway_data['body'] = json_encode(array(
                'server_key' => "{$this->name}:{$this->id}",
                'secret_key' => $this->secretKey,
            ));
            $gateway_connection->send($gateway_data);
            $gateway_connection->connect();
            $this->_connectingGatewayAddresses[$addr] = $addr;
        }
        unset($this->_waitingConnectGatewayAddresses[$addr]);
    }

    /**
     * 检查 gateway 的通信端口是否都已经连
     * 如果有未连接的端口，则尝试连接
     *
     * @param array $addresses_list
     */
    public function checkGatewayConnections($addresses_list)
    {
        if (empty($addresses_list)) {
            return;
        }
        foreach ($addresses_list as $addr) {
            if (!isset($this->_waitingConnectGatewayAddresses[$addr])) {
                $this->tryToConnectGateway($addr);
            }
        }
    }

    /**
     * 当连接上 gateway 的通讯端口时触发
     * 将连接 connection 对象保存起来
     *
     * @param TcpConnection $connection
     * @return void
     */
    public function onConnectGateway($connection)
    {
        $this->gatewayConnections[$connection->remoteAddr] = $connection;
        unset($this->_connectingGatewayAddresses[$connection->remoteAddr], $this->_waitingConnectGatewayAddresses[$connection->remoteAddr]);
    }

    /**
     * 当与 gateway 的连接出现错误时触发
     *
     * @param TcpConnection $connection
     * @param int           $error_no
     * @param string        $error_msg
     */
    public function onGatewayError($connection, $error_no, $error_msg)
    {
        echo "GatewayConnection Error : $error_no ,$error_msg\n";
    }

    /**
     * 获取所有 Gateway 内部通讯地址
     *
     * @return array
     */
    public function getAllGatewayAddresses()
    {
        return $this->_gatewayAddresses;
    }
}
