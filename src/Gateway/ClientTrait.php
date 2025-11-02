<?php

namespace localzet\Cluster\Gateway;

use localzet\Cluster\Protocols\Cluster;
use localzet\Server\Connection\TcpConnection;

trait ClientTrait
{
    /**
     * 当客户端发来数据时，转发给server处理
     *
     * @param TcpConnection $connection
     * @param mixed $data
     */
    public function onClientMessage(TcpConnection $connection, mixed $data): void
    {
        $connection->pingNotResponseCount = -1;
        $this->sendToServer(Cluster::CMD_ON_MESSAGE, $connection, $data);
    }

    /**
     * 当客户端连接上来时，初始化一些客户端的数据
     * 包括全局唯一的client_id、初始化session等
     *
     * @param TcpConnection $connection
     */
    public function onClientConnect(TcpConnection $connection): void
    {
        $connection->id = self::generateConnectionId();
        // 保存该连接的内部通讯的数据包报头，避免每次重新初始化
        $connection->gatewayHeader = array(
            'local_ip' => ip2long($this->lanIp),
            'local_port' => $this->lanPort,
            'client_ip' => ip2long($connection->getRemoteIp()),
            'client_port' => $connection->getRemotePort(),
            'gateway_port' => $this->_gatewayPort,
            'connection_id' => $connection->id,
            'flag' => 0,
        );
        // 连接的 session
        $connection->session = '';
        // 该连接的心跳参数
        $connection->pingNotResponseCount = -1;
        // 该链接发送缓冲区大小
        $connection->maxSendBufferSize = $this->sendToClientBufferSize;
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

        $this->sendToServer(Cluster::CMD_ON_CONNECT, $connection);
    }

    /**
     * 当客户端关闭时
     *
     * @param TcpConnection $connection
     */
    public function onClientClose(TcpConnection $connection): void
    {
        // 尝试通知 server，触发 Event::onClose
        $this->sendToServer(Cluster::CMD_ON_CLOSE, $connection);
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

}