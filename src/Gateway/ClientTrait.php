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
        // Сохранение заголовка пакета для внутренней коммуникации (кеширование для производительности)
        $connection->gatewayHeader = array(
            'local_ip' => ip2long($this->lanIp),
            'local_port' => $this->lanPort,
            'client_ip' => ip2long($connection->getRemoteIp()),
            'client_port' => $connection->getRemotePort(),
            'gateway_port' => $this->_gatewayPort,
            'connection_id' => $connection->id,
            'flag' => 0,
        );
        // Инициализация сессии соединения
        $connection->session = '';
        // Счетчик неотвеченных ping (-1 означает недавнюю активность клиента)
        $connection->pingNotResponseCount = -1;
        // Размер буфера отправки для этого соединения
        $connection->maxSendBufferSize = $this->sendToClientBufferSize;
        // Сохранение объекта соединения клиента
        $this->_clientConnections[$connection->id] = $connection;

        // Вызов пользовательского callback onConnect, если задан
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
        // Уведомление Business Worker о закрытии соединения
        $this->sendToServer(Cluster::CMD_ON_CLOSE, $connection);
        unset($this->_clientConnections[$connection->id]);
        // Очистка данных привязки UID
        if (!empty($connection->uid)) {
            $uid = $connection->uid;
            unset($this->_uidConnections[$uid][$connection->id]);
            if (empty($this->_uidConnections[$uid])) {
                unset($this->_uidConnections[$uid]);
            }
        }
        // Очистка данных групп
        if (!empty($connection->groups)) {
            foreach ($connection->groups as $group) {
                unset($this->_groupConnections[$group][$connection->id]);
                if (empty($this->_groupConnections[$group])) {
                    unset($this->_groupConnections[$group]);
                }
            }
        }
        // Вызов callback onClose
        if ($this->_onClose) {
            call_user_func($this->_onClose, $connection);
        }
    }

}