<?php

namespace localzet\Cluster\Gateway;

use localzet\Server\Connection\TcpConnection;

trait ServerTrait
{
    /**
     * 发送数据给 server 进程
     *
     * @param int $cmd
     * @param TcpConnection $connection
     * @param mixed $body
     * @return bool
     */
    protected function sendToServer($cmd, $connection, $body = '')
    {
        $gateway_data = $connection->gatewayHeader;
        $gateway_data['cmd'] = $cmd;
        $gateway_data['body'] = $body;
        $gateway_data['ext_data'] = $connection->session;
        if ($this->_serverConnections) {
            // Вызов функции маршрутизации для выбора Business Worker
            /** @var TcpConnection $server_connection */
            $server_connection = call_user_func($this->router, $this->_serverConnections, $connection, $cmd, $body);
            if (false === $server_connection->send($gateway_data)) {
                $msg = "SendBufferToServer fail. May be the send buffer are overflow. See http://doc2.workerman.net/send-buffer-overflow.html";
                static::log($msg);
                return false;
            }
        }
        // Если нет доступных Business Worker
        else {
            // В первые 1-2 секунды после запуска ошибки отправки нормальны (соединения еще не установлены)
            // Логируем только после истечения времени
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
}