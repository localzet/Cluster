<?php
/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 *
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as published
 *              by the Free Software Foundation, either version 3 of the License, or
 *              (at your option) any later version.
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *              GNU Affero General Public License for more details.
 *
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 *              For any questions, please contact <creator@localzet.com>
*/

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
                $msg = 'SendBufferToServer failed: the send buffer may have overflowed.';
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
                $msg = 'SendBufferToServer failed: no Business Worker connection is ready.';
                static::log($msg);
            }
            $connection->destroy();
            return false;
        }
        return true;
    }
}
