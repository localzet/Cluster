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

namespace localzet\Cluster\Lib;

use Exception;

/**
 * Контекст содержит текущий пользовательский UID, внутреннюю связь local_ip, local_port, socket_id, client, client_ip и client_port_port
 */
class Context
{
    /**
     * Локальный IP
     *
     * @var ?int
     */
    public static ?int $local_ip = null;

    /**
     * Локальный порт
     *
     * @var ?int
     */
    public static ?int $local_port = null;

    /**
     * Клиентский IP
     *
     * @var ?int
     */
    public static ?int $client_ip = null;

    /**
     * Клиентский порт
     *
     * @var int
     */
    public static $client_port;

    /**
     * Клиентский ID
     *
     * @var string
     */
    public static $client_id;

    /**
     * ID соединений
     *
     * @var int
     */
    public static $connection_id;

    /**
     * Старая сессия
     *
     * @var string
     */
    public static $old_session;

    /**
     * Шифровка сессии (Сериализация)
     *
     * @param mixed $session_data
     * @return string
     */
    public static function sessionEncode($session_data = '')
    {
        if ($session_data !== '') {
            return serialize($session_data);
        }
        return '';
    }

    /**
     * Декодирование сессии (десериализация)
     *
     * @param string $session_buffer
     * @return mixed
     */
    public static function sessionDecode($session_buffer)
    {
        return unserialize($session_buffer);
    }

    /**
     * Чистка контекста
     *
     * @return void
     */
    public static function clear()
    {
        self::$local_ip = self::$local_port = self::$client_ip = self::$client_port =
        self::$client_id = self::$connection_id = self::$old_session = null;
    }

    /**
     * Преобразование адреса в client_id
     *
     * @param int $local_ip
     * @param int $local_port
     * @param int $connection_id
     * @return string
     */
    public static function addressToClientId($local_ip, $local_port, $connection_id)
    {
        return bin2hex(pack('NnN', $local_ip, $local_port, $connection_id));
    }

    /**
     * Преобразование client_id в адрес
     *
     * @param string $client_id
     * @return array
     * @throws Exception
     */
    public static function clientIdToAddress($client_id)
    {
        if (strlen($client_id) !== 20) {
            echo new Exception("client_id $client_id is invalid");
            return false;
        }
        return unpack('Nlocal_ip/nlocal_port/Nconnection_id', pack('H*', $client_id));
    }
}
