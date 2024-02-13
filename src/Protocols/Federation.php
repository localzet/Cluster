<?php declare(strict_types=1);

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

namespace localzet\Cluster\Protocols;

/**
 * Двоичный протокол между MultiCore и WebCore
 *
 * struct Federation
 * {
 *     unsigned int                 pack_len,
 *     unsigned char                cmd,
 *     unsigned int                 local_ip,
 *     unsigned short               local_port,
 *     unsigned int                 client_ip,
 *     unsigned short               client_port,
 *     unsigned int                 connection_id,
 *     unsigned char                flag,
 *     unsigned short               gateway_port,
 *     unsigned int                 ext_len,
 *     char[ext_len]                ext_data,
 *     char[pack_length-HEAD_LEN]   body
 * }
 * NCNnNnNCnN
 */
class Federation
{
    /** Для WebCore: Новое соединение */
    const CMD_ON_CONNECT = 1;

    /** Для WebCore: Новое событие */
    const CMD_ON_MESSAGE = 3;

    /** Для WebCore: Закрытие соединения */
    const CMD_ON_CLOSE = 4;

    /** Для MultiCore: Отправить данные одному */
    const CMD_SEND_TO_ONE = 5;

    /** Для MultiCore: Отправить данные всем */
    const CMD_SEND_TO_ALL = 6;

    /**
     * Для MultiCore:
     *  1. Если есть событие, подключение будет закрыто сразу после отправки
     *  2. Если нет событий, соедиение будет немедленно закрыто.
     */
    const CMD_KICK = 7;

    /** Для MultiCore: Немедленно закрыть соединение */
    const CMD_DESTROY = 8;

    /** Для MultiCore: Отправить данные и обновить сессию */
    const CMD_UPDATE_SESSION = 9;

    /** Получить сессии */
    const CMD_GET_ALL_CLIENT_SESSIONS = 10;

    /** Определение онлайн */
    const CMD_IS_ONLINE = 11;

    /** Связать client_id с UID */
    const CMD_BIND_UID = 12;

    /** Отвязать client_id от UID */
    const CMD_UNBIND_UID = 13;

    /** Отправить данные в UID */
    const CMD_SEND_TO_UID = 14;

    /** Получить client_id по UID */
    const CMD_GET_CLIENT_ID_BY_UID = 15;

    /** Вступить в группу */
    const CMD_JOIN_GROUP = 20;

    /** Покинуть группу */
    const CMD_LEAVE_GROUP = 21;

    /** Отправить сообщение группе */
    const CMD_SEND_TO_GROUP = 22;

    /** Получить члена группы */
    const CMD_GET_CLIENT_SESSIONS_BY_GROUP = 23;

    /** Получить количество соединений группы */
    const CMD_GET_CLIENT_COUNT_BY_GROUP = 24;

    /** Поиск с условиями */
    const CMD_SELECT = 25;

    /** Получить идентификатор группы */
    const CMD_GET_GROUP_ID_LIST = 26;

    /** Отменить пакет */
    const CMD_UNGROUP = 27;

    /** Соединение MultiCore и WebCore */
    const CMD_SERVER_CONNECT = 200;

    /** Сердцебиение */
    const CMD_PING = 201;

    /** Подключение MultiCore Client */
    const CMD_GATEWAY_CLIENT_CONNECT = 202;

    /** Получить сессию по client_id */
    const CMD_GET_SESSION_BY_CLIENT_ID = 203;

    /** Установить сессию */
    const CMD_SET_SESSION = 204;

    /** Соединение с WebSocket */
    const CMD_ON_WEBSOCKET_CONNECT = 205;

    /** Тело - это количество */
    const FLAG_BODY_IS_SCALAR = 0x01;

    /** Уведомление Gateway При отправке метода Encode Protocol */
    const FLAG_NOT_CALL_ENCODE = 0x02;

    /**
     * 包头长度
     *
     * @var int
     */
    const HEAD_LEN = 28;

    public static $empty = array(
        'cmd'           => 0,
        'local_ip'      => 0,
        'local_port'    => 0,
        'client_ip'     => 0,
        'client_port'   => 0,
        'connection_id' => 0,
        'flag'          => 0,
        'gateway_port'  => 0,
        'ext_data'      => '',
        'body'          => '',
    );

    /**
     * 返回包长度
     *
     * @param string $buffer
     * @return int return current package length
     */
    public static function input($buffer)
    {
        if (strlen($buffer) < self::HEAD_LEN) {
            return 0;
        }

        $data = unpack("Npack_len", $buffer);
        return $data['pack_len'];
    }

    /**
     * 获取整个包的 buffer
     *
     * @param mixed $data
     * @return string
     */
    public static function encode($data)
    {
        $flag = (int)is_scalar($data['body']);
        if (!$flag) {
            $data['body'] = serialize($data['body']);
        }
        $data['flag'] |= $flag;
        $ext_len      = strlen($data['ext_data']??'');
        $package_len  = self::HEAD_LEN + $ext_len + strlen($data['body']);
        return pack("NCNnNnNCnN", $package_len,
            $data['cmd'], $data['local_ip'],
            $data['local_port'], $data['client_ip'],
            $data['client_port'], $data['connection_id'],
            $data['flag'], $data['gateway_port'],
            $ext_len) . $data['ext_data'] . $data['body'];
    }

    /**
     * 从二进制数据转换为数组
     *
     * @param string $buffer
     * @return array
     */
    public static function decode($buffer)
    {
        $data = unpack("Npack_len/Ccmd/Nlocal_ip/nlocal_port/Nclient_ip/nclient_port/Nconnection_id/Cflag/ngateway_port/Next_len",
            $buffer);
        if ($data['ext_len'] > 0) {
            $data['ext_data'] = substr($buffer, self::HEAD_LEN, $data['ext_len']);
            if ($data['flag'] & self::FLAG_BODY_IS_SCALAR) {
                $data['body'] = substr($buffer, self::HEAD_LEN + $data['ext_len']);
            } else {
                $data['body'] = unserialize(substr($buffer, self::HEAD_LEN + $data['ext_len']));
            }
        } else {
            $data['ext_data'] = '';
            if ($data['flag'] & self::FLAG_BODY_IS_SCALAR) {
                $data['body'] = substr($buffer, self::HEAD_LEN);
            } else {
                $data['body'] = unserialize(substr($buffer, self::HEAD_LEN));
            }
        }
        return $data;
    }
}