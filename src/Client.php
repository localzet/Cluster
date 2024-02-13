<?php

/**
 * @package     MultiCore Server
 * @link        https://localzet.gitbook.io
 * @author      localzet <creator@localzet.ru>
 * @copyright   Copyright (c) 2018-2022 RootX Group
 * @license     https://www.localzet.ru/license GNU GPLv3 License
 */

namespace localzet\Cluster;

/** localzet WebCore */

use localzet\Server\Connection\TcpConnection;

/** RootX MultiCore */

use localzet\Cluster\Protocols\Federation;
use localzet\Cluster\Lib\Context;

/**
 * RootX MultiCore Client
 */
class Client
{
    /**
     * Экземпляр шлюза
     *
     * @var object
     */
    protected static $businessServer = null;

    /**
     * Адрес регистрационного центра
     *
     * @var string|array
     */
    public static $registerAddress = '127.0.0.1:1236';

    /**
     * Секретный ключ
     * @var string
     */
    public static $secretKey = '';

    /**
     * Время соединения
     * @var int
     */
    public static $connectTimeout = 3;

    /**
     * Длительное соединение?
     * @var bool
     */
    public static $persistentConnection = false;

    /**
     * Чтобы очистить зарегистрированный кеш адреса
     * @var bool
     */
    public static $addressesCacheDisable = false;

    /**
     * Подключение ко всем клиентам (или к списку в $client_id_array)
     *
     * @param string $message               Сообщение клиенту
     * @param array  $client_id_array       Массив идентификаторов клиентов (отправка списку)
     * @param array  $exclude_client_id     Массив идентификаторов клиентов (отправка всем, кроме)
     * @param bool   $raw                   Отправить исходные данные (то есть метод кодирования протокола шлюза не вызывает протокола шлюза))
     * @return void
     * @throws \Exception
     */
    public static function sendToAll(
        $message,
        $client_id_array = null,
        $exclude_client_id = null,
        $raw = false
    ) {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_SEND_TO_ALL;
        $gateway_data['body'] = $message;
        if ($raw) {
            $gateway_data['flag'] |= Federation::FLAG_NOT_CALL_ENCODE;
        }

        if ($exclude_client_id) {
            if (!is_array($exclude_client_id)) {
                $exclude_client_id = [$exclude_client_id];
            }
            if ($client_id_array) {
                $exclude_client_id = array_flip($exclude_client_id);
            }
        }

        if ($client_id_array) {
            if (!is_array($client_id_array)) {
                echo new \Exception(
                    '$client_id_array:' . var_export($client_id_array, true)
                );
                return;
            }
            $data_array = [];
            foreach ($client_id_array as $client_id) {
                if (isset($exclude_client_id[$client_id])) {
                    continue;
                }
                $address = Context::clientIdToAddress($client_id);
                if ($address) {
                    $key =
                        long2ip($address['local_ip']) .
                        ":{$address['local_port']}";
                    $data_array[$key][$address['connection_id']] =
                        $address['connection_id'];
                }
            }
            foreach ($data_array as $addr => $connection_id_list) {
                $the_gateway_data = $gateway_data;
                $the_gateway_data['ext_data'] = json_encode([
                    'connections' => $connection_id_list,
                ]);
                static::sendToGateway($addr, $the_gateway_data);
            }
            return;
        } elseif (empty($client_id_array) && is_array($client_id_array)) {
            return;
        }

        if (!$exclude_client_id) {
            return static::sendToAllGateway($gateway_data);
        }

        $address_connection_array = static::clientIdArrayToAddressArray(
            $exclude_client_id
        );

        // Если есть экземпляр Business - отправляй данные через длительное соединение
        if (static::$businessServer) {
            foreach (static::$businessServer->gatewayConnections
                as $address => $gateway_connection) {
                $gateway_data['ext_data'] = isset(
                    $address_connection_array[$address]
                )
                    ? json_encode([
                        'exclude' => $address_connection_array[$address],
                    ])
                    : '';
                /** @var TcpConnection $gateway_connection */
                $gateway_connection->send($gateway_data);
            }
        }
        // Получаем адреса шлюзов из Регистрационного центра и отправляем данные через них
        else {
            $all_addresses = static::getAllGatewayAddressesFromRegister();
            foreach ($all_addresses as $address) {
                $gateway_data['ext_data'] = isset(
                    $address_connection_array[$address]
                )
                    ? json_encode([
                        'exclude' => $address_connection_array[$address],
                    ])
                    : '';
                static::sendToGateway($address, $gateway_data);
            }
        }
    }

    /**
     * Отправить сообщение клиенту
     *
     * @param string    $client_id
     * @param string $message
     * @param bool $raw
     * @return bool
     */
    public static function sendToClient($client_id, $message, $raw = false)
    {
        return static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_SEND_TO_ONE,
            $message,
            '',
            $raw
        );
    }

    /**
     * Отправить сообщение текущему клиенту
     *
     * @param string $message
     * @param bool $raw
     * @return bool
     */
    public static function sendToCurrentClient($message, $raw = false)
    {
        return static::sendCmdAndMessageToClient(
            null,
            Federation::CMD_SEND_TO_ONE,
            $message,
            '',
            $raw
        );
    }

    /**
     * Определить в сети ли UID
     *
     * @param string $uid
     * @return int 0|1
     */
    public static function isUidOnline($uid)
    {
        return (int) static::getClientIdByUid($uid);
    }

    /**
     * Определить в сети ли client_id
     *
     * @param string $client_id
     * @return int 0|1
     */
    public static function isOnline($client_id)
    {
        $address_data = Context::clientIdToAddress($client_id);
        if (!$address_data) {
            return 0;
        }
        $address =
            long2ip($address_data['local_ip']) .
            ":{$address_data['local_port']}";
        if (isset(static::$businessServer)) {
            if (!isset(static::$businessServer->gatewayConnections[$address])) {
                return 0;
            }
        }
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_IS_ONLINE;
        $gateway_data['connection_id'] = $address_data['connection_id'];
        return (int) static::sendAndRecv($address, $gateway_data);
    }

    /**
     * Получить все клиентские сессии или сессии группы (client_id - ключ)
     *
     * @param string $group
     * @return array
     */
    public static function getAllClientSessions($group = '')
    {
        $gateway_data = Federation::$empty;
        if (!$group) {
            $gateway_data['cmd'] = Federation::CMD_GET_ALL_CLIENT_SESSIONS;
        } else {
            $gateway_data['cmd'] =
                Federation::CMD_GET_CLIENT_SESSIONS_BY_GROUP;
            $gateway_data['ext_data'] = $group;
        }
        $status_data = [];
        $all_buffer_array = static::getBufferFromAllGateway($gateway_data);
        foreach ($all_buffer_array as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $data) {
                if ($data) {
                    foreach ($data as $connection_id => $session_buffer) {
                        $client_id = Context::addressToClientId(
                            $local_ip,
                            $local_port,
                            $connection_id
                        );
                        if ($client_id === Context::$client_id) {
                            $status_data[$client_id] = (array) $_SESSION;
                        } else {
                            $status_data[$client_id] = $session_buffer
                                ? Context::sessionDecode($session_buffer)
                                : [];
                        }
                    }
                }
            }
        }
        return $status_data;
    }

    /**
     * Получить сессии группы клиентов
     *
     * @param string $group
     *
     * @return array
     */
    public static function getClientSessionsByGroup($group)
    {
        if (static::isValidGroupId($group)) {
            return static::getAllClientSessions($group);
        }
        return [];
    }

    /**
     * Получить кол-во всех клиентов
     *
     * @return int
     */
    public static function getAllClientCount()
    {
        return static::getClientCountByGroup();
    }

    /**
     * Получить кол-во клиентов группы
     *
     * @param string $group
     * @return int
     */
    public static function getClientCountByGroup($group = '')
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_GET_CLIENT_COUNT_BY_GROUP;
        $gateway_data['ext_data'] = $group;
        $total_count = 0;
        $all_buffer_array = static::getBufferFromAllGateway($gateway_data);
        foreach ($all_buffer_array as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $count) {
                if ($count) {
                    $total_count += $count;
                }
            }
        }
        return $total_count;
    }

    /**
     * Получить список client_id группы
     *
     * @param string $group
     * @return array
     */
    public static function getClientIdListByGroup($group)
    {
        if (!static::isValidGroupId($group)) {
            return [];
        }

        $data = static::select(
            ['uid'],
            ['groups' => is_array($group) ? $group : [$group]]
        );
        $client_id_map = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    $client_id = Context::addressToClientId(
                        $local_ip,
                        $local_port,
                        $connection_id
                    );
                    $client_id_map[$client_id] = $client_id;
                }
            }
        }
        return $client_id_map;
    }

    /**
     * Получить все client_id
     *
     * @return array
     */
    public static function getAllClientIdList()
    {
        return static::formatClientIdFromGatewayBuffer(static::select(['uid']));
    }

    /**
     * Форматировать client_id
     *
     * @param $data
     * @return array
     */
    protected static function formatClientIdFromGatewayBuffer($data)
    {
        $client_id_list = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    $client_id = Context::addressToClientId(
                        $local_ip,
                        $local_port,
                        $connection_id
                    );
                    $client_id_list[$client_id] = $client_id;
                }
            }
        }
        return $client_id_list;
    }

    /**
     * Получить список client_id по UID
     *
     * @param string $uid
     * @return array
     */
    public static function getClientIdByUid($uid)
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_GET_CLIENT_ID_BY_UID;
        $gateway_data['ext_data'] = $uid;
        $client_list = [];
        $all_buffer_array = static::getBufferFromAllGateway($gateway_data);
        foreach ($all_buffer_array as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $connection_id_array) {
                if ($connection_id_array) {
                    foreach ($connection_id_array as $connection_id) {
                        $client_list[] = Context::addressToClientId(
                            $local_ip,
                            $local_port,
                            $connection_id
                        );
                    }
                }
            }
        }
        return $client_list;
    }

    /**
     * Получить список UID в группе
     *
     * @param string $group
     * @return array
     */
    public static function getUidListByGroup($group)
    {
        if (!static::isValidGroupId($group)) {
            return [];
        }

        $group = is_array($group) ? $group : [$group];
        $data = static::select(['uid'], ['groups' => $group]);
        $uid_map = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    if (!empty($info['uid'])) {
                        $uid_map[$info['uid']] = $info['uid'];
                    }
                }
            }
        }
        return $uid_map;
    }

    /**
     * Получить кол-во UID в группе
     *
     * @param string $group
     * @return int
     */
    public static function getUidCountByGroup($group)
    {
        if (static::isValidGroupId($group)) {
            return count(static::getUidListByGroup($group));
        }
        return 0;
    }

    /**
     * Получить список всех UID
     *
     * @return array
     */
    public static function getAllUidList()
    {
        $data = static::select(['uid']);
        $uid_map = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    if (!empty($info['uid'])) {
                        $uid_map[$info['uid']] = $info['uid'];
                    }
                }
            }
        }
        return $uid_map;
    }

    /**
     * Получить кол-во всех UID
     * @return int
     */
    public static function getAllUidCount()
    {
        return count(static::getAllUidList());
    }

    /**
     * Получить UID по client_id
     *
     * @param $client_id
     * @return mixed
     */
    public static function getUidByClientId($client_id)
    {
        $data = static::select(['uid'], ['client_id' => [$client_id]]);
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $info) {
                    return $info['uid'];
                }
            }
        }
    }

    /**
     * Получить список всех group_id
     *
     * @return array
     */
    public static function getAllGroupIdList()
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_GET_GROUP_ID_LIST;
        $group_id_list = [];
        $all_buffer_array = static::getBufferFromAllGateway($gateway_data);
        foreach ($all_buffer_array as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $group_id_array) {
                if (is_array($group_id_array)) {
                    foreach ($group_id_array as $group_id) {
                        if (!isset($group_id_list[$group_id])) {
                            $group_id_list[$group_id] = $group_id;
                        }
                    }
                }
            }
        }
        return $group_id_list;
    }

    /**
     * Получить количество UID всех групп, то есть количество пользователей каждой группы
     *
     * @return array
     */
    public static function getAllGroupUidCount()
    {
        $group_uid_map = static::getAllGroupUidList();
        $group_uid_count_map = [];
        foreach ($group_uid_map as $group_id => $uid_list) {
            $group_uid_count_map[$group_id] = count($uid_list);
        }
        return $group_uid_count_map;
    }

    /**
     * Получить список UID всех групп
     *
     * @return array
     */
    public static function getAllGroupUidList()
    {
        $data = static::select(['uid', 'groups']);
        $group_uid_map = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    if (empty($info['uid']) || empty($info['groups'])) {
                        break;
                    }
                    $uid = $info['uid'];
                    foreach ($info['groups'] as $group_id) {
                        if (!isset($group_uid_map[$group_id])) {
                            $group_uid_map[$group_id] = [];
                        }
                        $group_uid_map[$group_id][$uid] = $uid;
                    }
                }
            }
        }
        return $group_uid_map;
    }

    /**
     * Получить списки client_id всех групп
     *
     * @return array
     */
    public static function getAllGroupClientIdList()
    {
        $data = static::select(['groups']);
        $group_client_id_map = [];
        foreach ($data as $local_ip => $buffer_array) {
            foreach ($buffer_array as $local_port => $items) {
                // $items = ['connection_id' => ['uid' => x, 'group' => [x, x..], 'session' => [..]], 'client_id' => [..], ..];
                foreach ($items as $connection_id => $info) {
                    if (empty($info['groups'])) {
                        break;
                    }
                    $client_id = Context::addressToClientId(
                        $local_ip,
                        $local_port,
                        $connection_id
                    );
                    foreach ($info['groups'] as $group_id) {
                        if (!isset($group_client_id_map[$group_id])) {
                            $group_client_id_map[$group_id] = [];
                        }
                        $group_client_id_map[$group_id][$client_id] = $client_id;
                    }
                }
            }
        }
        return $group_client_id_map;
    }

    /**
     * Получить кол-во client_id всех групп, то есть получить количество соединений каждой группы
     *
     * @return array
     */
    public static function getAllGroupClientIdCount()
    {
        $group_client_map = static::getAllGroupClientIdList();
        $group_client_count_map = [];
        foreach ($group_client_map as $group_id => $client_id_list) {
            $group_client_count_map[$group_id] = count($client_id_list);
        }
        return $group_client_count_map;
    }

    /**
     * Поиск данных с условиями
     *
     * @param array $fields
     * @param array $where
     * @return array
     */
    protected static function select(
        $fields = ['session', 'uid', 'groups'],
        $where = []
    ) {
        $t = microtime(true);
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_SELECT;
        $gateway_data['ext_data'] = ['fields' => $fields, 'where' => $where];
        $gateway_data_list = [];

        if (isset($where['client_id'])) {
            $client_id_list = $where['client_id'];
            unset($gateway_data['ext_data']['where']['client_id']);
            $gateway_data['ext_data']['where']['connection_id'] = [];
            foreach ($client_id_list as $client_id) {
                $address_data = Context::clientIdToAddress($client_id);
                if (!$address_data) {
                    continue;
                }
                $address =
                    long2ip($address_data['local_ip']) .
                    ":{$address_data['local_port']}";
                if (!isset($gateway_data_list[$address])) {
                    $gateway_data_list[$address] = $gateway_data;
                }
                $gateway_data_list[$address]['ext_data']['where']['connection_id'][$address_data['connection_id']] =
                    $address_data['connection_id'];
            }

            foreach ($gateway_data_list as $address => $item) {
                $gateway_data_list[$address]['ext_data'] = json_encode(
                    $item['ext_data']
                );
            }

            if (count($where) !== 1) {
                $gateway_data['ext_data'] = json_encode(
                    $gateway_data['ext_data']
                );
                foreach (static::getAllGatewayAddress() as $address) {
                    if (!isset($gateway_data_list[$address])) {
                        $gateway_data_list[$address] = $gateway_data;
                    }
                }
            }

            $data = static::getBufferFromSomeGateway($gateway_data_list);
        } else {
            $gateway_data['ext_data'] = json_encode($gateway_data['ext_data']);
            $data = static::getBufferFromAllGateway($gateway_data);
        }

        return $data;
    }

    /**
     * Генерировать пакеты для проверки легитимности этого клиента
     *
     * @return string
     */
    protected static function generateAuthBuffer()
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_MULTI_CLIENT_CONNECT;
        $gateway_data['body'] = json_encode([
            'secret_key' => static::$secretKey,
        ]);
        return Federation::encode($gateway_data);
    }

    /**
     * Получить буфер от шлюза
     *
     * @param array $gateway_data_array
     * @return array
     * @throws \Exception
     */
    protected static function getBufferFromSomeGateway($gateway_data_array)
    {
        $gateway_buffer_array = [];
        $auth_buffer = static::$secretKey ? static::generateAuthBuffer() : '';
        foreach ($gateway_data_array as $address => $gateway_data) {
            if ($auth_buffer) {
                $gateway_buffer_array[$address] =
                    $auth_buffer . Federation::encode($gateway_data);
            } else {
                $gateway_buffer_array[$address] = Federation::encode(
                    $gateway_data
                );
            }
        }
        return static::getBufferFromGateway($gateway_buffer_array);
    }

    /**
     * Получить буфер от всех шлюзов
     *
     * @param string $gateway_data
     * @return array
     * @throws \Exception
     */
    protected static function getBufferFromAllGateway($gateway_data)
    {
        $addresses = static::getAllGatewayAddress();
        $gateway_buffer_array = [];
        $gateway_buffer = Federation::encode($gateway_data);
        $gateway_buffer = static::$secretKey
            ? static::generateAuthBuffer() . $gateway_buffer
            : $gateway_buffer;
        foreach ($addresses as $address) {
            $gateway_buffer_array[$address] = $gateway_buffer;
        }

        return static::getBufferFromGateway($gateway_buffer_array);
    }

    /**
     * Получить адреса всех шлюзов
     *
     * @return array
     * @throws \Exception
     */
    protected static function getAllGatewayAddress()
    {
        if (isset(static::$businessServer)) {
            $addresses = static::$businessServer->getAllGatewayAddresses();
            if (empty($addresses)) {
                throw new \Exception(
                    'Business::getAllGatewayAddresses вернул пустой ответ'
                );
            }
        } else {
            $addresses = static::getAllGatewayAddressesFromRegister();
            if (empty($addresses)) {
                return [];
            }
        }
        return $addresses;
    }

    /**
     * Получить буфер от шлюза
     * @param $gateway_buffer_array
     * @return array
     */
    protected static function getBufferFromGateway($gateway_buffer_array)
    {
        $client_array = $status_data = $client_address_map = $receive_buffer_array = $recv_length_array = [];

        foreach ($gateway_buffer_array as $address => $gateway_buffer) {
            $client = stream_socket_client(
                "tcp://$address",
                $errno,
                $errmsg,
                static::$connectTimeout
            );
            if (
                $client &&
                strlen($gateway_buffer) ===
                stream_socket_sendto($client, $gateway_buffer)
            ) {
                $socket_id = (int) $client;
                $client_array[$socket_id] = $client;
                $client_address_map[$socket_id] = explode(':', $address);
                $receive_buffer_array[$socket_id] = '';
            }
        }
        // 5 секунд
        $timeout = 5;
        $time_start = microtime(true);
        // Запрос на получение
        while (count($client_array) > 0) {
            $write = $except = [];
            $read = $client_array;
            if (@stream_select($read, $write, $except, $timeout)) {
                foreach ($read as $client) {
                    $socket_id = (int) $client;
                    $buffer = stream_socket_recvfrom($client, 65535);
                    if ($buffer !== '' && $buffer !== false) {
                        $receive_buffer_array[$socket_id] .= $buffer;
                        $receive_length = strlen(
                            $receive_buffer_array[$socket_id]
                        );
                        if (
                            empty($recv_length_array[$socket_id]) &&
                            $receive_length >= 4
                        ) {
                            $recv_length_array[$socket_id] = current(
                                unpack('N', $receive_buffer_array[$socket_id])
                            );
                        }
                        if (
                            !empty($recv_length_array[$socket_id]) &&
                            $receive_length >=
                            $recv_length_array[$socket_id] + 4
                        ) {
                            unset($client_array[$socket_id]);
                        }
                    } elseif (feof($client)) {
                        unset($client_array[$socket_id]);
                    }
                }
            }
            if (microtime(true) - $time_start > $timeout) {
                break;
            }
        }
        $format_buffer_array = [];
        foreach ($receive_buffer_array as $socket_id => $buffer) {
            $local_ip = ip2long($client_address_map[$socket_id][0]);
            $local_port = $client_address_map[$socket_id][1];
            $format_buffer_array[$local_ip][$local_port] = unserialize(
                substr($buffer, 4)
            );
        }
        return $format_buffer_array;
    }

    /**
     * Закрыть клиента с сообщением
     *
     * @param string $client_id
     * @param string $message
     * @return void
     */
    public static function closeClient($client_id, $message = null)
    {
        if ($client_id === Context::$client_id) {
            return static::closeCurrentClient($message);
        }
        // Не для текущего пользователя, используйте адрес из хранилища
        else {
            $address_data = Context::clientIdToAddress($client_id);
            if (!$address_data) {
                return false;
            }
            $address =
                long2ip($address_data['local_ip']) .
                ":{$address_data['local_port']}";
            return static::kickAddress(
                $address,
                $address_data['connection_id'],
                $message
            );
        }
    }

    /**
     * Закрыть текущего клиента с сообщением
     *
     * @param string $message
     * @return bool
     * @throws \Exception
     */
    public static function closeCurrentClient($message = null)
    {
        if (!Context::$connection_id) {
            throw new \Exception(
                'MultiClient::closeCurrentClient не может быть вызван из асинхронного контекста'
            );
        }
        $address = long2ip(Context::$local_ip) . ':' . Context::$local_port;
        return static::kickAddress($address, Context::$connection_id, $message);
    }

    /**
     * Разорвать соединение
     *
     * @param string $client_id
     * @return bool
     */
    public static function destoryClient($client_id)
    {
        if ($client_id === Context::$client_id) {
            return static::destoryCurrentClient();
        }
        // Не для текущего пользователя, используйте адрес из хранилища
        else {
            $address_data = Context::clientIdToAddress($client_id);
            if (!$address_data) {
                return false;
            }
            $address =
                long2ip($address_data['local_ip']) .
                ":{$address_data['local_port']}";
            return static::destroyAddress(
                $address,
                $address_data['connection_id']
            );
        }
    }

    /**
     * Разорвать текущее соединение
     *
     * @return bool
     * @throws \Exception
     */
    public static function destoryCurrentClient()
    {
        if (!Context::$connection_id) {
            throw new \Exception(
                'MultiClient::destoryCurrentClient не может быть вызван из асинхронного контекста'
            );
        }
        $address = long2ip(Context::$local_ip) . ':' . Context::$local_port;
        return static::destroyAddress($address, Context::$connection_id);
    }

    /**
     * Привязать client_id к UID
     *
     * @param string        $client_id
     * @param int|string $uid
     * @return void
     */
    public static function bindUid($client_id, $uid)
    {
        static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_BIND_UID,
            '',
            $uid
        );
    }

    /**
     * Отвязать client_id от uid
     *
     * @param string        $client_id
     * @param int|string $uid
     * @return void
     */
    public static function unbindUid($client_id, $uid)
    {
        static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_UNBIND_UID,
            '',
            $uid
        );
    }

    /**
     * Добавить client_id в группу
     *
     * @param string $client_id
     * @param int|string $group
     * @return void
     */
    public static function joinGroup($client_id, $group)
    {
        static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_JOIN_GROUP,
            '',
            $group
        );
    }

    /**
     * Убрать client_id из группы
     *
     * @param string $client_id
     * @param int|string $group
     *
     * @return void
     */
    public static function leaveGroup($client_id, $group)
    {
        static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_LEAVE_GROUP,
            '',
            $group
        );
    }

    /**
     * Отменить пакет
     *
     * @param int|string $group
     *
     * @return void
     */
    public static function ungroup($group)
    {
        if (!static::isValidGroupId($group)) {
            return false;
        }
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_UNGROUP;
        $gateway_data['ext_data'] = $group;
        return static::sendToAllGateway($gateway_data);
    }

    /**
     * Отправить в UID
     *
     * @param int|string|array $uid
     * @param string           $message
     * @param bool $raw
     *
     * @return void
     */
    public static function sendToUid($uid, $message, $raw = false)
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_SEND_TO_UID;
        $gateway_data['body'] = $message;
        if ($raw) {
            $gateway_data['flag'] |= Federation::FLAG_NOT_CALL_ENCODE;
        }

        if (!is_array($uid)) {
            $uid = [$uid];
        }

        $gateway_data['ext_data'] = json_encode($uid);

        static::sendToAllGateway($gateway_data);
    }

    /**
     * Отправить в группу
     *
     * @param int|string|array $group             Группа (не должно быть 0, '0', false, нулевым массивом и т.д.)
     * @param string           $message           Информация
     * @param array            $exclude_client_id Исключить client_id из адресатов
     * @param bool             $raw               Исходные данные?
     *
     * @return void
     */
    public static function sendToGroup(
        $group,
        $message,
        $exclude_client_id = null,
        $raw = false
    ) {
        if (!static::isValidGroupId($group)) {
            return false;
        }
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_SEND_TO_GROUP;
        $gateway_data['body'] = $message;
        if ($raw) {
            $gateway_data['flag'] |= Federation::FLAG_NOT_CALL_ENCODE;
        }

        if (!is_array($group)) {
            $group = [$group];
        }

        $default_ext_data_buffer = json_encode([
            'group' => $group,
            'exclude' => null,
        ]);
        if (empty($exclude_client_id)) {
            $gateway_data['ext_data'] = $default_ext_data_buffer;
            return static::sendToAllGateway($gateway_data);
        }

        // 分组发送，有排除的client_id，需要将client_id转换成对应gateway进程内的connectionId
        if (!is_array($exclude_client_id)) {
            $exclude_client_id = [$exclude_client_id];
        }

        $address_connection_array = static::clientIdArrayToAddressArray(
            $exclude_client_id
        );
        // 如果有businessServer实例，说明运行在serverman环境中，通过businessServer中的长连接发送数据
        if (static::$businessServer) {
            foreach (static::$businessServer->gatewayConnections
                as $address => $gateway_connection) {
                $gateway_data['ext_data'] = isset(
                    $address_connection_array[$address]
                )
                    ? json_encode([
                        'group' => $group,
                        'exclude' => $address_connection_array[$address],
                    ])
                    : $default_ext_data_buffer;
                /** @var TcpConnection $gateway_connection */
                $gateway_connection->send($gateway_data);
            }
        }
        // 运行在其它环境中，通过注册中心得到gateway地址
        else {
            $addresses = static::getAllGatewayAddressesFromRegister();
            foreach ($addresses as $address) {
                $gateway_data['ext_data'] = isset(
                    $address_connection_array[$address]
                )
                    ? json_encode([
                        'group' => $group,
                        'exclude' => $address_connection_array[$address],
                    ])
                    : $default_ext_data_buffer;
                static::sendToGateway($address, $gateway_data);
            }
        }
    }

    /**
     * 更新 session，框架自动调用，开发者不要调用
     *
     * @param string    $client_id
     * @param string $session_str
     * @return bool
     */
    public static function setSocketSession($client_id, $session_str)
    {
        return static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_SET_SESSION,
            '',
            $session_str
        );
    }

    /**
     * 设置 session，原session值会被覆盖
     *
     * @param string   $client_id
     * @param array $session
     *
     * @return void
     */
    public static function setSession($client_id, array $session)
    {
        if (Context::$client_id === $client_id) {
            $_SESSION = $session;
            Context::$old_session = $_SESSION;
        }
        static::setSocketSession($client_id, Context::sessionEncode($session));
    }

    /**
     * 更新 session，实际上是与老的session合并
     *
     * @param string   $client_id
     * @param array $session
     *
     * @return void
     */
    public static function updateSession($client_id, array $session)
    {
        if (Context::$client_id === $client_id) {
            $_SESSION = array_replace_recursive((array) $_SESSION, $session);
            Context::$old_session = $_SESSION;
        }
        static::sendCmdAndMessageToClient(
            $client_id,
            Federation::CMD_UPDATE_SESSION,
            '',
            Context::sessionEncode($session)
        );
    }

    /**
     * 获取某个client_id的session
     *
     * @param string   $client_id
     * @return mixed false表示出错、null表示用户不存在、array表示具体的session信息
     */
    public static function getSession($client_id)
    {
        $address_data = Context::clientIdToAddress($client_id);
        if (!$address_data) {
            return false;
        }
        $address =
            long2ip($address_data['local_ip']) .
            ":{$address_data['local_port']}";
        if (isset(static::$businessServer)) {
            if (!isset(static::$businessServer->gatewayConnections[$address])) {
                return null;
            }
        }
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_GET_SESSION_BY_CLIENT_ID;
        $gateway_data['connection_id'] = $address_data['connection_id'];
        return static::sendAndRecv($address, $gateway_data);
    }

    /**
     * 向某个用户网关发送命令和消息
     *
     * @param string    $client_id
     * @param int    $cmd
     * @param string $message
     * @param string $ext_data
     * @param bool $raw
     * @return boolean
     */
    protected static function sendCmdAndMessageToClient(
        $client_id,
        $cmd,
        $message,
        $ext_data = '',
        $raw = false
    ) {
        // 如果是发给当前用户则直接获取上下文中的地址
        if ($client_id === Context::$client_id || $client_id === null) {
            $address = long2ip(Context::$local_ip) . ':' . Context::$local_port;
            $connection_id = Context::$connection_id;
        } else {
            $address_data = Context::clientIdToAddress($client_id);
            if (!$address_data) {
                return false;
            }
            $address =
                long2ip($address_data['local_ip']) .
                ":{$address_data['local_port']}";
            $connection_id = $address_data['connection_id'];
        }
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = $cmd;
        $gateway_data['connection_id'] = $connection_id;
        $gateway_data['body'] = $message;
        if (!empty($ext_data)) {
            $gateway_data['ext_data'] = $ext_data;
        }
        if ($raw) {
            $gateway_data['flag'] |= Federation::FLAG_NOT_CALL_ENCODE;
        }

        return static::sendToGateway($address, $gateway_data);
    }

    /**
     * 发送数据并返回
     *
     * @param int   $address
     * @param mixed $data
     * @return bool
     * @throws \Exception
     */
    protected static function sendAndRecv($address, $data)
    {
        $buffer = Federation::encode($data);
        $buffer = static::$secretKey
            ? static::generateAuthBuffer() . $buffer
            : $buffer;
        $client = stream_socket_client(
            "tcp://$address",
            $errno,
            $errmsg,
            static::$connectTimeout
        );
        if (!$client) {
            throw new \Exception(
                "Не могу соединиться с tcp://$address $errmsg"
            );
        }
        if (strlen($buffer) === stream_socket_sendto($client, $buffer)) {
            $timeout = 5;
            // 阻塞读
            stream_set_blocking($client, 1);
            // 1秒超时
            stream_set_timeout($client, 1);
            $all_buffer = '';
            $time_start = microtime(true);
            $pack_len = 0;
            while (1) {
                $buf = stream_socket_recvfrom($client, 655350);
                if ($buf !== '' && $buf !== false) {
                    $all_buffer .= $buf;
                } else {
                    if (feof($client)) {
                        throw new \Exception(
                            "Соедиенние закрыто tcp://$address"
                        );
                    } elseif (microtime(true) - $time_start > $timeout) {
                        break;
                    }
                    continue;
                }
                $recv_len = strlen($all_buffer);
                if (!$pack_len && $recv_len >= 4) {
                    $pack_len = current(unpack('N', $all_buffer));
                }
                // 回复的数据都是以\n结尾
                if (
                    ($pack_len && $recv_len >= $pack_len + 4) ||
                    microtime(true) - $time_start > $timeout
                ) {
                    break;
                }
            }
            // 返回结果
            return unserialize(substr($all_buffer, 4));
        } else {
            throw new \Exception(
                "MultiClient::sendAndRecv($address, \$bufer) Не может отправить даные!",
                502
            );
        }
    }

    /**
     * 发送数据到网关
     *
     * @param string $address
     * @param array  $gateway_data
     * @return bool
     */
    protected static function sendToGateway($address, $gateway_data)
    {
        return static::sendBufferToGateway(
            $address,
            Federation::encode($gateway_data)
        );
    }

    /**
     * 发送buffer数据到网关
     * @param string $address
     * @param string $gateway_buffer
     * @return bool
     */
    protected static function sendBufferToGateway($address, $gateway_buffer)
    {
        // 有$businessServer说明是serverman环境，使用$businessServer发送数据
        if (static::$businessServer) {
            if (!isset(static::$businessServer->gatewayConnections[$address])) {
                return false;
            }
            return static::$businessServer->gatewayConnections[$address]->send(
                $gateway_buffer,
                true
            );
        }
        // 非serverman环境
        $gateway_buffer = static::$secretKey
            ? static::generateAuthBuffer() . $gateway_buffer
            : $gateway_buffer;
        $flag = static::$persistentConnection
            ? STREAM_CLIENT_PERSISTENT | STREAM_CLIENT_CONNECT
            : STREAM_CLIENT_CONNECT;
        $client = stream_socket_client(
            "tcp://$address",
            $errno,
            $errmsg,
            static::$connectTimeout,
            $flag
        );
        return strlen($gateway_buffer) ==
            stream_socket_sendto($client, $gateway_buffer);
    }

    /**
     * 向所有 gateway 发送数据
     *
     * @param string $gateway_data
     * @throws \Exception
     *
     * @return void
     */
    protected static function sendToAllGateway($gateway_data)
    {
        $buffer = Federation::encode($gateway_data);
        // 如果有businessServer实例，说明运行在serverman环境中，通过businessServer中的长连接发送数据
        if (static::$businessServer) {
            foreach (static::$businessServer->gatewayConnections
                as $gateway_connection) {
                /** @var TcpConnection $gateway_connection */
                $gateway_connection->send($buffer, true);
            }
        }
        // 运行在其它环境中，通过注册中心得到gateway地址
        else {
            $all_addresses = static::getAllGatewayAddressesFromRegister();
            foreach ($all_addresses as $address) {
                static::sendBufferToGateway($address, $buffer);
            }
        }
    }

    /**
     * 踢掉某个网关的 socket
     *
     * @param string $address
     * @param int    $connection_id
     * @return bool
     */
    protected static function kickAddress($address, $connection_id, $message)
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_KICK;
        $gateway_data['connection_id'] = $connection_id;
        $gateway_data['body'] = $message;
        return static::sendToGateway($address, $gateway_data);
    }

    /**
     * 销毁某个网关的 socket
     *
     * @param string $address
     * @param int    $connection_id
     * @return bool
     */
    protected static function destroyAddress($address, $connection_id)
    {
        $gateway_data = Federation::$empty;
        $gateway_data['cmd'] = Federation::CMD_DESTROY;
        $gateway_data['connection_id'] = $connection_id;
        return static::sendToGateway($address, $gateway_data);
    }

    /**
     * 将clientid数组转换成address数组
     *
     * @param array $client_id_array
     * @return array
     */
    protected static function clientIdArrayToAddressArray(
        array $client_id_array
    ) {
        $address_connection_array = [];
        foreach ($client_id_array as $client_id) {
            $address_data = Context::clientIdToAddress($client_id);
            if ($address_data) {
                $address =
                    long2ip($address_data['local_ip']) .
                    ":{$address_data['local_port']}";
                $address_connection_array[$address][$address_data['connection_id']] = $address_data['connection_id'];
            }
        }
        return $address_connection_array;
    }

    /**
     * 设置 gateway 实例
     *
     * @param \localzet\Cluster\Business $business_server_instance
     */
    public static function setBusiness($business_server_instance)
    {
        static::$businessServer = $business_server_instance;
    }

    /**
     * 获取通过注册中心获取所有 gateway 通讯地址
     *
     * @return array
     * @throws \Exception
     */
    protected static function getAllGatewayAddressesFromRegister()
    {
        static $addresses_cache, $last_update;
        if (static::$addressesCacheDisable) {
            $addresses_cache = null;
        }
        $time_now = time();
        $expiration_time = 1;
        $register_addresses = (array) static::$registerAddress;
        $client = null;
        if (
            empty($addresses_cache) ||
            $time_now - $last_update > $expiration_time
        ) {
            foreach ($register_addresses as $register_address) {
                set_error_handler(function () {
                });
                $client = stream_socket_client(
                    'tcp://' . $register_address,
                    $errno,
                    $errmsg,
                    static::$connectTimeout
                );
                restore_error_handler();
                if ($client) {
                    break;
                }
            }
            if (!$client) {
                throw new \Exception(
                    'Не могу соединиться с tcp://' .
                        $register_address .
                        ' ' .
                        $errmsg
                );
            }

            fwrite(
                $client,
                '{"event":"server_connect","secret_key":"' .
                    static::$secretKey .
                    '"}' .
                    "\n"
            );
            stream_set_timeout($client, 5);
            $ret = fgets($client, 655350);
            if (!$ret || !($data = json_decode(trim($ret), true))) {
                throw new \Exception(
                    'MultiClient::getAllGatewayAddressesFromRegister. tcp://' .
                        $register_address .
                        ' вернул ' .
                        var_export($ret, true)
                );
            }
            $last_update = $time_now;
            $addresses_cache = $data['addresses'];
        }
        if (!$addresses_cache) {
            throw new \Exception(
                'MultiClient::getAllGatewayAddressesFromRegister() с registerAddress:' .
                    json_encode(static::$registerAddress) .
                    '  вернул ' .
                    var_export($addresses_cache, true)
            );
        }
        return $addresses_cache;
    }

    /**
     * 检查群组id是否合法
     *
     * @param $group
     * @return bool
     */
    protected static function isValidGroupId($group)
    {
        if (empty($group)) {
            echo new \Exception(
                'Группа (' . var_export($group, true) . ') пуста'
            );
            return false;
        }
        return true;
    }
}

if (!class_exists('\Protocols\Federation')) {
    class_alias(
        'localzet\Cluster\Protocols\Federation',
        'Protocols\Federation'
    );
}
