<?php

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright (c) 2026 Localzet Group
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

use localzet\Server;
use localzet\Cluster\Gateway;

require_once __DIR__ . '/bootstrap.php';

$gateway = new Gateway("websocket://127.0.0.1:7273");

$gateway->name = 'ChatGateway';

$gateway->count = 4;

$gateway->lanIp = '127.0.0.1';

$gateway->startPort = 2800;

$gateway->pingInterval = 10;

$gateway->pingData = '{"type":"ping"}';

$gateway->registerAddress = '127.0.0.1:1236';
$gateway->secretKey = $clusterSecret;

// Обработчик подключения клиента
$gateway->onConnect = function ($connection) {
    Server::log('onConnect');

    // Обработчик WebSocket handshake
    $connection->onWebSocketConnect = function ($connection, $http_header) {
        Server::log('onWebSocketConnect');
        // Можно проверить источник соединения через $_SERVER['HTTP_ORIGIN']
        // и закрыть соединение, если источник не разрешен
    };

    // Обработчик сообщений от клиента
    $connection->onMessage = function ($client_id, $message) {
        Server::log($message);
    };
};

// Обработчик сообщений от клиентов
$gateway->onMessage = function ($client_id, $message) {
    Server::log(print_r($message, true));
};

if (!defined('GLOBAL_START')) {
    Server::runAll();
}
