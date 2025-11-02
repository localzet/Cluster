<?php

use localzet\Server;
use localzet\Cluster\Gateway;

require_once __DIR__ . '/vendor/autoload.php';

$gateway = new Gateway("Websocket://0.0.0.0:7273");

$gateway->name = 'ChatGateway';

$gateway->count = 4;

$gateway->lanIp = '127.0.0.1';

$gateway->startPort = 2800;

$gateway->pingInterval = 10;

$gateway->pingData = '{"type":"ping"}';

$gateway->registerAddress = '127.0.0.1:1236';
$gateway->secretKey = 'your-secret-key-here';

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
