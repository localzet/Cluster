<?php

use \GatewayWorker\Gateway;
use \localzet\Server\Autoloader;
use localzet\Server;
use localzet\Cluster\Server as MultiCore;

require_once __DIR__ . '/vendor/autoload.php';

$gateway = new MultiCore("Websocket://0.0.0.0:7273");

$gateway->name = 'ChatGateway';

$gateway->count = 4;

$gateway->lanIp = '127.0.0.1';

$gateway->startPort = 2800;

$gateway->pingInterval = 10;

$gateway->pingData = '{"type":"ping"}';

$gateway->registerAddress = '127.0.0.1:800';

// 当客户端连接上来时，设置连接的onWebSocketConnect，即在websocket握手时的回调
$gateway->onConnect = function ($connection) {
    Server::log('onConnect');

    $connection->onWebSocketConnect = function ($connection, $http_header) {
        Server::log('onWebSocketConnect');
        // 可以在这里判断连接来源是否合法，不合法就关掉连接
        // $_SERVER['HTTP_ORIGIN']标识来自哪个站点的页面发起的websocket链接
        // if($_SERVER['HTTP_ORIGIN'] != 'http://chat.workerman.net')
        // {
        //     $connection->close();
        // }
        // onWebSocketConnect 里面$_GET $_SERVER是可用的
        // var_dump($_GET, $_SERVER);
    };
    $connection->onMessage = function ($client_id, $message) {
        Server::log($message);
        // echo $message;
        // Log::debug($message);
        // throw new Exception($message);
        // Client::sendToClient($client_id, "Возвращаю \"$message\"");
    };
};

$gateway->onMessage = function ($client_id, $message) {
        Server::log(print_r($message, true));
    // echo $message;
    // Log::debug($message);
    // throw new Exception($message);
    // Client::sendToClient($client_id, "Возвращаю \"$message\"");
};

if (!defined('GLOBAL_START')) {
    Server::runAll();
    // Server::log('fff');

}
