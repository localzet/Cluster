<?php

use localzet\Server;
use localzet\Cluster\Business;

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/Events.php';

// Создание Business Worker процесса
$worker = new Business();
$worker->name = 'ChatBusinessWorker';
$worker->count = 4;
$worker->registerAddress = '127.0.0.1:1236';
$worker->secretKey = 'your-secret-key-here';
$worker->eventHandler = 'Events';

$worker->onMessage = function ($client_id, $message) {
    Server::log(print_r($message, true));
};

if (!defined('GLOBAL_START')) {
    Server::runAll();
}
