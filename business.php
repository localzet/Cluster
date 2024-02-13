
<?php

use localzet\Server;
use localzet\Cluster\BusinessServer;

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/Events.php';

// bussinessWorker 进程
$worker = new BusinessServer();
// worker名称
$worker->name = 'ChatBusinessWorker';
// bussinessWorker进程数量
$worker->count = 4;
// 服务注册地址
$worker->registerAddress = '127.0.0.1:8000';

$worker->onMessage = function ($client_id, $message) {
    Server::log(print_r($message, true));
};

// 如果不是在根目录启动，则运行runAll方法
if (!defined('GLOBAL_START')) {
    Server::runAll();
}
