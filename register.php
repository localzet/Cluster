<?php

use localzet\Cluster\Register;

require_once __DIR__ . '/vendor/autoload.php';

$register = new Register('text://0.0.0.0:1236');
$register->name = 'Register';
$register->secretKey = 'your-secret-key-here';

if (!defined('GLOBAL_START')) {
    \localzet\Server::runAll();
}

