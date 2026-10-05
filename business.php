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
use localzet\Cluster\Business;

require_once __DIR__ . '/bootstrap.php';
require_once __DIR__ . '/Events.php';

// Создание Business Worker процесса
$worker = new Business();
$worker->name = 'ChatBusinessWorker';
$worker->count = 4;
$worker->registerAddress = '127.0.0.1:1236';
$worker->secretKey = $clusterSecret;
$worker->eventHandler = 'Events';

$worker->onMessage = function ($client_id, $message) {
    Server::log(print_r($message, true));
};

if (!defined('GLOBAL_START')) {
    Server::runAll();
}
