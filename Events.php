<?php

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 *
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as published
 *              by the Free Software Foundation, either version 3 of the License, or
 *              (at your option) any later version.
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *              GNU Affero General Public License for more details.
 *
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 *              For any questions, please contact <creator@localzet.com>
*/

use localzet\Server;

class Events
{
    public static function onServerStart($server)
    {
        Server::log('onServerStart');
    }

    public static function onConnect($client_id)
    {
        Server::log('onConnect');
    }

    public static function onWebSocketConnect($client_id, $data)
    {
        Server::log('onWebSocketConnect');
    }

    public static function onMessage($client_id, $message)
    {
        Server::log($message);
        // echo $message;
        // Log::debug($message);
        // throw new Exception($message);
        // Client::sendToClient($client_id, "Возвращаю \"$message\"");
    }

    public static function onClose($client_id)
    {
        Server::log('onClose');
    }
}
