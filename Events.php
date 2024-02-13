<?php

/**
 * @package     MultiCore FrameX Plugin
 * @link        https://localzet.gitbook.io
 * @author      localzet <creator@localzet.ru>
 * @copyright   Copyright (c) 2018-2022 RootX Group
 * @license     https://www.localzet.ru/license GNU GPLv3 License
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
