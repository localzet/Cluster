<?php

declare(strict_types=1);

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 * 
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright (c) 2018-2024 Localzet Group
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

namespace localzet\Cluster;

use localzet\Server\Timer;
use localzet\Server;

/**
 * Регистратор
 * Отвечает за регистрацию и аутентификацию соединений в Localzet Cluster.
 */
class Register extends Server
{

    /**
     * @var string Секретный ключ для аутентификации соединений.
     */
    public $secretKey = '';

    /**
     * @var array Массив соединений со шлюзами.
     */
    protected $_gatewayConnections = array();

    /**
     * @var array Массив соединений со серверами.
     */
    protected $_serverConnections = array();

    /**
     * @var int Время запуска процесса.
     */
    protected $_startTime = 0;

    /**
     * Конструктор класса Register.
     * 
     * @param string|null $socketName Имя сокета.
     * @param array $contextOption Дополнительные параметры контекста.
     */
    public function __construct(string $socketName = null, array $contextOption = [])
    {
        $this->name = 'Регистратор';
        $this->reloadable = false;
        parent::__construct($socketName, $contextOption);
    }

    /**
     * Запускает регистратор.
     */
    public function run(): void
    {
        // Устанавливаем обратный вызов onConnect для установки нового соединения.
        $this->onConnect = array($this, 'onConnect');

        // Устанавливаем обратный вызов onMessage для обработки сообщений.
        $this->onMessage = array($this, 'onMessage');

        // Устанавливаем обратный вызов onClose для закрытия соединения.
        $this->onClose = array($this, 'onClose');

        // Записываем время запуска процесса.
        $this->_startTime = time();

        // Устанавливаем протокол передачи сообщений - Text.
        $this->protocol = '\localzet\Server\Protocols\Text';

        // Отключаем опцию reusePort.
        $this->reusePort = false;

        // Запускаем сервер.
        parent::run();
    }

    /**
     * Устанавливает таймер для закрытия соединения, если не была получена аутентификация.
     *
     * @param \localzet\Server\Connection\ConnectionInterface $connection Соединение.
     */
    public function onConnect($connection)
    {
        $connection->timeout_timerid = Timer::add(10, function () use ($connection) {
            Server::log("Регистратор: Время аутентификации регистратора истекло (" . $connection->getRemoteIp() . ")");
            $connection->close();
        }, null, false);
    }

    /**
     * Обрабатывает полученное сообщение.
     *
     * @param \localzet\Server\Connection\ConnectionInterface $connection Соединение.
     * @param string $buffer Буфер с сообщением.
     */
    public function onMessage($connection, $buffer)
    {
        // Удаляем таймер для закрытия соединения.
        Timer::del($connection->timeout_timerid);

        // Распаковываем данные из JSON.
        $data = @json_decode($buffer, true);

        if (empty($data['event'])) {
            $error = "Невернный запрос для Регистратора. Информация о запросе (IP:" . $connection->getRemoteIp() . ", Буфер запроса:$buffer)";
            Server::log($error);
            return $connection->close($error);
        }

        $event = $data['event'];
        $secret_key = isset($data['secret_key']) ? $data['secret_key'] : '';

        // Проверяем аутентификацию в зависимости от типа соединения.
        switch ($event) {
            case 'gateway_connect':
                // Соединение со шлюзом
                if (empty($data['address'])) {
                    echo "Адрес не найден\n";
                    return $connection->close();
                }
                if ($secret_key !== $this->secretKey) {
                    Server::log("Регистратор: Ключ не соответствует " . var_export($secret_key, true) . " !== " . var_export($this->secretKey, true));
                    return $connection->close();
                }
                $this->_gatewayConnections[$connection->id] = $data['address'];
                $this->broadcastAddresses();
                break;
            case 'server_connect':
                // Соединение с сервером
                if ($secret_key !== $this->secretKey) {
                    Server::log("Регистратор: Ключ не соответствует " . var_export($secret_key, true) . " !== " . var_export($this->secretKey, true));
                    return $connection->close();
                }
                $this->_serverConnections[$connection->id] = $connection;
                $this->broadcastAddresses($connection);
                break;
            case 'ping':
                // Сообщение ping
                break;
            default:
                // Неизвестное событие
                Server::log("Регистратор: неизвестное событие: $event IP: " . $connection->getRemoteIp() . " Буфер: $buffer");
                $connection->close();
        }
    }

    /**
     * Обработчик закрытия соединения.
     *
     * @param \localzet\Server\Connection\ConnectionInterface $connection Соединение.
     */
    public function onClose($connection)
    {
        // Удаляем соединение со списков шлюзов и обновляем список адресов.
        if (isset($this->_gatewayConnections[$connection->id])) {
            unset($this->_gatewayConnections[$connection->id]);
            $this->broadcastAddresses();
        }

        // Удаляем соединение со списков серверов.
        if (isset($this->_serverConnections[$connection->id])) {
            unset($this->_serverConnections[$connection->id]);
        }
    }

    /**
     * Отправляет сообщение с адресами шлюзов всем серверам.
     *
     * @param \localzet\Server\Connection\ConnectionInterface|null $connection Соединение, для которого отправляется сообщение.
     */
    public function broadcastAddresses($connection = null)
    {
        $data = array(
            'event' => 'broadcast_addresses',
            'addresses' => array_unique(array_values($this->_gatewayConnections)),
        );

        $buffer = json_encode($data);

        if ($connection) {
            // Если указано конкретное соединение, отправляем ему сообщение.
            $connection->send($buffer);
            return;
        }

        // Отправляем сообщение всем серверам.
        foreach ($this->_serverConnections as $con) {
            $con->send($buffer);
        }
    }
}
