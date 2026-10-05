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
/**
 * Запуск всех компонентов Cluster
 * 
 * Использование:
 *   php start.php start    - запуск
 *   php start.php stop     - остановка
 *   php start.php restart  - перезапуск
 *   php start.php reload   - перезагрузка (hot reload)
 *   php start.php status   - статус
 */

define('GLOBAL_START', 1);

require_once __DIR__ . '/register.php';
require_once __DIR__ . '/gateway.php';
require_once __DIR__ . '/business.php';


\localzet\Server::runAll();
