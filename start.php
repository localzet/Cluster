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
