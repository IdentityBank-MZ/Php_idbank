<?php
# * ********************************************************************* *
# *                                                                       *
# *   Driver for IDBank                                                   *
# *   This file is part of idbank. This project may be found at:          *
# *   https://github.com/IdentityBank/Php_idbank.                         *
# *                                                                       *
# *   Copyright (C) 2020 by Identity Bank. All Rights Reserved.           *
# *   https://www.identitybank.eu - You belong to you                     *
# *                                                                       *
# *   This program is free software: you can redistribute it and/or       *
# *   modify it under the terms of the GNU Affero General Public          *
# *   License as published by the Free Software Foundation, either        *
# *   version 3 of the License, or (at your option) any later version.    *
# *                                                                       *
# *   This program is distributed in the hope that it will be useful,     *
# *   but WITHOUT ANY WARRANTY; without even the implied warranty of      *
# *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the        *
# *   GNU Affero General Public License for more details.                 *
# *                                                                       *
# *   You should have received a copy of the GNU Affero General Public    *
# *   License along with this program. If not, see                        *
# *   https://www.gnu.org/licenses/.                                      *
# *                                                                       *
# * ********************************************************************* *

################################################################################
# Namespace                                                                    #
################################################################################

namespace idb\idbank;

################################################################################
# Include(s)                                                                   #
################################################################################

include_once 'simplelog.inc';

################################################################################
# Use(s)                                                                       #
################################################################################

use Exception;
use idbyii2\helpers\Localization;
use xmz\simplelog\SimpleLogLevel;
use xmz\simplelog\SNLog as Log;
use function xmz\simplelog\registerLogger;

################################################################################
# Class(es)                                                                    #
################################################################################

abstract class IdBankClient
{

    /**
     * @var string
     */
    private static $logPath = "/var/log/p57b/";

    /**
     * @var null
     */
    public $validateServiceIdbIdFunction = null;
    /**
     * @var $service - We supporting 'business' and 'people' services via IDB API
     */
    protected $service = null;
    protected $accountName = null;
    protected $page = null;
    protected $pageSize = 25;
    private $errors = [];
    private $maxBufferSize = 4096;
    private $host = null;
    private $port = null;
    private $configuration = null;

    /**
     * IdBankClient constructor.
     *
     * @param $service - We supporting 'business' and 'people' services via IDB API
     * @param $accountName
     */
    public function __construct($service, $accountName)
    {
        $this->service = $service;
        $this->accountName = $accountName;
    }

    /**
     * @param $service - We supporting 'business' and 'people' services via IDB API
     * @param $accountName
     *
     * @return IdBankClient|null
     */
    public static abstract function model($service, $accountName);

    /**
     * @param $host
     * @param $port
     */
    public function setConnection($host, $port, $configuration = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->configuration = $configuration;
    }

    /* ---------------------------- General --------------------------------- */

    /**
     * @return array
     */
    public function getErrors()
    {
        return $this->errors;
    }

    /**
     * @return string|null
     */
    public function createAccountMetadata()
    {
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "createAccountMetadata"
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param      $action
     * @param null $validateAttributes
     */
    protected function validateServiceIdbId($action, $validateAttributes = null)
    {
        if ($this->validateServiceIdbIdFunction) {
            $this->validateServiceIdbIdFunction->validate(
                $this->accountName,
                $this->service,
                $action,
                $validateAttributes
            );
        }
    }

    /* ----------------------------- Metadata ------------------------------- */

    /**
     * @param $response
     *
     * @return mixed
     */
    public abstract function parseResponse($response);

    /**
     * @param $query
     *
     * @return bool|string|null
     */
    protected function execute($query)
    {
        try {
            $this->logRequestQuery($query);
            $query = json_encode($query);

            if (!empty($this->host)) {
                $this->host = gethostbyname($this->host);
            }

            if (empty($query) || empty($this->host) || empty($this->port)) {
                return null;
            }

            // ***
            $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
            // ***

            if ($socket === false) {
                $this->errors[] = "socket_create() failed: reason: " . socket_strerror(socket_last_error());
            }

            // ***
            $result = socket_connect($socket, $this->host, $this->port);
            // ***

            if ($result === false) {
                $this->errors[] = "socket_connect() failed. Reason: ($result) " . socket_strerror(
                        socket_last_error($socket)
                    );
            }

            if (empty($this->configuration['Security'])) {
                $queryResult = $this->executeRequestNone($socket, $query);
            } elseif (strtoupper($this->configuration['Security']['type']) === 'TOKEN') {
                $queryResult = $this->executeRequestToken($socket, $query);
            } elseif (strtoupper($this->configuration['Security']['type']) === 'CERTIFICATE') {
                $queryResult = $this->executeRequestCertificate($socket, $query);
            } else {
                $queryResult = $this->executeRequestNone($socket, $query);
            }

            // ***
            socket_close($socket);
            // ***

            if (empty($this->errors)) {
                return $queryResult;
            }
        } catch (Exception $e) {
            error_log('Problem processing your query.');
            error_log(json_encode(['host' => $this->host, 'port' => $this->port]));
            if (!empty($e) and !empty($e->getMessage())) {
                error_log($e->getMessage());
            }
        }

        return null;
    }

    /**
     * @param $query
     */
    private function logRequestQuery($query)
    {
        if (SimpleLogLevel::get() < SimpleLogLevel::DEBUG) {
            return;
        }
        $logName = "p57b.idbank.query.log";
        $logPath = self::$logPath . $logName;
        registerLogger($logName, $logPath);

        $pid = getmypid();
        Log::debug(
            $logName,
            "$pid - " .
            "[Q|" . json_encode($query) . "]"
        );
    }

    /**
     * @param $socket
     * @param $query
     *
     * @return string|null
     */
    public function executeRequestNone($socket, $query)
    {
        $queryResult = null;
        try {
            socket_write($socket, $query, strlen($query));

            $queryResult = '';
            while ($result = socket_read($socket, $this->maxBufferSize)) {
                $queryResult .= $result;
            }
        } catch (Exception $e) {
            $queryResult = null;
        }

        return $queryResult;
    }

    /* --------------------------- Access data ------------------------------ */

    /**
     * @param $socket
     * @param $query
     *
     * @return bool|string|null
     */
    public function executeRequestToken($socket, $query)
    {
        $queryResult = null;
        if (!empty($this->configuration['socketTimeout'])) {
            socket_set_option(
                $socket,
                SOL_SOCKET,
                SO_RCVTIMEO,
                ['sec' => $this->configuration['socketTimeout'], 'usec' => 0]
            );
            socket_set_option(
                $socket,
                SOL_SOCKET,
                SO_SNDTIMEO,
                ['sec' => $this->configuration['socketTimeout'], 'usec' => 0]
            );
        }
        try {

            $dataChecksum = md5($query);
            $dataLength = strlen($query);
            $dataChecksumLength = strlen($dataChecksum);
            $size = $dataLength + $dataChecksumLength;
            $size = pack('P', $size);
            $protocolVersion = 1;
            $protocolVersion = pack('V', $protocolVersion);
            $token = $this->configuration['Security']['token'];
            $token = str_pad($token, $this->configuration['Security']['tokenSizeBytes'], " ", STR_PAD_RIGHT);
            $id = time();
            $id = pack('P', $id);
            $dataChecksumType = str_pad('MD5', 8);

            socket_write($socket, $protocolVersion, strlen($protocolVersion));
            socket_write($socket, $token, strlen($token));
            socket_write($socket, $size, strlen($size));
            socket_write($socket, $id, strlen($id));
            socket_write($socket, $dataChecksumType, strlen($dataChecksumType));
            socket_write($socket, $dataChecksum, strlen($dataChecksum));
            socket_write($socket, $query, strlen($query));

            $version = '';
            while ($result = socket_read($socket, 4)) {
                $version .= $result;
                if (4 <= strlen($version)) {
                    break;
                }
            }
            if (!empty($version)) {
                $version = unpack('V', $version);
                $version = intval($version);
            }
            if ($version != 1) {
                return null;
            }
            $token = '';
            while ($result = socket_read($socket, $this->configuration['Security']['tokenSizeBytes'])) {
                $token .= $result;
                if ($this->configuration['Security']['tokenSizeBytes'] <= strlen($token)) {
                    break;
                }
            }
            $size = '';
            while ($result = socket_read($socket, 8)) {
                $size .= $result;
                if (8 <= strlen($size)) {
                    break;
                }
            }
            if (!empty($size)) {
                $size = unpack('P', $size);
            }
            $id = '';
            while ($result = socket_read($socket, 8)) {
                $id .= $result;
                if (8 <= strlen($id)) {
                    break;
                }
            }
            if (!empty($id)) {
                $id = unpack('P', $id);
            }
            $checksumType = '';
            while ($result = socket_read($socket, 8)) {
                $checksumType .= $result;
                if (8 <= strlen($checksumType)) {
                    break;
                }
            }
            $checksumType = trim($checksumType);

            $queryResult = '';
            while ($result = socket_read($socket, $this->maxBufferSize)) {
                $queryResult .= $result;
                if ($size <= strlen($queryResult)) {
                    break;
                }
            }
            $checksum = substr($queryResult, 0, 32);
            $queryResult = substr($queryResult, 32);
            if (strtoupper($checksumType) === 'MD5') {
                $dataChecksum = md5($queryResult);
            } else {
                $dataChecksum = null;
            }
            if (strtolower($checksum) !== $dataChecksum) {
                $queryResult = null;
            }
        } catch (Exception $e) {
            $queryResult = null;
        }

        return $queryResult;
    }

    /**
     * @param $socket
     * @param $query
     *
     * @return null
     */
    public function executeRequestCertificate($socket, $query)
    {
        $queryResult = null;
        try {
        } catch (Exception $e) {
            $queryResult = null;
        }

        return $queryResult;
    }

    /**
     * @return string|null
     * @throws Exception
     */
    public function getAccountMetadata()
    {
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "getAccountMetadata"
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $metadata
     *
     * @return mixed
     * @throws Exception
     */
    public function setAccountMetadata($metadata)
    {
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "setAccountMetadata",
            "metadata" => $metadata
        ];
        $this->logMetadata($this->service, $this->accountName, $metadata);

        return $this->parseResponse($this->execute($query));
    }

    /* ---------------------------- Relation -------------------------------- */

    /**
     * @param $service
     * @param $accountName
     * @param $metadata
     *
     * @throws Exception
     */
    private function logMetadata($service, $accountName, $metadata)
    {
        $logName = "p57b.metadata.log";
        $logPath = self::$logPath . $logName;
        $time = Localization::getDateTimeNumberString();
        registerLogger($logName, $logPath);

        $pid = getmypid();
        Log::fatal(
            $logName,
            "$pid - " .
            "[T|" . $time . "]." .
            "[S|" . $service . "]." .
            "[A|" . $accountName . "]." .
            "[M|" . json_encode($metadata) . "]"
        );
    }

    /**
     * @return mixed
     */
    public function deleteAccountMetadata()
    {
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "deleteAccountMetadata",
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param      $idbId
     * @param null $businessDbId
     *
     * @return string|null
     */
    public function delete($idbId, $businessDbId = null)
    {
        if ($this->service !== 'business') {
            $this->validateServiceIdbId(__FUNCTION__, ['idbId' => $idbId]);
        }
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "deleteItem",
            "businessDbId" => $businessDbId,
            "idbId" => $idbId
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param      $idbId
     * @param null $dataTypes - Specify what data you would like to get or NULL to get all
     * @param null $businessDbId
     *
     * @return string|null
     */
    public function get($idbId, $dataTypes = null, $businessDbId = null)
    {
        if ($this->service !== 'business') {
            $this->validateServiceIdbId(__FUNCTION__, ['idbId' => $idbId]);
        }
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "getItem",
            "idbId" => $idbId,
            "businessDbId" => $businessDbId,
            "DataTypes" => $dataTypes
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $idbId
     * @param $data
     * @param $businessDbId
     *
     * @return string|null
     */
    public function put($idbId, $data, $businessDbId = null)
    {
        if ($this->service !== 'business') {
            $this->validateServiceIdbId(__FUNCTION__, ['idbId' => $idbId]);
        }
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $businessDbId,
            "query" => "putItem",
            "idbId" => $idbId,
            "data" => $data
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $idbId
     * @param $data
     *
     * @param $businessDbId
     *
     * @return string|null
     */
    public function update($idbId, $data, $businessDbId = null)
    {
        if ($this->service !== 'business') {
            $this->validateServiceIdbId(__FUNCTION__, ['idbId' => $idbId]);
        }
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $businessDbId,
            "query" => "updateItem",
            "idbId" => $idbId,
            "data" => $data
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $businessId
     * @param $peopleId
     *
     * @return string|null
     */
    public function checkRelationBusiness2People($businessId, $peopleId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId,
                'peopleId' => $peopleId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "checkRelationBusiness2People",
            "businessId" => $businessId,
            "peopleId" => $peopleId
        ];

        return $this->parseResponse($this->execute($query));
    }

    /* ---------------------------- Internal -------------------------------- */

    /**
     * @param $businessId
     * @param $peopleId
     *
     * @return string|null
     */
    public function setRelationBusiness2People($businessId, $peopleId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId,
                'peopleId' => $peopleId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "setRelationBusiness2People",
            "businessId" => $businessId,
            "peopleId" => $peopleId
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $businessId
     * @param $peopleId
     *
     * @return string|null
     */
    public function deleteRelationBusiness2People($businessId, $peopleId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId,
                'peopleId' => $peopleId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "deleteRelationBusiness2People",
            "businessId" => $businessId,
            "peopleId" => $peopleId
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $businessId
     *
     * @return mixed
     */
    public function deleteRelationsForBusiness($businessId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "deleteRelationBusiness2People",
            "businessId" => $businessId . '.uid.%'
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $peopleId
     *
     * @return mixed
     */
    public function deleteRelationsForPerson($peopleId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'peopleId' => $peopleId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "deleteRelationBusiness2People",
            "peopleId" => $peopleId . '%'
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $businessId
     *
     * @return string|null
     */
    public function getRelatedPeoples($businessId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "getRelatedPeoples",
            "businessId" => $businessId,
            "PaginationConfig" => [
                'Page' => 0,
                'PageSize' => 0,
            ]
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $businessId
     *
     * @return string|null
     */
    public function getRelatedPeoplesBusinessId($businessId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'businessId' => $businessId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "getRelatedPeoples",
            "selectAll" => true,
            "businessId" => $businessId,
            "PaginationConfig" => [
                'Page' => 0,
                'PageSize' => 0,
            ]
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @param $peopleId
     *
     * @return string|null
     */
    public function getRelatedBusinesses($peopleId)
    {
        $this->service = 'relation';
        $this->validateServiceIdbId(
            __FUNCTION__,
            [
                'peopleId' => $peopleId
            ]
        );
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "query" => "getRelatedBusinesses",
            "peopleId" => $peopleId,
            "PaginationConfig" => [
                'Page' => 0,
                'PageSize' => 0,
            ]
        ];

        return $this->parseResponse($this->execute($query));
    }
}

################################################################################
#                                End of file                                   #
################################################################################
