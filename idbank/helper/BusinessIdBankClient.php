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
# Use(s)                                                                       #
################################################################################

use Exception;
use idbyii2\helpers\IdbAccountId;
use idbyii2\helpers\Localization;
use idbyii2\models\db\BusinessDatabaseData;
use yii\helpers\ArrayHelper;

################################################################################
# Class(es)                                                                    #
################################################################################

/**
 * Class BusinessIdBankClient
 *
 * @package idb\idbank
 */
class BusinessIdBankClient extends IdBankClient
{

    /**
     * @param $service - We supporting 'business' and 'people' services via IDB API
     * @param $accountName
     *
     * @return BusinessIdBankClient|null
     */
    public static function model($service, $accountName)
    {
        if (!empty($service) && !empty($accountName)) {
            return new self($service, $accountName);
        }

        return null;
    }

    /**
     * @param $dataTypes
     *
     * @return mixed
     * @throws Exception
     */
    public function createAccount($dataTypes)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = BusinessDatabaseData::create($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "query" => "createAccount",
            "DataTypes" => $dataTypes,
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /* ----------------------- Manage business data ------------------------- */

    /**
     * @param $query
     *
     * @return bool|string|null
     */
    protected function execute($query)
    {
        $data = base64_encode(json_encode($query));
        exec("idbconsole idb-task/take-credits '$data'");

        return parent::execute($query);
    }

    /**
     * Parse response json by status code.
     *
     * Parse response json by status code, for now supported:
     * 200, 201,457, 1511
     *
     * @param null|string $response
     *
     * @return mixed
     */
    public function parseResponse($response)
    {
        $response = json_decode($response, true);

        //TODO: Make better solution for return values by statusCode
        if (!empty($response['statusCode'])) {
            switch ($response['statusCode']) {
                case 200:
                    return json_decode($response['result'], true);
                case 201:
                    return true;
                case 457:
                    return 457;
                case 1511:
                default:
                    return null;
            }
        } else {
            return null;
        }

    }

    /**
     * @param $dataTypes
     *
     * @return mixed
     * @throws Exception
     */
    public function recreateAccount($dataTypes)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "dropCreateAccount",
            "DataTypes" => $dataTypes,
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $businessId
     *
     * @return string|null
     * @throws Exception
     */
    private static function getDatabaseNameByBusinessId($businessId)
    {
        $idbDatabaseData = BusinessDatabaseData::getDatabaseNameByBusinessId($businessId);
        if (empty($idbDatabaseData)) {
            throw new Exception('IDB ID business DB accessed before initialization!');
        }

        return $idbDatabaseData;
    }

    /* --------------------------- Access data------------------------------- */

    /**
     * @param $dataTypes
     *
     * @return mixed
     * @throws Exception
     */
    public function updateDataTypes($dataTypes)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "updateDataTypes",
            "DataTypes" => $dataTypes,
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function deleteAccount()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "deleteAccount"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $filterExpression
     * @param $expressionAttributeNames
     * @param $expressionAttributeValues
     *
     * @return mixed
     * @throws Exception
     */
    public function count($filterExpression, $expressionAttributeNames, $expressionAttributeValues)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "query" => "count",
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "FilterExpression" => $filterExpression,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     *
     * @return mixed
     * @throws Exception
     */
    public function countAll(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null
    )
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $parsedIds = IdbAccountId::parse($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "query" => "countAllItems",
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "FilterExpression" => $filterExpression,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues,
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $data
     *
     * @return mixed
     * @throws Exception
     */
    public function putMultiple($data)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "putItems",
            "data" => $data
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $pageSize
     *
     * @return $this
     */
    public function setPageSize($pageSize)
    {
        if (!empty($pageSize) && is_int($pageSize) && ($pageSize > 0)) {
            $this->pageSize = $pageSize;
        }

        return $this;
    }

    /**
     * @param $page
     *
     * @return $this
     */
    public function setPage($page)
    {
        $this->page = $page;

        return $this;
    }

    /**
     * @param      $page
     * @param null $pageSize
     *
     * @return $this
     */
    public function setPagination($page, $pageSize = null)
    {
        $this->page = $page;
        if (!empty($pageSize) && is_int($pageSize) && ($pageSize > 0)) {
            $this->pageSize = $pageSize;
        }

        return $this;
    }

    /**
     * @param      $id
     * @param null $dataTypes
     * @param null $orderByDataTypes
     *
     * @return mixed
     * @throws Exception
     */
    public function findById(
        $id,
        $dataTypes = null,
        $orderByDataTypes = null
    )
    {
        $exp = [
            'o' => '=',
            'l' => '#col',
            'r' => ':col'
        ];

        return $this->find($exp, ['#col' => 'id'], [':col' => $id], $dataTypes, $orderByDataTypes);
    }

    public function findWithBiggerId($id)
    {
        $exp = [
            'o' => '>',
            'l' => '#col',
            'r' => ':col'
        ];

        return $this->find($exp, ['#col' => 'id'], [':col' => $id]);
    }

    /**
     * @return int
     * @throws Exception
     */
    public function findLastId()
    {
        $pageSize = $this->pageSize;
        $this->setPageSize(1);
        $response = $this->find(null, null, null, ['id'], ['id' => 'desc']);
        $this->setPageSize($pageSize);

        if (!empty($response['QueryData']) && !empty($response['QueryData'][0])) {
            return $response['QueryData'][0][0];
        }

        return 0;
    }

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     * @param null $dataTypes
     * @param null $orderByDataTypes
     * @param string $query
     *
     * @return mixed
     * @throws Exception
     */
    public function find(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null,
        $dataTypes = null,
        $orderByDataTypes = null,
        $query = "findItems"
    )
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "query" => $query,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "FilterExpression" => $filterExpression,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues,
            "DataTypes" => $dataTypes,
            "OrderByDataTypes" => $orderByDataTypes,
            "PaginationConfig" => [
                'Page' => $this->page,
                'PageSize' => $this->pageSize,
            ]
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param      $idbId
     * @param null $accountName
     *
     * @return string|null
     * @throws Exception
     */
    public function delete($idbId, $accountName = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        if (empty($accountName)) {
            $accountName = $this->accountName;
        }
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $response = parent::delete($idbId, $accountName);
        $this->accountName = $accountName;

        return $response;
    }

    /**
     * @param      $idbId
     * @param null $dataTypes
     *
     * @param null $accountName
     *
     * @return string|null
     * @throws Exception
     */
    public function get($idbId, $dataTypes = null, $accountName = null)
    {
        $idbId = intval($idbId);
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        if (empty($accountName)) {
            $accountName = $this->accountName;
        }
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $response = parent::get($idbId, $dataTypes, $accountName);
        $this->accountName = $accountName;

        return $response;
    }

    /**
     * @param      $data
     * @param null $notused
     *
     * @return mixed|string|null
     * @throws Exception
     */
    public function put($data, $notused = null, $accountName = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        if (empty($accountName)) {
            $accountName = $this->accountName;
        }
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $response = parent::put($notused, $data, $accountName);
        $this->accountName = $accountName;

        return $response;
    }

    /**
     * @param $idbId
     * @param $data
     *
     * @return string|null
     * @throws Exception
     */
    public function putById($idbId, $data)
    {
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;

        $response = parent::put($idbId, $data);
        $this->accountName = $accountName;

        return $response;
    }

    /**
     * @param      $idbId
     * @param      $data
     * @param null $accountName
     *
     * @return string|null
     * @throws Exception
     */
    public function update($idbId, $data, $accountName = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        if (empty($accountName)) {
            $accountName = $this->accountName;
        }
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $response = parent::update($idbId, $data, $accountName);
        $this->accountName = $accountName;

        return $response;
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function checkIfAccountExists()
    {
        $response = $this->findCountAll(null, null, null, null, null);

        return $response;
    }

    /* -------------------- Change request from people ---------------------- */

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     * @param null $dataTypes
     * @param null $orderByDataTypes
     * @param string $query
     *
     * @return mixed
     * @throws Exception
     */
    public function findCountAll(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null,
        $dataTypes = null,
        $orderByDataTypes = null,
        $query = "findCountAllItems"
    )
    {
        return $this->find(
            $filterExpression,
            $expressionAttributeNames,
            $expressionAttributeValues,
            $dataTypes,
            $orderByDataTypes,
            $query
        );
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function createAccountCR()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "createAccountChangeRequest"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountCR()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "deleteAccountChangeRequest"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * Recreate AccountCR database.
     *
     * @return mixed
     * @throws Exception
     */
    public function recreateAccountCR()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "account" => $this->accountName,
            "query" => "dropCreateAccountChangeRequest",
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "service" => "business"
        ];

        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return array|null
     * @throws Exception
     */
    public function getAllAccountCRs()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "query" => "getAllAccountCRs"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $id
     *
     * @return mixed
     * @throws Exception
     */
    public function getAccountCR($id)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            'id' => $id,
            "query" => "getAccountCR"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $status
     *
     * @return mixed
     * @throws Exception
     */
    public function getAccountCRbyStatus($status)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            'status' => $status,
            "query" => "getAccountCRbyStatus"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param        $userId
     * @param        $data
     * @param        $status
     *
     * @param string $tag
     *
     * @return mixed
     * @throws Exception
     */
    public function addAccountCRbyUserId($userId, $data, $status, $tag = '')
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            'userId' => $userId,
            'data' => $data,
            'tag' => $tag,
            'status' => $status,
            "query" => "addAccountCRbyUserId"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param      $id
     * @param      $tag
     * @param null $status
     *
     * @return mixed
     * @throws Exception
     */
    public function updateAccountCR($id, $tag, $status = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            'id' => $id,
            'tag' => $tag,
            'status' => $status,
            "query" => "updateAccountCR"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     * @param string $query
     *
     * @return mixed
     * @throws Exception
     */
    public function findCountAllCR(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null,
        $query = "findCountAllAccountCRItems"
    )
    {
        return $this->findCR(
            $filterExpression,
            $expressionAttributeNames,
            $expressionAttributeValues,
            $query
        );
    }

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     * @param null $order
     * @param string $query
     *
     * @return mixed
     * @throws Exception
     */
    public function findCR(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null,
        $order = null,
        $query = "findAccountCRItems"
    )
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => $query,
            "FilterExpression" => $filterExpression,
            "OrderByDataTypes" => $order,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues,
            "PaginationConfig" =>
                [
                    'Page' => $this->page,
                    'PageSize' => $this->pageSize,
                ]
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /* -------------------- Send invitations for people ---------------------- */

    /**
     * @param null $filterExpression
     * @param null $expressionAttributeNames
     * @param null $expressionAttributeValues
     *
     * @return mixed
     * @throws Exception
     */
    public function countAllCR(
        $filterExpression = null,
        $expressionAttributeNames = null,
        $expressionAttributeValues = null
    )
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.cr';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => 'business',
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "countAllAccountCRItems",
            "FilterExpression" => $filterExpression,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues,
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function createAccountST()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "createAccountStatus"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return array|null
     * @throws Exception
     */
    public function getAllAccountSTs()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "getAllAccountSTs"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $userId
     *
     * @return mixed
     * @throws Exception
     */
    public function getAccountSTbyUserId($userId)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            'userId' => $userId,
            "query" => "getAccountSTbyUserId"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $status
     *
     * @return mixed
     * @throws Exception
     */
    public function getAccountSTbyStatus($status)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            'status' => $status,
            "query" => "getAccountSTbyStatus"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountST()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "deleteAccountStatus"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function recreateAccountST()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "account" => $this->accountName,
            "query" => "dropCreateAccountStatus",
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "service" => "business"
        ];

        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $userId
     *
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountSTbyUserId($userId)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            'userId' => $userId,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "deleteAccountSTbyUserId"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $userId
     * @param $data
     * @param $status
     *
     * @return mixed
     * @throws Exception
     */
    public function addAccountSTbyUserId($userId, $data, $status)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            'userId' => $userId,
            'data' => $data,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            'status' => $status,
            "query" => "addAccountSTbyUserId"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param      $userId
     * @param      $data
     * @param null $status
     *
     * @return mixed
     * @throws Exception
     */
    public function updateAccountSTbyUserId($userId, $data, $status = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.st';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            'userId' => $userId,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            'data' => $data,
            'status' => $status,
            "query" => "updateAccountSTbyUserId"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * Events
     */
    /**
     * @return mixed
     * @throws Exception
     */
    public function createAccountEvents()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.ev';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "createAccountEvents"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountEvents()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.ev';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "deleteAccountEvents"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $oid
     * @param $eventType
     * @param $eventAction
     * @param $eventTime
     * @return mixed
     * @throws Exception
     */
    public function addAccountEvent($oid, $eventType, $eventAction, $eventTime, $metadata = null)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.ev';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "account" => $this->accountName,
            "query" => "addAccountEvent",
            "oid" => $oid,
            "metadata" => json_encode($metadata),
            "type" => $eventType,
            "event_time" => $eventTime,
            "event_action" => $eventAction,
            "service" => $this->service
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $dbId
     * @return mixed
     * @throws Exception
     */
    public function findCountAllEventsToCache($dbId)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $this->validateServiceIdbId(__FUNCTION__);

        $date = new \DateTime();
        $date->add(new \DateInterval('PT24H'));

        $filterExpression = [
            'o' => '<=',
            'l' => '#data',
            'r' => ':data'
        ];
        $expressionAttributeNames = ['#data' => 'event_time'];
        $expressionAttributeValues = [':data' => Localization::getDatabaseDateTime($date)];

        $query = [
            "account" => $dbId . '.ev',
            "query" => "findCountAllAccountEvents",
            "service" => "business",
            "PaginationConfig" => [
                'Page' => $this->page,
                'PageSize' => $this->pageSize,
            ],
            "FilterExpression" => $filterExpression,
            "ExpressionAttributeNames" => $expressionAttributeNames,
            "ExpressionAttributeValues" => $expressionAttributeValues,
        ];

        $response = $this->execute($query);

        return $this->parseResponse($response);
    }

    /**
     * @param $id
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountEvent($id)
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.ev';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "account" => $this->accountName,
            "query" => "deleteAccountEvent",
            "id" => (int)$id,
            "service" => "business"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * Pseudonymisation
     */

    /**
     * @param $database
     * @return mixed
     */
    public function recreateAccountPseudonymisation($database)
    {
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $database,
            "query" => "recreateAccountPseudonymisation"
        ];

        return $this->parseResponse($this->execute($query));
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function createAccountPseudonymisations()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.pn';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);
        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "businessDbId" => $accountName,
            "query" => "createAccountPseudonymisations"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @return mixed
     * @throws Exception
     */
    public function deleteAccountPseudonymisations()
    {
        $this->validateServiceIdbId(__FUNCTION__ . 'Base');
        $parsedIds = IdbAccountId::parse($this->accountName);
        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $idbDatabaseData .= '.pn';
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $accountName,
            "oid" => ArrayHelper::getValue($parsedIds, 'oid', null),
            "query" => "deleteAccountPseudonymisations"
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $data
     * @param $businessDbId
     *
     * @return string|null
     * @throws Exception
     */
    public function putPseudonymisation($data, $businessDbId = null)
    {
        if (empty($businessDbId)) {
            $businessDbId = $this->accountName;
        }

        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $businessDbId,
            "query" => "putPseudonymisationItem",
            "data" => $data
        ];
        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

    /**
     * @param $idbId
     * @param $data
     *
     * @param $businessDbId
     *
     * @return string|null
     * @throws Exception
     */
    public function updatePseudonymisation($idbId, $data, $businessDbId = null)
    {
        if ($this->service !== 'business') {
            $this->validateServiceIdbId(__FUNCTION__, ['idbId' => $idbId]);
        }

        $idbDatabaseData = self::getDatabaseNameByBusinessId($this->accountName);
        $accountName = $this->accountName;
        $this->accountName = $idbDatabaseData;
        $this->validateServiceIdbId(__FUNCTION__);

        $query = [
            "service" => $this->service,
            "account" => $this->accountName,
            "businessDbId" => $businessDbId,
            "query" => "updatePseudonymisationItem",
            "idbId" => $idbId,
            "data" => $data
        ];

        $response = $this->execute($query);
        $this->accountName = $accountName;

        return $this->parseResponse($response);
    }

}

################################################################################
#                                End of file                                   #
################################################################################
