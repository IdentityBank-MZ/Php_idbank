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
# Class(es)                                                                    #
################################################################################

/**
 * Class BusinessIdBankClient
 *
 * @package idb\idbank
 */
class PeopleIdBankClient extends IdBankClient
{

    const DATA_TYPES = 'dataTypes';
    const DATA_SETS = 'dataSets';
    const DATA_SETS_TYPES = 'dataSetTypes';
    const MAPS = "maps";

    /**
     * @param $response
     *
     * @return mixed
     */
    public function parseResponse($response)
    {

        $response = json_decode($response, true);
        $result = null;

        if ($response['statusCode'] == '200') {
            if (array_key_exists('result', $response)) {
                $result = json_decode($response['result'], true);
            }

            if (array_key_exists('Data', $result)) {
                $result = json_decode($result['Data'], true);
            }

            if (array_key_exists('Metadata', $result)) {
                $result = json_decode($result['Metadata'], true);
            }

            if (!is_array($result)) {
                $result = json_decode($result, true);
            }
        } elseif ($response['statusCode'] == '1511') {
            $result = null;
        }

        return $result;
    }

    /**
     * @param $service - We supporting 'business' and 'people' services via IDB API
     * @param $accountName
     *
     * @return \idb\idbank\PeopleIdBankClient|null
     */
    public static function model($service, $accountName)
    {
        if (!empty($service) && !empty($accountName)) {
            return new self($service, $accountName);
        }

        return null;
    }

    /**
     * @param       $idbId
     * @param array $data
     * @param bool  $isUpdate
     *
     * @return string|null
     */
    public function addData($idbId, array $data)
    {
        $response = $this->get($idbId);
        if (is_null($response)) {
            return $this->put($idbId, json_encode($this->prepareData($idbId, $data, false)));
        }

        return $this->update($idbId, json_encode($this->prepareData($idbId, $data, true)));
    }

    /**
     * @param       $idbId
     * @param array $data
     *
     * @return void
     */
    public function deleteData($idbId, array $data)
    {
        $response = $this->get($idbId);

        if (!is_null($response)) {
            $this->delete($idbId);

            foreach ($response[PeopleIdBankClient::DATA_TYPES] as $key => $dataType) {
                if (
                    $data['attribute'] == $dataType['attribute'] && $data['display'] == $dataType['display']
                    && $data['value'] == $dataType['value']
                ) {
                    unset($response[PeopleIdBankClient::DATA_TYPES][$key]);
                    break;
                }
            }
            foreach ($response[PeopleIdBankClient::DATA_SETS] as $key => $dataType) {
                if ($data['attribute'] == $dataType['attribute'] && $data['display'] == $dataType['display']) {
                    unset($response[PeopleIdBankClient::DATA_SETS][$key]);
                    break;
                }
            }
            $this->addData($idbId, $response);
        }
    }

    /**
     * @param       $idbId
     * @param array $data
     * @param bool  $isUpdate
     *
     * @return array
     */
    private function prepareData($idbId, array $data, bool $isUpdate = false)
    {
        if ($isUpdate) {
            $result = $this->get($idbId);
            if (!array_key_exists(self::MAPS, $data)) {
                $result[self::MAPS] = [];
            }
        } else {
            $result = [
                self::DATA_TYPES => [

                ],
                self::DATA_SETS => [

                ],
                self::MAPS => [

                ]
            ];
        }

        if (array_key_exists(self::DATA_TYPES, $data) && is_array($data[self::DATA_TYPES])) {
            foreach ($data[self::DATA_TYPES] as $key => $dataType) {
                $tmp = [
                    'attribute' => $dataType['attribute'],
                    'value' => $dataType['value'],
                    'display' => $dataType['display']
                ];
                array_push($result[self::DATA_TYPES], $tmp);
            }
        }

        if (array_key_exists(self::DATA_SETS, $data) && is_array($data[self::DATA_SETS])) {
            foreach ($data[self::DATA_SETS] as $key => $dataSet) {
                $tmp = ['attribute' => $dataSet['attribute'], 'display' => $dataSet['display']];
                foreach ($dataSet['values'] as $key => $value) {
                    $tmp['values'][$key] = $value;
                }
                array_push($result[self::DATA_SETS], $tmp);
            }
        }

        if (array_key_exists('mapping', $data)) {
            array_push($result[self::MAPS], $data);
        }

        return $result;
    }

    /**
     * @param        $peopleDbUserId
     * @param string $attribute
     * @param string $display
     *
     * @return mixed
     */
    public function findDataSet($peopleDbUserId, string $attribute, string $display)
    {
        $response = $this->get($peopleDbUserId);

        if (!is_null($response)) {
            foreach ($response[PeopleIdBankClient::DATA_SETS] as $value) {
                if ($attribute == $value['attribute'] && $display == $value['display']) {
                    return $value;
                }
            }
        }
    }

    /**
     * @param        $peopleDbUserId
     * @param string $attribute
     * @param string $display
     * @param string $value
     *
     * @return mixed
     */
    public function findDataType($peopleDbUserId, string $attribute, string $display, string $value)
    {
        $response = $this->get($peopleDbUserId);

        if (!is_null($response)) {
            foreach ($response[PeopleIdBankClient::DATA_TYPES] as $dataType) {
                if (
                    $attribute == $dataType['attribute'] && $display == $dataType['display']
                    && $value == $dataType['value']
                ) {
                    return $dataType;
                }
            }
        }
    }
}

################################################################################
#                                End of file                                   #
################################################################################
