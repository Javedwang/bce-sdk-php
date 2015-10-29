<?php
/*
* Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* Http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

namespace BaiduBce\Services\Qns;

use BaiduBce\Auth\BceV1Signer;
use BaiduBce\BceBaseClient;
use BaiduBce\Exception\BceClientException;
use BaiduBce\Http\BceHttpClient;
use BaiduBce\Http\HttpContentTypes;
use BaiduBce\Http\HttpHeaders;
use BaiduBce\Http\HttpMethod;
use BaiduBce\Services\Media\MediaOptions;

class QnsClient extends BceBaseClient
{

    private $signer;
    private $httpClient;
    private $prefix = '/v1/';

    /**
     * QnsClient 构造方法
     *
     * @param array $config The client configuration
     */
    function __construct(array $config)
    {
        parent::__construct($config, 'qns');
        $this->signer = new BceV1Signer();
        $this->httpClient = new BceHttpClient();
        $this->prefix .=$config['account'];
    }

    /**
     * 用于创建一个新的Topic
     * TODO:完善可选参数
     */
    public function createTopic($topicName,$options = [])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::PUT,
            [
                'config' => $config,
            ],
            '/topic/'.$topicName
        );

    }

    /**
     * 用于删除指定的Topic。当Topic被删除后，与其关联的Subscription并不会被删除，但是对应的topic属性会被置为空字符串。
     */
    public function deleteTopic($topicName,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::DELETE,
            [
                'config' => $config,
            ],
            '/topic/'.$topicName
        );
    }

    /**
     * 用于查询用户的topic列表。
     */
    public function listTopic($options = [])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
            ],
            '/topic'
        );
    }

    /**
     * 查询指定topic的subscription列表。
     */
    public function listTopicSubscriptions($topicName,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
            ],
            '/topic/'.$topicName.'/subscription'
        );
    }

    /**
     * 用于查询指定Topic状态。
     */
    public function getTopicAttributes($topicName,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
            ],
            '/topic/'.$topicName
        );
    }

    /**
     * 更新一个已存在的Topic的信息。
     */
    public function setTopicAttributes($topicName,$delayInSeconds=3600,$maximumMessageSizeInBytes=262144,$messageRetentionPeriodInSeconds=1209600,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        $param = [
            'delayInSeconds'=>$delayInSeconds,
            'maximumMessageSizeInBytes'=>$maximumMessageSizeInBytes,
            'messageRetentionPeriodInSeconds'=>$messageRetentionPeriodInSeconds
        ];
        return $this->sendRequest(
            HttpMethod::PUT,
            [
                'config' => $config,
                'headers'=>['If-Match'=>'*'],
                'body'=>json_encode($param)
            ],
            '/topic/'.$topicName
        );
    }

    /**
     * 用于发送消息到topic中，单个请求不超过256KB。单次发送的消息个数不超过1000。
     */
    public function sendTopic($topicName,$messageBody,$delayInSeconds=0,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        $params=[
            'messages'=>[
                [
                    'messageBody'=>$messageBody,
                    'delayInSeconds'=>$delayInSeconds
                ],
                [
                    'messageBody'=>$messageBody,
                ]
            ]
        ];
        return $this->sendRequest(
            HttpMethod::POST,
            [
                'config' => $config,
                'body'=>json_encode($params)
            ],
            '/topic/'.$topicName.'/message'
        );
    }

    /**
     * 本接口用于创建一个新的Subscritpion。
     */
    public function createSubscription($topicName,$subscriptionName,$pushConfig=['endpoint'=>'','version'=>'V1alpha'],$receiveMessageWaitTimeInSeconds=0,$visibilityTimeoutInSeconds=30,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        $param = [
            'receiveMessageWaitTimeInSeconds'=>$receiveMessageWaitTimeInSeconds,
            'topic'=>$topicName,
            'visibilityTimeoutInSeconds'=>$visibilityTimeoutInSeconds,
            'pushConfig'=>$pushConfig
        ];
        return $this->sendRequest(
            HttpMethod::PUT,
            [
                'config' => $config,
                'body'=>json_encode($param)
            ],
            '/subscription/'.$subscriptionName
        );
    }

    /**
     * 用于删除指定的Subscritpion。
     */
    public function deleteSubscription($subscriptionName,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::DELETE,
            [
                'config' => $config,
            ],
            '/subscription/'.$subscriptionName
        );
    }

    /**
     * 用于查询用户的subscription列表。
     */
    public function listSubscription($options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
            ],
            '/subscription'
        );
    }

    /**
     * 用于查询指定subscription状态。
     */
    public function getSubscriptionAttributes($subscriptionName,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
            ],
            '/subscription/'.$subscriptionName
        );
    }

    /**
     * 更新一个已存在的subscription的信息。
     */
    public function setSubscriptionAttributes($subscriptionName,$receiveMessageWaitTimeInSeconds=0,$visibilityTimeoutInSeconds=30,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        $param = [
            'receiveMessageWaitTimeInSeconds'=>$receiveMessageWaitTimeInSeconds,
            'visibilityTimeoutInSeconds'=>$visibilityTimeoutInSeconds,
        ];
        return $this->sendRequest(
            HttpMethod::PUT,
            [
                'config' => $config,
                'headers'=>['If-Match'=>'*'],
                'body'=>json_encode($param)
            ],
            '/subscription/'.$subscriptionName
        );
    }

    /**
     * 用于消费者使用消息队列的消息，receive message操作会将取得的消息状态变成Invisible，Invisible的时间长度由Subscription属性VisibilityTimeout指定。
     * 消费者在VisibilityTimeout时间内消费成功后需要调用delete message接口删除该消息，否则该消息将会被重新置为Visible，此消息又可被消费者重新消费。
     * 如果有太多的消息被接收后没有被删除（目前上限为12000），则无法继续接收，receive message请求将收到OverLimit错误。
     */
    public function receiveSubscriptionMessage($subscriptionName,$waitInSeconds=0,$maxMessages=1,$peek=null,$options=[])
    {
        $params=[
            'waitInSeconds'=>$waitInSeconds,
            'maxMessages'=>$maxMessages,
            'peek'=>$peek
        ];
        $params = array_filter($params);
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::GET,
            [
                'config' => $config,
                'params'=>$params
            ],
            '/subscription/'.$subscriptionName.'/message'
        );
    }

    /**
     * 用于删除已经被消费过的消息，消费者需将上次消费后得到的receiptHandle作为参数来定位要删除的消息。本操作只有在nextVisibleTime时刻之前执行才能成功；如果过了 nextVisibleTime 时刻，消息已经变为 Visible 状态，receiptHandle就会失效，删除失败，需重新消费获取新的receiptHandle。
     */
    public function deleteSubscriptionMessage($subscriptionName,$receiptHandle,$options=[])
    {
        list($config) = $this->parseOptions($options,'config');
        return $this->sendRequest(
            HttpMethod::DELETE,
            [
                'config' => $config,
                'params' =>['receiptHandle'=>$receiptHandle]
            ],
            '/subscription/'.$subscriptionName.'/message'
        );
    }

    /**
     * 用于修改被消费过并且还处于的 Invisible 状态的消息下次可被消费的时间，成功修改消息的visibilityTimeout 后,返回新的 receiptHandle。
     */
    public function changeSubscriptionVisibility()
    {

    }

    /**
     * Create HttpClient and send request
     * @param string $httpMethod The Http request method
     * @param array $varArgs The extra arguments
     * @param string $requestPath The Http request uri
     * @return mixed The Http response and headers.
     */
    private function sendRequest($httpMethod, array $varArgs, $requestPath = '/')
    {
        $defaultArgs = array(
            'config' => array(),
            'body' => null,
            'headers' => array(),
            'params' => array(),
        );

        $args = array_merge($defaultArgs, $varArgs);
        if (empty($args['config'])) {
            $config = $this->config;
        } else {
            $config = array_merge(
                array(),
                $this->config,
                $args['config']
            );
        }
        if (!isset($args['headers'][HttpHeaders::CONTENT_TYPE])) {
            $args['headers'][HttpHeaders::CONTENT_TYPE] = HttpContentTypes::JSON;
        }
        $path = $this->prefix . $requestPath;
        $response = $this->httpClient->sendRequest(
            $config,
            $httpMethod,
            $path,
            $args['body'],
            $args['headers'],
            $args['params'],
            $this->signer
        );

        $result = $this->parseJsonResult($response['body']);
        $result->metadata = $this->convertHttpHeadersToMetadata($response['headers']);
        return $result;
    }
}
