<?php
/**
 * Created by PhpStorm.
 * User: JavedWang
 * Date: 2015/10/27
 * Time: 20:58
 */
require_once 'index.php';
require_once 'src/BaiduBce/Services/Qns/QnsClient.php';
$config =
    array(
        'credentials' => array(
            'ak' => 'AK',
            'sk' => 'SK',
        ),
        'account'=>'用户ID'
    );
$client = new \BaiduBce\Services\Qns\QnsClient($config);
$client->createTopic('topictest');
