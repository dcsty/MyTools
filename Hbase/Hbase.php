<?php
namespace DucsLib\hbase;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;

/* Dependencies. In the proper order. */
require_once __DIR__ . '/libphp/lib/Thrift/Transport/TTransport.php';
require_once __DIR__ . '/libphp/lib/Thrift/Transport/TSocket.php';
require_once __DIR__ . '/libphp/lib/Thrift/Protocol/TProtocol.php';
require_once __DIR__ . '/libphp/lib/Thrift/Protocol/TBinaryProtocol.php';
require_once __DIR__ . '/libphp/lib/Thrift/Protocol/TBinaryProtocolAccelerated.php';
require_once __DIR__ . '/libphp/lib/Thrift/Transport/TBufferedTransport.php';
require_once __DIR__ . '/libphp/lib/Thrift/Type/TMessageType.php';
require_once __DIR__ . '/libphp/lib/Thrift/Factory/TStringFuncFactory.php';
require_once __DIR__ . '/libphp/lib/Thrift/StringFunc/TStringFunc.php';
require_once __DIR__ . '/libphp/lib/Thrift/StringFunc/Core.php';
require_once __DIR__ . '/libphp/lib/Thrift/Type/TType.php';
require_once __DIR__ . '/libphp/lib/Thrift/Exception/TException.php';
require_once __DIR__ . '/libphp/lib/Thrift/Exception/TTransportException.php';
require_once __DIR__ . '/libphp/lib/Thrift/Exception/TProtocolException.php';

require_once(__DIR__ . '/gen-php/THBaseService.php');
require_once(__DIR__ . '/gen-php/Types.php');

class Hbase{

    private $client = false;
    private $hbaseConf = [];
    private $tableName = "";
    private $defaultSize = 1000;
    private $columns = [];
    private $family = "";
    private $transport = false;
    private $resData = [];
    private $scannerId = 0;
    private $maxTime = 0;
    private $minTime = 0;

    /**
     * Hbase constructor.
     * @name 构造函数
     */
    public function __construct()
    {
        $this->hbaseConf = include_once "hbase.config.php";
        $this->openClient();
    }

    /**
     * @name 字段列表
     */
    public function setColumns($columns){
        if(!empty($columns) && !empty($this->family) ){
            if( !is_array($columns) ){
                $columns = (array) $columns;
            }
            foreach ($columns as $column) {
                $temp = new \TColumn();
                $temp->family = $this->family;
                $temp->qualifier = $column;
                $this->columns[] = $temp;
            }
        }
    }

    /**
     * @name 每页需要获取的数据条数
     */
    public function setSize($pageSize){
        $this->defaultSize = $pageSize;
    }

    /**
     * @name 设置数据表
     * @param $table
     */
    public function setTable($table){
        $this->tableName = $table;
    }

    /**
     * @name 设置字段family
     * @param $family
     */
    public function setFamily($family){
        $this->family = $family;
    }

    public function setMaxTime($maxTime){
        $this->maxTime = $maxTime;
    }

    public function setMintime($minTime){
        $this->minTime = $minTime;
    }

    /**
     * @name 打开hbase连接池
     */
    private function openClient(){
        $socket = new TSocket($this->hbaseConf['host'], $this->hbaseConf['port']);

        $socket->setSendTimeout($this->hbaseConf['sendTimeout']); // Ten seconds (too long for production, but this is just a demo ;)
        $socket->setRecvTimeout($this->hbaseConf['recvTimeout']); // Twenty seconds

        $this->transport = new TBufferedTransport($socket);
        $protocol = new \Thrift\Protocol\TBinaryProtocolAccelerated($this->transport);

        $this->client = new namespace\THBaseServiceClient($protocol);
        $this->transport->open();
    }

    /**
     * @name 通过scan获取数据
     */
    public function ScanDataByPre($filter = ""){
        $scan = new \TScan();
        if( !empty($filter) ){
            $scan->filterString = "PrefixFilter('" . $filter . "')";
        }
        if( !empty($this->columns) ){
            $scan->columns = $this->columns;
        }
        if( !empty($this->maxTime) && !empty($this->minTime) ){
            $timeRange = new \TTimeRange();
            $timeRange->minStamp = ($this->minTime - 1) * 1000;
            $timeRange->maxStamp = ($this->maxTime + 1) * 1000;
            $scan->timeRange = $timeRange;
        }

        $this->scannerId = $this->client->openScanner($this->tableName,$scan);
        $scanRets = $this->client->getScannerRows($this->scannerId, $this->defaultSize);
        foreach($scanRets as $scanRet)
        {
            if( !empty($scanRet->columnValues) ){
                foreach ($scanRet->columnValues as $value){
                    $value = (array) $value;
                    $this->resData[$scanRet->row][$value['qualifier']] = $value['value'];
                }
            }
        }
        return $this->resData;
    }

    /**
     * @name 通过startRow获取数据
     * @param string $filter
     * @return array
     */
    public function ScanDataByRow($filter = ""){
        $scan = new \TScan();
        if( !empty($filter) ){
            $scan->startRow = $filter;
            $scan->stopRow = intval($filter) + 1;
        }
        if( !empty($this->columns) ){
            $scan->columns = $this->columns;
        }

        if( !empty($this->maxTime) && !empty($this->minTime) ){
            $timeRange = new \TTimeRange();
            $timeRange->minStamp = ($this->minTime - 1) * 1000;
            $timeRange->maxStamp = ($this->maxTime + 1) * 1000;
            $scan->timeRange = $timeRange;
        }

        $this->scannerId = $this->client->openScanner($this->tableName,$scan);
        $scanRets = $this->client->getScannerRows($this->scannerId, $this->defaultSize);
        foreach($scanRets as $scanRet)
        {
            if( !empty($scanRet->columnValues) ){
                foreach ($scanRet->columnValues as $value){
                    $value = (array) $value;
                    $this->resData[$scanRet->row][$value['qualifier']] = $value['value'];
                }
            }
        }
        return $this->resData;
    }

    /**
     * @name 通过GET方法获取数据
     * @param $filter
     */
    public function ObtainDataByGet( $filter ){
        $get = new \TGet();
        $get->row = $filter;
        if( !empty($this->columns) ){
            $get->columns = $this->columns;
        }
        if( !empty($this->maxTime) && !empty($this->minTime) ){
            $timeRange = new \TTimeRange();
            $timeRange->minStamp = ($this->minTime - 1) * 1000;
            $timeRange->maxStamp = ($this->maxTime + 1) * 1000;
            $get->timeRange = $timeRange;
        }

        $arr = $this->client->get($this->tableName, $get);
        $results = $arr->columnValues;
        foreach($results as $result)
        {
            $this->resData[$filter][$result->qualifier] = $result->value;
        }
        return $this->resData;
    }


    /**
     * Hbase destructor.
     * @name 析构函数
     */
    public function __destruct()
    {
        $this->scannerId !== 0 && $this->client->closeScanner($this->scannerId);
        $this->transport->close();
    }
}