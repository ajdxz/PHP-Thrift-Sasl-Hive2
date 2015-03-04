<?php
/**
 * Hive Package
 * @since: 2014-11-19
 * @author:ajdxz
 */

$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/Thrift';
require_once $GLOBALS['THRIFT_ROOT'] . '/packages/hive_service/TCLIService.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/transport/TSaslClientTransport.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/protocol/TBinaryProtocol.php';

class Hive
{
	/**
	 * TSocket
	 * @var $socket
	 */
	protected $_socket 			= null;
	
	/**
	 * TSaslClientTransport
	 * @var $transport
	 */
	protected $_transport		= null;
	
	/**
	 * TCLIServiceClient
	 * @var $client	
	 */
	protected $_client			= null;
	
	/**
	 * TBinaryProtocol
	 * @var $_protocol Object
	 */
	protected $_protocol		= null;
	
	/**
	 * response session
	 * @var $_openSessionResp
	 */
	protected $_openSessionResp	= null;
	
	/**
	 * sql operation Handle
	 * @var $_operationHandle
	 */
	protected $_operationHandle	= null; 
	
	/**
	 * Maximum rows of return
	 * @var $_maxRows
	 */
	protected $_maxRows			= 10000;
	
	/**
	 * Has more rows
	 * @var hasMoreRows bool
	 */
	protected $_hasMoreRows		= true;
	
	/**
	 * 连接Hive
	 * @param $conf
	 */
	public function connect() {
		$this->_socket = new TSocket('127.0.0.1', 10000);
		$this->_socket->setSendTimeout(10000);
		$this->_socket->setRecvTimeout(10000);
		$this->_transport = new TSaslClientTransport($this->_socket);
		$this->_protocol  = new TBinaryProtocol($this->_transport);
		$this->_client = new TCLIServiceClient($this->_protocol);
		$this->_transport->open();
		//经过 sasl 用户验证返回 session object
		$openSessionReq = new TOpenSessionReq(array(
			'username' => 'username',
			'password' => 'password',
			'configuration' => null
		));
		$this->_openSessionResp = $this->_client->OpenSession($openSessionReq);
		$this->_maxRows = isset($conf['maxRows']) && is_numeric($conf['maxRows'])
					    ? $conf['maxRows'] : $this->_maxRows;
	}

	/**
	 * sql execute
	 * @var param $sql
	 */
	protected function execute ($sql) {
		$query = new TExecuteStatementReq(array(
				"sessionHandle" => $this->_openSessionResp->sessionHandle,
				"statement" 	=> $sql,
				"confOverlay" 	=> null
		));
		$this->_operationHandle = $this->_client->ExecuteStatement($query);
	}
	
	/**
	 * 获取参数
	 */
	public function fetch() {
		$rows = array();
		while ($this->_hasMoreRows) {
			$rows += $this->fetchSet();
		}
		return $rows;
	}
	
	/**
	 * 执行设置
	 */
	public function fetchSet() {
		$rows = array();
		$fetchReq = new TFetchResultsReq (array(
			'operationHandle' => $this->_operationHandle->operationHandle,
			'orientation'	  => TFetchOrientation::FETCH_NEXT,
			'maxRows'		  => $this->_maxRows,
		));
		return $this->_fetch($fetchReq);
	} 
	
	/**
	 * 得到数据 
	 * @param $rows
	 * @param $fetchReq
	 */
	protected function _fetch($fetchReq) {
		$resultsRes = $this->_client->FetchResults($fetchReq);
		$rowData = array();
		foreach ($resultsRes->results->rows as  $key => $row) {
			$rows = array();
			foreach ($row->colVals as $colValue) {
				$rows[] =  trim($this->_getValue($colValue));
			}
			$rowData[] = $rows;
		}
		if (0 == count($resultsRes->results->rows)) {
			$this->_hasMoreRows = false;
		}
		return $rowData;
	}
	
	/**
	 * 获取返回数据中的数据
	 * @param colValue
	 */
	protected function _getValue($colValue) {
		if ($colValue->boolVal)
			return $colValue->boolVal->value;
		elseif ($colValue->byteVal)
			return $colValue->byteVal->value;
		elseif ($colValue->i16Val)
			return $colValue->i16Val->value;
		elseif ($colValue->i32Val)
			return $colValue->i32Val->value;
		elseif ($colValue->i64Val)
			return $colValue->i64Val->value;
		elseif ($colValue->doubleVal)
			return $colValue->doubleVal->value;
		elseif ($colValue->stringVal)
			return $colValue->stringVal->value;
	}
	
	/**
	 * __destruct
	 */
	function __destruct() {
		if ($this->_operationHandle) {
			$req = new TCloseOperationReq(array(
				'operationHandle' => $this->_operationHandle->operationHandle
			));
			$this->_client->CloseOperation($req);
		}
	}
}
