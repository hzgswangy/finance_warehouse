<?php

/**
 * 货币基金都从网页抓,货币基金波动很小
 */
class mssql
{

	private static $mssql;

	private static $data; //拆分分红存储


	function __construct($router)
	{
		$work=implode('-',$router);
		self::log("{$work} Work Start !!!",true);
	}

	private static function getDb()
	{
		if(!self::$mssql)
		{
			$options=array(PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION,PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC,PDO::ATTR_TIMEOUT=>3);
			if(DIRECTORY_SEPARATOR=='/')
			{
				//linux
				self::$mssql=new PDO("odbc:Driver=SQLNativeClient;Server=10.4.27.179;Database=GTA_QIA_QDB;Uid=funddev;Pwd=123456789Aa;",'funddev','123456789Aa',$options);
			}
			else
			{
				//windows
				self::$mssql=new PDO("odbc:Driver={SQL Server};Server=10.4.27.179;Database=GTA_QIA_QDB;",'funddev','123456789Aa',$options);
			}
		}
		return self::$mssql;
	}

	private static function runSql($sql)
	{
		try
		{
			return self::getDb()->exec($sql);
		}
		catch (PDOException $e)
		{
			// wangyu test start
			//self::log("Error in runsql $e->getMessage()");
			// wangyu test end
			return app::Error(500,"Run Sql [ {$sql} ] Error : ".$e->getMessage());
		}
	}
	private static function getData($sql)
	{
		try
		{
			$rs=self::getDb()->query($sql);
			return $rs===false?array():$rs->fetchAll(PDO::FETCH_ASSOC);
		}
		catch (PDOException $e)
		{
			return app::Error(500,"Run Sql [ {$sql} ] Error : ".$e->getMessage());
		}
	}
	private static function getLine($sql)
	{
		try
		{
			$rs=self::getDb()->query($sql);
			return $rs===false?array():$rs->fetch(PDO::FETCH_ASSOC);
		}
		catch (PDOException $e)
		{
			return app::Error(500,"Run Sql [ {$sql} ] Error : ".$e->getMessage());
		}
	}
	private static function getVar($sql)
	{
		try
		{
			$rs=self::getDb()->query($sql);
			return $rs===false?null:$rs->fetchColumn();
		}
		catch (PDOException $e)
		{
			return app::Error(500,"Run Sql [ {$sql} ] Error : ".$e->getMessage());
		}
	}

	private static function log($msg,$writeFile=false)
	{
		echo date('H:i:s').' '.$msg.PHP_EOL;
		if($writeFile)
		{
			file_put_contents(date('Y-m-d').'-run.log',date('Y-m-d H:i:s').' '.$msg.PHP_EOL,FILE_APPEND);
		}
	}

	private static function getRootDb()
	{
		if(!self::$mssql)
		{
			$options=array(PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION,PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC,PDO::ATTR_TIMEOUT=>3);
			if(DIRECTORY_SEPARATOR=='/')
			{
				//linux
				self::$mssql=new PDO("odbc:Driver=SQLNativeClient;Server=10.4.27.179;Database=GTA_QIA_QDB;Uid=sa;Pwd=123456789Aa;",'sa','123456789Aa',$options);
			}
			else
			{
				//windows
				self::$mssql=new PDO("odbc:Driver={SQL Server};Server=10.4.27.179;Database=GTA_QIA_QDB;",'sa','123456789Aa',$options);
			}
		}
		return self::$mssql;
	}

	private static function runRootSql($sql)
	{
		try
		{
			return self::getRootDb()->exec($sql);
		}
		catch (PDOException $e)
		{
			return app::Error(500,"Run Sql [ {$sql} ] Error : ".$e->getMessage());
		}
	}

	// get max id for target table in mysql
	private static function get_max_id($table_name, $id_name) {
		#$sql = "SELECT top 1 $id_name FROM $table_name ORDER BY $id_name desc";
		$sql = "SELECT `{$id_name}` FROM `{$table_name}` ORDER BY `{$id_name}` desc limit 1";
		$id_array = DB::getData($sql);
		if (count($id_array) > 0) {
			$max_id = (int) ($id_array[0][$id_name]);
		} else {
			$max_id  = -1;
		}
		return $max_id;
	}

	// get row count for target table in mysql
	private static function get_row_count($table_name) {
		$sql = "SELECT count(*) FROM $table_name ";
		$ret = DB::getData($sql);
		$count = (int) ($ret[0]["count(*)"]);
		return $count;
	}

	// get row count for target table in sql server
	private static function get_ms_row_count($table_name, $addtion_where = null) {
		$sql = "SELECT count(*) FROM [dbo].[{$table_name}] ";
		if ($addtion_where != null) {
			$sql = $sql." where {$addtion_where}";
		}
		var_dump($sql);
		$ret = self::getData($sql);
		var_dump($ret);
		$count = (int) ($ret[0][""]);
		return $count;
	}

	/*
	 * get the start pos for the matched $head->$start(eg. xml)
	 * start pos should behind the start a little,
	 * $end_pos: the pos of the end pattern
	 */
	function get_patten_string($data, $head, $end, $start_pos, &$end_pos) {
		$p1 = strpos($data, $head, $start_pos);
		if ($p1 === false) {
			return false;
		}
		$p2 = $this->get_pattern_pos($data, $head, $end, $p1+1);
		if ($p2 === false) {
			return false;
		}
		$res = substr($data, $p1, $p2 - $p1);
		$end_pos = $p2;
		return $res;
	}

	function get_patten_string_right_pos($data, $head, $end, $start_pos, &$end_pos) {

		$ret = $this->get_pattern_pos($data, $head, $end, $start_pos+1);
		if ($ret === false) {
			return false;
		}
		$end_pos = $ret;
		$p1 = $start_pos; $p2 = $end_pos;
		$res = substr($data, $p1, $p2 - $p1);
		$end_pos = $p2;
		return $res;
	}

	/*
	 * get the string for the matched $head->$start(eg. xml)
	 * NOTICE: the start pos is right the pos of the start pattern
	 * $end_pos: the pos of the end pattern
	 */
	function get_pattern_pos($data, $head, $end, $start_pos) {
		/*	
			echo "get_pattern_pos\n";
			var_dump($data);
			var_dump($head);
			var_dump($end);
			var_dump($start_pos);
		//*/
		$p1 = strpos($data, $head, $start_pos);
		$p2 = strpos($data, $end, $start_pos);
		while ($p2 != false) {
			if ($p1 == false || $p2 < $p1) {
				return $p2;
			}
			$start_pos = $p2 + 1;
			$p1 = strpos($data, $head, $start_pos);
			$p2 = strpos($data, $end, $start_pos);
		}
		return false;
	}

	function get_start_and_end_pos(&$data, $field, $size, $start_pos, &$end_pos) {
		echo "get_start_and_end_pos: ";
		$id = $data[$start_pos][$field];
		$end_pos = $start_pos;
		for (; $end_pos < $size; $end_pos ++) {
			if ( $data[$end_pos][$field] != $id ) {
				break;
			}
		}
		$end_pos --;
		echo " {$id}, {$field}, {$start_pos}, {$end_pos}\n";
	}

	function getDateTime($date_s) {
		$year_p = strpos($date_s, "年");
		$mon_p = strpos($date_s, "月");
		$day_p = strpos($date_s, "日");

		$date = new DateTime();
		if ($year_p == false || $mon_p == false || $day_p == false) {
			$date->setDate(2000, 1, 1);
			return $date->format('Y-m-d');
		}
		$year = (int) substr($date_s, 0, $year_p);
		$month = (int) substr($date_s, $year_p + 1, $mon_p - $year_p - 1 );
		$day = (int) substr($date_s, $mon_p+1, $day_p - $mon_p - 1);
		$date->setDate($year, $month, $day);
		return $date->format('Y-m-d');
	}

	/**
	 * used for batch query
	 * when insert_count == margin-1, do the sql and reset
	 * @param $sql
	 * @param $insert_count
	 * @param $margin
	 */
	function run_sql(&$sql, &$insert_count, $margin) {
		if ( ($insert_count % $margin) >= ($margin - 1) ) {
			echo "do sql\n";
			var_dump($sql);
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count++;
		}
	}

	/*
 * $mysql_id, order
 */
	function syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array = [], $mssql_id = null, $addtion_where = null, $pre_msdata_proc_function = null) {
		// check variable
		if ( !is_array($mysql_fields) || !is_array($mssql_fields) ) {
			echo "input fields is not array";
			return false;
		}

		if ( count($mssql_fields) != count($mysql_fields) ) {
			echo "input fields is not equal";
			return false;
		}

		// load partial every time
		$sql = "";
		if ($mssql_id !== null) {
			// get total size
			$sz = $this->get_ms_row_count($mssql_tbname, $addtion_where);
			$start_pos = 0;
			$margin = 100;
			$end_pos = $start_pos + $margin;
			echo "start pos: ";
			var_dump($start_pos);
			while ($start_pos < $sz) {
				$sql = "SELECT * from (SELECT *, ROW_NUMBER() OVER (ORDER BY $mssql_id) as row FROM [dbo].[{$mssql_tbname}]) a WHERE a.row >= {$start_pos} and a.row <= {$end_pos}";
				if ($addtion_where != null) {
					$sql = "SELECT * from (SELECT *, ROW_NUMBER() OVER (ORDER BY $mssql_id) as row FROM [dbo].[{$mssql_tbname}] where {$addtion_where}) a WHERE a.row >= {$start_pos} and a.row <= {$end_pos}";
				} else {
					$sql = "SELECT * from (SELECT *, ROW_NUMBER() OVER (ORDER BY $mssql_id) as row FROM [dbo].[{$mssql_tbname}]) a WHERE a.row >= {$start_pos} and a.row <= {$end_pos}";
				}
        var_dump($sql);
				$source_data = $this->getData($sql);
				var_dump("source");
				var_dump($source_data);
				if ($pre_msdata_proc_function != null) {
					//$pre_msdata_proc_function($source_data);
					$source_data = call_user_func($pre_msdata_proc_function, $source_data);
				}
				$this->syn_data($source_data, $mssql_fields, $mysql_tbname, $mysql_fields, $mysql_id_array);
				$start_pos = $end_pos + 1;
				$end_pos = $start_pos + $margin;
			}

			foreach ($mssql_fields as $filed) {
				$sql = $sql." `{$filed}`";
			}
		} else {
			$sql = "SELECT ";
			foreach ($mssql_fields as $filed) {
				$sql = $sql." `{$filed}`";
			}
			$sql = $sql." from [dbo].[{$mssql_tbname}]";
			if ($addtion_where != null) {
				$sql = $sql." where {$addtion_where}";
			}
			$source_data = $this->getData($sql);
			$this->syn_data($source_data, $mssql_fields, $mysql_tbname, $mysql_fields, $mysql_id_array);
		}
	}

	function syn_data($source_data, $source_fields, $target_tbname, $target_fields, $target_key_array = []) {
		echo "do syn_data\n";
		$margin = 50;
		$sz = count($source_data);
		$count = $sz / $margin;
		if ( $sz % $margin != 0 ) {
			$count ++;
		}
		$slicde_data = array_chunk ($source_data, $count);
		foreach ($slicde_data as $data_array) {
			//var_dump($data_array);
			$sql = "INSERT INTO {$target_tbname} (";
			foreach ($target_fields as $fileds) {
				$sql = $sql." `{$fileds}`,";
			}
			$tmp_sz = strlen($sql);
			$sql = substr($sql, 0, $tmp_sz-1);
			$sql = $sql.") VALUES";
			foreach ($data_array as $item) {
				//var_dump($item["fund_code"]);
				$sql = $sql." (";
				foreach($source_fields as $fields) {
					if (!array_key_exists($fields, $item)) {
						$item[$fields] = "";
					}
					$sql = $sql."'{$item[$fields]}', ";
				}
				// delete last ","
				$tmp_sz = strlen($sql);
				$sql = substr($sql, 0, $tmp_sz-2);
				$sql = $sql."),";
			}
			// delete last ","
			$tmp_sz = strlen($sql);
			$sql = substr($sql, 0, $tmp_sz-1);
			// add duplicate
			$sql = $sql." ON DUPLICATE KEY UPDATE ";
			foreach ($target_fields as $fields) {
				if (in_array($fields, $target_key_array)) {
					continue;
				}
				$sql = $sql." {$fields}=VALUES({$fields}),";
			}
			// delete last ","
			$tmp_sz = strlen($sql);
			$sql = substr($sql, 0, $tmp_sz-1);
			var_dump($sql);
			DB::runSql($sql);
		}

	}

	/**
	 * 默认两张表的字段一致
	 * @param $source_table
	 * @param $target_table
	 * @param $order_id: whether add "''" in sql statement
	 * @param $is_value
	 * @param array $exclude_array: ignore fields
	 */
	function compareTable($source_table, $target_table, $order_id, $is_value, $exclude_array=[]) {
		// get propertylist
		echo "start compareTable";
		$sql = "select `column_name` from `information_schema`.`columns` where `table_name`='{$source_table}'";
		$ret = DB::getData($sql);
		$filed_array = [];
		foreach ($ret as $item) {
			array_push($filed_array, $item["column_name"]);
		}

		// get id count for both
		$max_target_id = $this->get_max_id($target_table, $order_id);
		$max_source_id = $this->get_max_id($source_table, $order_id);
		$max_id = max($max_target_id, $max_source_id);

		$margin = 5000;
		$start_id = 0;
		$end_id = $margin + $start_id;

		while ($start_id <= $max_id) {
			echo " start_id: {$start_id}, end_id: {$end_id} \n";
			$this->comparePartTable($source_table, $target_table, $order_id, $is_value, $filed_array, $exclude_array, $start_id, $end_id);
			$start_id += $margin;
			$end_id += $margin;
		}

		echo "end compareTable";

	}

	function comparePartTable($source_table, $target_table, $order_id, $is_value, $field_array, $exclude_array=[], $start_id, $end_id) {
		if ($is_value) {
			$sql_source = "select * from {$source_table} where $order_id >= {$start_id} and $order_id < {$end_id} order by $order_id";
			$sql_target = "select * from {$target_table} where $order_id >= {$start_id} and $order_id < {$end_id} order by $order_id";
		} else {
			$sql_source = "select * from {$source_table} where $order_id >= '{$start_id}' and $order_id < '{$end_id}' order by $order_id";
			$sql_target = "select * from {$target_table} where $order_id >= '{$start_id}' and $order_id < '{$end_id}' order by $order_id";
		}

		$source_data =  DB::getData($sql_source);
		$source_size = count($source_data);
		$target_data = DB::getData($sql_target);
		$target_size = count($target_data);

		// doulbe loop to compare
		$pos_source = 0;
		$pos_target = 0;
		for (; $pos_source < $source_size; ) {
			for (; $pos_target < $target_size; ) {
				$item_source = $source_data[$pos_source];
				$item_target = $target_data[$pos_target];
				if ($this->compareRow($item_source, $item_target, $field_array, $exclude_array)) {
					$pos_source ++;
					$pos_target ++;
					continue;
				} else {
					$source_id = $item_source[$order_id];
					$target_id = $item_target[$order_id];

					if ($source_id == $target_id) {
						// different
						echo "different occur: \n";
						echo "  souce row: ";
						var_dump($item_source);
						echo "  target row: ";
						var_dump($item_target);
						echo "\n";
						$pos_source ++;
						$pos_target ++;
						break;
					} else if ($source_id < $target_id) {
						// target lack
						// add data
						echo "target lack: \n";
						echo "  source row: ";
						var_dump($item_target);
						echo "\n";
						$pos_source ++;
						break;
					} else {
						// source lack
						echo "source lack: \n";
						echo "  target row: ";
						var_dump($item_source);
						echo "\n";
						$pos_target ++;
					}
				}

			}
			if ($pos_target == $target_size) {
				break;
			}
		}

		for (; $pos_source < $source_size; $pos_source ++) {
			$item_source = $source_data[$pos_source];
			echo "target lack: \n";
			echo "  source row: ";
			var_dump($item_source);
			echo "\n";
		}

		for (; $pos_target < $target_size; $pos_target ++) {
			$item_target = $target_data[$pos_target];
			echo "source lack: \n";
			echo "  target row: ";
			var_dump($item_target);
			echo "\n";
		}

	}

	function compareRow($source_row, $target_row, $filed_array, $exclude_array = []) {
		foreach ( $filed_array as $filed ) {
			if (in_array($filed, $exclude_array)) {
				continue;
			}
			if ( $target_row[$filed] != $source_row[$filed] ) {
				echo "compareRow: ";
				echo "{$filed}";
				echo " ";
				echo $source_row[$filed];
				echo " ";
				echo $target_row[$filed];
				echo "\n";
				return false;
			}
		}

		return true;
	}

	//分红,拆分信息读取
	public static function splitprepare()
	{
		$data=array('bonus'=>array(),'split'=>array());
		$dataBonus=&$data['bonus'];
		$dataSplit=&$data['split'];
		$bonus=DB::getData("SELECT code,date(date_divid) date_divid,per FROM `fund_bonus` ORDER BY `code`");
		foreach ($bonus as $item)
		{
			$code=$item['code'];
			if(!isset($dataBonus[$code]))
			{
				$dataBonus[$code]=array($item['date_divid']=>$item['per']);
			}
			else
			{
				$dataBonus[$code][$item['date_divid']]=$item['per'];
			}
		}
		//下面计算拆分
		$split=DB::getData("SELECT `code`,date(date_cal) date_cal,split_percent FROM `fund_split` ORDER BY `code`");
		foreach ($split as $item)
		{
			$code=$item['code'];
			if(!isset($dataSplit[$code]))
			{
				$dataSplit[$code]=array($item['date_cal']=>$item['split_percent']);
			}
			else
			{
				$dataSplit[$code][$item['date_cal']]=$item['split_percent'];
			}
		}
		self::$data=$data;
		self::log("get bonus and split ok");
	}

	/**
	 * 每日执行,消耗时间较长(2个小时)可中断,指定点继续执行
	 */
	function index($goto=0)
	{
		// self::log('Work will start at '.date('Y-m-d H:i',time()+3600*12))||sleep(3600*12);
		$this->sync($goto);
		$this->update_relative_bonus_split();
	}

	/**
	 * 每日执行
	 */
	function task2()
	{
		$this->funddividend(); //分红同步
		$this->resolution(); //拆分同步
		app::run(array('bond','index')); //中证全债数据抓取
	}

	/**
	 * 资产配置,有关季度的数据,每月执行,消耗内存较大,执行时间长(2个小时)
	 */
	function task3()
	{
		$this->stock();
	}

	/*************************************基金净值同步 需每日同步***********************************/

	function sync($goto=0,$countRange=false)
	{
		$sql="SELECT DISTINCT SYMBOL FROM [dbo].[FUND_NAV] WHERE NAV IS NOT NULL AND ACCUMULATIVENAV IS NOT NULL ORDER BY SYMBOL";
		$data=self::getData($sql);
		$symbols=array_column($data,'SYMBOL');
		$all=count($symbols);
		self::log('total symbols :'.$all);
		foreach ($symbols as $num=>$symbol)
		{
			// 240021 260102 690212是一个货币基金
			if($num<$goto or in_array($symbol,array('000366','240021','260102','690212'))) //排除部分基金
			{
				continue;
			}
			$msg=$this->syncSingle($symbol,$countRange);
			self::log(($num+1).' Done,Remain '.($all-1-$num).'=>'.$msg,true);
		}
	}

	/**
	 * 启动range涨幅纠偏工作,可修复近期range的错误数据，耗时较长
	 */
	function startCount($goto=0)
	{
		$this->sync($goto,true);
	}

	/**
	 * 同步单个基金，无条件同步
	 */
	public function syncSingle($symbol,$countRange=false)
	{
		if($countRange)
		{
			return $this->countRange($symbol);
		}
		$sql="SELECT NAV,ACCUMULATIVENAV,CONVERT(DATE,TRADINGDATE) TRADINGDATE,TRADINGDATE TRADINGDATE_FULL, UPDATETIME FROM (SELECT TOP 10 * FROM [dbo].[FUND_NAV] WHERE SYMBOL={$symbol} ORDER BY TRADINGDATE DESC) a ORDER BY a.TRADINGDATE ASC";
		$origin=self::getData($sql);
		$sql="INSERT INTO `fund_value` (`code`,`unit`,`all`,`range`,`create_date`,`update_date`) VALUES ('{$symbol}',:unit,:all,:range,:create_date,:update_date) ON DUPLICATE KEY UPDATE update_date=:update_date,`range`=:range,`unit`=:unit,`all`=:all";
		$stm=DB::prepare($sql);
		$map=array_column($origin,null,'TRADINGDATE');
		$keys=array_keys($map);
		$i=0;
		foreach ($origin as $index=>$item)
		{
			if($index<1)continue;
			$date=$item['TRADINGDATE'];
			//计算涨幅 $range
			$range=self::getRange($date,$map,$keys,$symbol);
			$stm->bindParam(':unit',$item['NAV']);
			$stm->bindParam(':all',$item['ACCUMULATIVENAV']);
			$stm->bindParam(':range',$range);
			$stm->bindParam(':create_date',$item['TRADINGDATE_FULL']);
			$stm->bindParam(':update_date',$item['UPDATETIME']);
			var_dump($stm);
			$ret=$stm->execute();
			$i++;
			self::log("range for {$symbol} of date {$date} is {$range} => {$ret}");
		}
		return "{$symbol} UPDATEED {$i} ,NEW {$item['TRADINGDATE']}";
	}
	/**
	 * 由历史数据计算涨幅
	 */
	private static function getRange($date,$map,$keys,$symbol)
	{
		$keyIndex=array_search($date,$keys);
		$item=$map[$date];
		$prevDate=$keyIndex>0?$keys[$keyIndex-1]:null;//上一个交易日
		$prevItem=is_null($prevDate)?0:$map[$prevDate];
		$prevNav=$prevItem?floatval($prevItem['NAV']):0;
		$currentNav=floatval($item['NAV']);
		//需要检测是否含有分红等
		$currentNav=self::fixCurrentNav($currentNav,$symbol,$date);

		if($prevNav)
		{
			$percent=sprintf('%.2f',round(($currentNav-$prevNav)/$prevNav,4)*100)."%";
		}
		else
		{
			$percent='0.00%';
		}
		return $percent;
	}
	//修正range,遍历全部
	private function countRange($symbol)
	{

		// $sql="SELECT unit,`all`,`range`,date(create_date) create_date FROM (SELECT * FROM `fund_value` WHERE `code`={$symbol} ORDER BY create_date DESC LIMIT 60) a ORDER BY a.create_date ASC";
		$sql="SELECT unit,`all`,`range`,date(create_date) create_date FROM  `fund_value` WHERE `code`={$symbol} ORDER BY create_date ";
		$data=DB::getData($sql);
		$i=$j=0;
		foreach ($data as $i=>$item)
		{
			if($i==0)
			{
				$prevNav=$item['unit'];
				continue;
			}
			$oldRange=$item['range'];
			$date=$item['create_date'];
			$currentNav=$item['unit'];

			//需要检测是否含有分红等
			$currentNav=self::fixCurrentNav($currentNav,$symbol,$date);
			if(!$prevNav)
			{
				var_dump($symbol,$date,$prevNav);die;
			}
			$range=sprintf('%.2f',round(($currentNav-$prevNav)/$prevNav,4)*100)."%";
			$prevNav=$item['unit'];
			if($range==$oldRange)
			{
				$i++;
				// self::log("{$symbol} Range for {$date} id OK {$range}");
			}
			else
			{
				$j++;
				$sql="UPDATE `fund_value` SET `range`='{$range}' WHERE code='{$symbol}' AND date(create_date)='{$date}' ";
				$ret=DB::runSql($sql);
				self::log("{$symbol} Update Range for {$date}, Fixed {$oldRange}=>{$range} , {$ret}");
			}

		}
		return "OK {$i},Fixed {$j}";

	}
	private static function  fixCurrentNav($currentNav,$code,$date)
	{
		if(!self::$data)
		{
			self::splitprepare();
		}
		if(isset(self::$data['bonus'][$code][$date]))
		{
			$currentNav=$currentNav+self::$data['bonus'][$code][$date];
		}
		else if(isset(self::$data['split'][$code][$date]))
		{
			$currentNav=$currentNav*self::$data['split'][$code][$date];
		}

		return $currentNav;
	}




	/************stock同步,无需每日,执行大约3小时,首次消耗内存较小,再次执行消耗很大(700Mb)*********/

	public function stock($code=null)
	{
		if(DIRECTORY_SEPARATOR=='/')
		{
			exit("You Should not run this on linux system");//linux 系统下，mb_convert_encoding转换出现乱码，需在windows电脑执行此操作
		}
		ini_set('memory_limit', '800M');
		$sql="SELECT DISTINCT MASTERFUNDCODE FROM [dbo].[FUND_PORTFOLIO_STOCK] ORDER BY MASTERFUNDCODE";
		if($code)
		{
			$sql="SELECT  DISTINCT MASTERFUNDCODE FROM [dbo].[FUND_PORTFOLIO_STOCK] WHERE MASTERFUNDCODE='{$code}'";
		}
		$data=self::getData($sql);
		$sql="SELECT `updateid` FROM `fund_stock`";
		$ret=DB::getData($sql);
		$ids=array_column($ret,'updateid');
		unset($ret,$sql);
		self::log('total num '.count($data));
		foreach($data as $item)
		{
			$code=$item['MASTERFUNDCODE'];
			$sql="SELECT *  FROM [dbo].[FUND_PORTFOLIO_STOCK] WHERE  MASTERFUNDCODE='{$code}' ORDER BY STARTDATE";
			$funData=self::getData($sql);
			$this->stockToMysql($code,$funData,$ids);
		}
		self::log('finished');

	}

	private function stockToMysql($code,$data,$ids)
	{
		$sql="INSERT INTO `fund_stock` (`fund_code`,`code`,`name`,`percent`,`mount`,`price`,`update_date`,`start_date`,`end_date`,`updateid`) VALUES ('{$code}',:code,:name,:percent,:mount,:price,:update_date,:start_date,:end_date,:updateid) ";
		$sql2="UPDATE `fund_stock` SET `name`=:name WHERE `updateid`=:updateid ";
		try
		{
			$stm=DB::prepare($sql);
			$stm2=DB::prepare($sql2);
			foreach ($data as $item)
			{
				if(!in_array($item['UPDATEID'],$ids))
				{
					$name=mb_convert_encoding($item['STOCKNAME'],"UTF-8","GBK");
					// iconv("GBK", "UTF-8", $item['STOCKNAME']);
					$stm->bindParam(':code',$item['SYMBOL']);
					$stm->bindParam(':name',$name);
					$stm->bindParam(':percent',$item['PROPORTION']);
					$stm->bindParam(':mount',$item['SHARES']);
					$stm->bindParam(':price',$item['MARKETVALUE']);
					$stm->bindParam(':update_date',$item['UPDATETIME']);
					$stm->bindParam(':start_date',$item['STARTDATE']);
					$stm->bindParam(':end_date',$item['ENDDATE']);
					$stm->bindParam(':updateid',$item['UPDATEID']);
					$stm->execute();
					self::log("add {$item['UPDATEID']} for {$code}");
				}
				else
				{
					$name=mb_convert_encoding($item['STOCKNAME'],"UTF-8","GBK");
					$stm2->bindParam(':name',$name);
					$stm2->bindParam(':updateid',$item['UPDATEID']);
					$stm2->execute();
					self::log("CODE {$code} UPDATEID {$item['UPDATEID']} UPDATE");
				}
			}
		}
		catch(PDOException $e)
		{
			exit($e->getMessage());
		}
	}


	/*********分红同步 FUND_FUNDDIVIDEND 增量同步,不修改历史数据*************/

	function funddividend()
	{
		//数据大约6000条,一次取出
		$sql="SELECT SYMBOL,RECORDDATE,PRIMARYEXDIVIDENDDATE,DISTRIBUTIONPLAN,PRIMARYPAYDATE_DIVIDEND,UPDATETIME,DIVIDENDPERSHARE,UPDATEID FROM [dbo].[FUND_FUNDDIVIDEND] ORDER BY SYMBOL";
		$data=self::getData($sql);
		$sql="SELECT updateid FROM `fund_bonus`";
		$ret=DB::getData($sql);
		$ids=array_column($ret,'updateid');
		unset($ret);
		$sql="INSERT INTO `fund_bonus` (`code`,`year`,`date_reg`,`date_divid`,`bonus`,`date_grant`,`update_date`,`per`,`updateid`) VALUES (:code,:year,:date_reg,:date_divid,:bonus,:date_grant,:update_date,:per,:updateid)  ON DUPLICATE KEY UPDATE date_reg=VALUES(date_reg), date_divid=VALUES(date_divid), bonus=VALUES(bonus), date_grant=VALUES(date_grant), update_date=VALUES(update_date), per=VALUES(per)";
		$stm=DB::prepare($sql);

		$stm=DB::prepare($sql);
		$add=$ignore=0;
		foreach ($data as $item)
		{
			if(!in_array($item['UPDATEID'],$ids))
			{
				if(!empty($item['PRIMARYEXDIVIDENDDATE']) && !empty($item['PRIMARYPAYDATE_DIVIDEND']))
				{
					$year=substr($item['RECORDDATE'],0,4);
					//$bonus=iconv("GBK", "UTF-8", $item['DISTRIBUTIONPLAN']);
					$bonus=$item['DISTRIBUTIONPLAN'];
					$stm->bindParam(':code',$item['SYMBOL']);
					$stm->bindParam(':year',$year);
					$stm->bindParam(':date_reg',$item['RECORDDATE']);
					$stm->bindParam(':date_divid',$item['PRIMARYEXDIVIDENDDATE']);
					$stm->bindParam(':bonus',$bonus);
					$stm->bindParam(':date_grant',$item['PRIMARYPAYDATE_DIVIDEND']);
					$stm->bindParam(':update_date',$item['UPDATETIME']);
					$stm->bindParam(':updateid',$item['UPDATEID']);
					$stm->bindParam(':per',$item['DIVIDENDPERSHARE']);
					self::log('add data for '.$item['SYMBOL']);
					$stm->execute();
					$add++;
				}
				else
				{
					self::log('ignore empty data '.$item['SYMBOL']);
					$ignore++;
				}
			}
		}
		self::log("finished add {$add} ignore {$ignore}");
	}


	/*************************************拆分同步***********************************/

	function resolution()
	{
		$sql="SELECT SYMBOL,SPLITRECORDDATE,SPLITDOBJECT,SPLITRATIO,UPDATETIME,UPDATEID FROM [dbo].[FUND_RESOLUTION]  ORDER BY SYMBOL";
		$data=self::getData($sql);
		$sql="SELECT updateid FROM `fund_split`";
		$ret=DB::getData($sql);
		$ids=array_column($ret,'updateid');
		unset($ret);
		$sql="INSERT INTO `fund_split` (`code`,`year`,`date_cal`,`split_type`,`split_percent`,`update_date`,`updateid`) VALUES (:code,:year,:date_cal,:split_type,:split_percent,:update_date,:updateid)";
		$stm=DB::prepare($sql);
		$add=$ignore=0;
		foreach ($data as $item)
		{
			if(!in_array($item['UPDATEID'],$ids))
			{
				if(!empty($item['SPLITRECORDDATE']) && !empty($item['SPLITRATIO']))
				{
					$year=substr($item['SPLITRECORDDATE'],0,4);
					$split_type=mb_convert_encoding($item['SPLITDOBJECT'],"UTF-8","GBK");
					$stm->bindParam(':code',$item['SYMBOL']);
					$stm->bindParam(':year',$year);
					$stm->bindParam(':date_cal',$item['SPLITRECORDDATE']);
					$stm->bindParam(':split_type',$split_type);
					$stm->bindParam(':split_percent',$item['SPLITRATIO']);
					$stm->bindParam(':update_date',$item['UPDATETIME']);
					$stm->bindParam(':updateid',$item['UPDATEID']);
					$add++;
					$stm->execute();
					self::log("add data for {$item['SYMBOL']}");
				}
				else
				{
					$ignore++;
					self::log('ignore empty data '.$item['SYMBOL']);
				}

			}
		}
		self::log("finished add {$add} ignore {$ignore}");
	}


	/**
	 * 检查程序,检查阶段涨幅数据是否更新
	 */
	function check()
	{
		$code=530008;
		$sql="SELECT * FROM `fund_data` WHERE `code`={$code} ORDER BY create_date DESC";
		$data=DB::getLine($sql);
		$lastUpdate=strtotime($data['update_date']);
		$last=date('Y-m-d H:i:s',time()-21600);
		if($lastUpdate<(time()-21600))
		{
			self::log('!!!!Warning:fund_data was not update at least 6 hours');
		}
		else
		{
			$num=DB::getVar("SELECT count(1) FROM `fund_data` where update_date > '{$last}'");
			self::log("fund_data was updated at {$data['update_date']},please check {$code} {$data['increase']}");
			self::log("total update {$num}/3034");
		}

		//下面检查货币基金
		$date=DB::getVar("SELECT date(create_date) create_date FROM `fund_value` WHERE `code`=740601 ORDER BY create_date DESC LIMIT 1");
		if($date!=date('Y-m-d',strtotime('-1 days')))
		{
			self::log("!!!!Warning:fund_value has no yesterday data,recent date {$date}");
		}
		else
		{
			self::log("fund_value last update {$date}");
			$last=date('Y-m-d 00:00:00',strtotime('-1 days'));
			$num=DB::getVar("SELECT count(1) FROM `fund_value` where create_date = '{$last}'");
			self::log("total update {$num}/3107");
		}
	}

	//从描述中算出每份派发现金,存储到per,无副作用,无需每天执行
	public function bonusfix()
	{
		$i=$j=0;
		$data=DB::getData("SELECT * FROM `fund_bonus`");
		foreach ($data as $item)
		{
			$id=$item['id'];
			$bonus=$item['bonus'];
			$oldPer=$item['per'];
			if(preg_match_all('/.*?10.*?([\d\.]+)/',$bonus,$matches))
			{
				$tmp=$matches[1][0];
				$per=round(($tmp/10),4);
				if($per==0)
				{
					exit("Zero Found {$per} {$id}");
				}
				if($oldPer!=$per)
				{
					$i++;
					DB::runSql("UPDATE `fund_bonus` SET `per`='{$per}' WHERE id={$id}");
					self::log("{$id} FIXED {$oldPer}=>{$per}");
				}
				else
				{
					$j++;
					self::log("{$id} IS ALREADY NEW");
				}
			}
			else
			{
				exit("not Match {$id}");
			}
		}
		$num=count($data);
		self::log("FIXED {$i}, OK {$j} ,Total {$num}");
	}

	/**
	 * 资产配置，基金涨幅,每天抓取，所有基金
	 */
	public function assets($code=530008)
	{
		$url="http://www.howbuy.com/fund/{$code}/";
		$html=file_get_contents($url);
		$pattern='/class="nTab9 jjzf_content" id="nTab9_0" style="display: block;">([\s\S]*?)sfw_tips_c/';
		$data=array('increase'=>'','increase_avg'=>'','increase_300'=>'','asset'=>'','trade'=>'');
		if(preg_match_all($pattern, $html, $matches))
		{
			$text=$matches[0][0];
			if(preg_match_all('/区间回报<\/td>([\s\S]*?)<\/tr>/',$text,$matches2))
			{
				$tmp=explode(PHP_EOL,$matches2[1][0]);
				$str=array();
				foreach ($tmp as $item)
				{
					if(!empty(trim($item)))
					{
						$str[]=trim(strip_tags($item));
					}
				}
				$data['increase']=implode('|',$str);
			}
			else
			{
				app::log("QUJIAN ERROR {$code}");
			}

			if(preg_match_all('/同类平均<\/td>([\s\S]*?)<\/tr>/', $text, $matches2))
			{
				$tmp=explode(PHP_EOL,$matches2[1][0]);
				$str=array();
				foreach ($tmp as $item)
				{
					if(!empty(trim($item)))
					{
						$str[]=trim(strip_tags($item));
					}
				}
				$data['increase_avg']=implode('|',$str);
			}
			else
			{
				app::log("TONGLEI ERROR {$code}");
			}
			if(preg_match_all('/(沪深300|全债指数)<\/td>([\s\S]*?)<\/tr>/', $text, $matches2))
			{
				$tmp=explode(PHP_EOL,$matches2[2][0]);
				$str=array();
				foreach ($tmp as $item)
				{
					if(!empty(trim($item)))
					{
						$str[]=trim(strip_tags($item));
					}
				}
				$data['increase_300']=implode('|',$str);
			}
			else
			{
				app::log("TONGLEI ERROR {$code}");
			}
		}
		else
		{
			app::log("Value Data Not Match {$code}");
		}
		//下面资产配置，行业配置
		$html=file_get_contents("http://static.howbuy.com/min/f=/upload/auto/script/fund/data_{$code}_v759.js");
		if(preg_match_all('/hyPieData(.*?)jjpmChartData=\{/', $html, $matches))
		{
			$text=$matches[0][0];
			if(preg_match_all('/hyPieData={(.*?)]/', $text, $matches2))
			{
				$tmp=$matches2[1][0];
				$str=array();
				if(preg_match_all("/name:'(.*?)',y:([\d.]+)\}/", $tmp, $matches2))
				{
					$names=$matches2[1];
					$percent=$matches2[2];
					foreach ($names as $index=>$item)
					{
						$str[]=$item.','.$percent[$index];
					}
					$data['trade']=implode('|',$str);
				}
			}
			else
			{
				app::log("ERROR FOUND hyPieData {$code}");
			}
			if(preg_match_all('/zcPieData={(.*?)]/', $text, $matches2))
			{
				$tmp=$matches2[1][0];
				$str=array();
				if(preg_match_all("/name:'(.*?)',y:([\d.]+)\}/", $tmp, $matches2))
				{
					$names=$matches2[1];
					$percent=$matches2[2];
					foreach ($names as $index=>$item)
					{
						$str[]=$item.','.$percent[$index];
					}
					$data['asset']=implode('|',$str);
				}
			}
			else
			{
				app::log("ERROR FOUND zcPieData {$code}");
			}
		}
		else
		{
			app::log("JS Data Not Match {$code}");
		}
		//store to mysql
		if(!empty($data['increase']) && !empty($data['increase_avg']) && !empty($data['increase_300']) )
		{
			$sql="UPDATE `fund_data` SET `increase`=:increase,`increase_avg`=:increase_avg,`increase_300`=:increase_300,`asset`=:asset,`trade`=:trade,`update_date`=:update_date WHERE code='{$code}' ";
			$stm=DB::prepare($sql);
			$now=date('Y-m-d H:i:s');
			$stm->bindParam(':increase',$data['increase']);
			$stm->bindParam(':increase_avg',$data['increase_avg']);
			$stm->bindParam(':increase_300',$data['increase_300']);
			$stm->bindParam(':asset',$data['asset']);
			$stm->bindParam(':trade',$data['trade']);
			$stm->bindParam(':update_date',$now);
			$ret=$stm->execute();
			self::log("update {$code} increase asset trade => {$ret}");
		}
		else
		{
			self::log("ERROR DATA for {$code}",true);
		}


	}
	//获取货币基金最新净值两条,执行较快（10分钟）,非货币基金不可执行,会导致range有问题
	public function newvalue($code=470028,$codes=null)
	{
		if(!$codes)
		{
			$codes=DB::getData("SELECT DISTINCT code FROM `fund_info` WHERE fund_type='理财型' OR fund_type='货币型' ORDER BY `code` ");
		}
		$codes=array_column($codes,'code');
		if(!in_array($code,$codes))
		{
			//必须是货币基金
			self::log("{$code} is not correct");
			return;
		}
		$url="http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={$code}&page=1&per=2&sdate=&edate";
		$html=file_get_contents($url);
		$data=array();
		if(preg_match_all('/(?:201\d-\d{2}-\d{2})(?:.*?)<\/tr>/', $html, $matches))
		{
			foreach ($matches[0] as $item)
			{
				$date=substr($item,0,10);
				$data[$date]=array();
				if(preg_match_all('/<td.*?[\d%]*<\/td>/', substr($item, 10), $matches2))
				{
					$all=str_replace('%','',trim(strip_tags($matches2[0][1])));
					$data[$date]['unit']=trim(strip_tags($matches2[0][0]));
					$data[$date]['all']=$all;
					$data[$date]['range']=sprintf('%.6f',$data[$date]['unit']/100).'%';
				}
			}
		}
		// var_dump($data);die;
		//store to mysql
		$sql="INSERT INTO `fund_value` (`code`,`unit`,`all`,`range`,`create_date`,`update_date`) VALUES ('{$code}',:unit,:all,:range,:create_date,:update_date) ON DUPLICATE KEY UPDATE update_date=:update_date,`range`=:range,`unit`=:unit,`all`=:all";
		$stm=DB::prepare($sql);
		$now=date('Y-m-d H:i:s');
		foreach($data as $date=>$item)
		{
			if(isset($item['unit'],$item['all'],$item['range']) && $item['all']!=='')
			{
				$stm->bindParam(':unit',$item['unit']);
				$stm->bindParam(':all',$item['all']);
				$stm->bindParam(':range',$item['range']);
				$stm->bindParam(':create_date',$date);
				$stm->bindParam(':update_date',$now);
				$stm->execute();
				self::log("update {$date} for {$code}");
			}
			else
			{
				self::log("Not Found Value Data {$code}",true);
			}
		}

	}

	/**
	 * fund_list 数据表更新，所有基金的最新数据,执行较快（5分钟）
	 */
	public function fundranking()
	{
		$html=file_get_contents("http://www.howbuy.com/fund/fundranking/");
		// $html=file_get_contents('1.tmp');
		$data=array();
		$pattern='/class="tdl"><a target="_blank" href="\/fund\/(\w{6})\/">(.*?)<\/a>[\s\S]*?width="5%">((?:\d{2}-\d{2})|--)<\/td>\s*<td width="6%" class="tdr">([\d.\-]+)<\/td>[\s\S]*?<td width="7%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td><td width="\d+%".*?([\d.%\-]+)(?:<\/span>)?<\/td>/';
		if(preg_match_all('/<tr><td width="4%" class="ck">[\s\S]+?<\/tr>/', $html, $matches))
		{
			//3419个基金
			self::log("total find ".count($matches[0]));
			foreach ($matches[0] as $index => $item)
			{
				if(preg_match_all($pattern, $item, $matches2))
				{
					$code=$matches2[1][0];
					$name=trim($matches2[2][0]);
					$date=$matches2[3][0];
					$value=$matches2[4][0];
					$week=$matches2[5][0];
					$month=$matches2[6][0];
					$month3=$matches2[7][0];
					$month6=$matches2[8][0];
					$year1=$matches2[9][0];
					$year=$matches2[10][0];
					$range=$matches2[11][0];
					$data[$code]=array('name'=>$name,'date'=>$date,'value'=>$value,'week'=>$week,'month'=>$month,'month3'=>$month3,'month6'=>$month6,'year'=>$year,'year1'=>$year1,'range'=>$range);
				}
				else
				{
					self::log("Find one item not match {$index}",true);
				}
			}
			self::log("Get All Data ".count($data));
			$sql="INSERT INTO `fund_list` (`code`,`name`,`date`,`value`,`week`,`month`,`month3`,`month6`,`year`,`year1`,`create_date`) VALUES 
			(:code,:name,:date,:value,:week,:month,:month3,:month6,:year,:year1,:create_date) ON DUPLICATE KEY UPDATE 
			`name`=:name,`date`=:date,`value`=:value,`week`=:week,`month`=:month,`month3`=:month3,`month6`=:month6,`year`=:year,`year1`=:year1";
			$stm=DB::prepare($sql);
			$create_date=date('Y-m-d H:i:s');
			foreach ($data as $code => $item)
			{
				$stm->bindParam(':code',$code);
				$stm->bindParam(':name',$item['name']);
				$stm->bindParam(':date',$item['date']);
				$stm->bindParam(':value',$item['value']);
				$stm->bindParam(':week',$item['week']);
				$stm->bindParam(':month',$item['month']);
				$stm->bindParam(':month3',$item['month3']);
				$stm->bindParam(':month6',$item['month6']);
				$stm->bindParam(':year1',$item['year1']);
				$stm->bindParam(':year',$item['year']);//year1代表近一年，year代表今年以来
				$stm->bindParam(':create_date',$create_date);
				$ret=$stm->execute();
				self::log("update code {$code} at {$item['date']} => {$ret}");
			}
		}
		else
		{
			self::log("fundranking not match");
		}
	}

	/**
	 * 资产配置详细数据,同步，每周即可,执行较快，内存消耗大
	 */
	public function allocation()
	{
		ini_set('memory_limit','300M');
		$origin=self::getData("SELECT * FROM [dbo].[FUND_ALLOCATION] WHERE CROSSCODE=2 ORDER BY MASTERFUNDCODE ");
		self::log('Total Task '.count($origin));
		$exist=DB::getData("SELECT updateid FROM `fund_allocation` ");
		$exist=array_column($exist,'updateid');
		$sql="INSERT INTO `fund_allocation` (`code`,`reporttypeid`,`start_date`,`end_date`,`crosscode`,`crossname`,
			`crossname_en`,`stockamount`,`commonstock`,`depositoryreceipts`,`reit`,`fundvalue`,`fixedincome`,`bondamount`,`absvalue`,
			`preciousmetals`,`derivative`,`forward`,`future` ,`interimoption`,`warrant`,`swap`,`structuredproduct`,`buyingbackthesale`,
			`buyoutrepo`,`moneymarketinstrument`,`totaldepositreserve`,`othersasset`,`totalasset`,`update_time`,`updateid`) VALUES 
			(:code,:reporttypeid,:start_date,:end_date,:crosscode,:crossname,:crossname_en,:stockamount,:commonstock,:depositoryreceipts,:reit,:fundvalue,
			:fixedincome,:bondamount,:absvalue,:preciousmetals,:derivative,:forward,:future,:interimoption,:warrant,:swap,:structuredproduct,:buyingbackthesale,
			:buyoutrepo, :moneymarketinstrument,:totaldepositreserve,:othersasset,:totalasset,:update_time,:updateid
			) ";
		$stm=DB::prepare($sql);
		foreach ($origin as $item)
		{
			if(!in_array($item['UPDATEID'],$exist))
			{
				$crossname=iconv("GBK", "UTF-8", $item['CROSSNAME']);
				$stm->bindParam(':code',$item['MASTERFUNDCODE']);
				$stm->bindParam(':reporttypeid',$item['REPORTTYPEID']);
				$stm->bindParam(':start_date',$item['STARTDATE']);
				$stm->bindParam(':end_date',$item['ENDDATE']);
				$stm->bindParam(':crosscode',$item['CROSSCODE']);
				$stm->bindParam(':crossname',$crossname);
				$stm->bindParam(':crossname_en',$item['CROSSNAME_EN']);
				$stm->bindParam(':stockamount',$item['STOCKAMOUNT']);
				$stm->bindParam(':commonstock',$item['COMMONSTOCK']);
				$stm->bindParam(':depositoryreceipts',$item['DEPOSITORYRECEIPTS']);
				$stm->bindParam(':reit',$item['REIT']);
				$stm->bindParam(':fundvalue',$item['FUNDVALUE']);
				$stm->bindParam(':fixedincome',$item['FIXEDINCOME']);
				$stm->bindParam(':bondamount',$item['BONDAMOUNT']);
				$stm->bindParam(':absvalue',$item['ABSVALUE']);
				$stm->bindParam(':preciousmetals',$item['PRECIOUSMETALS']);
				$stm->bindParam(':derivative',$item['DERIVATIVE']);
				$stm->bindParam(':forward',$item['FORWARD']);
				$stm->bindParam(':future',$item['FUTURE']);
				$stm->bindParam(':interimoption',$item['INTERIMOPTION']);
				$stm->bindParam(':warrant',$item['WARRANT']);
				$stm->bindParam(':swap',$item['SWAP']);
				$stm->bindParam(':structuredproduct',$item['STRUCTUREDPRODUCT']);
				$stm->bindParam(':buyingbackthesale',$item['BUYINGBACKTHESALE']);
				$stm->bindParam(':buyoutrepo',$item['BUYOUTREPO']);
				$stm->bindParam(':moneymarketinstrument',$item['MONEYMARKETINSTRUMENT']);
				$stm->bindParam(':totaldepositreserve',$item['TOTALDEPOSITRESERVE']);
				$stm->bindParam(':othersasset',$item['OTHERSASSET']);
				$stm->bindParam(':totalasset',$item['TOTALASSET']);
				$stm->bindParam(':update_time',$item['UPDATETIME']);
				$stm->bindParam(':updateid',$item['UPDATEID']);
				$ret=$stm->execute();
				self::log("Update {$item['MASTERFUNDCODE']} => {$ret}");
			}

		}

	}

	/**
	 * 货币基金每日净值更新2条，执行快（10分钟）
	 */
	public function task4($goto=0)
	{
		$codes=DB::getData("SELECT DISTINCT code FROM `fund_info` WHERE fund_type='理财型' OR fund_type='货币型' ORDER BY `code` ");
		$total=count($codes);
		self::log("Total Task {$total}");
		foreach ($codes as $index=>$item)
		{
			if($index<$goto)continue;
			$code=trim($item['code']);
			$this->newvalue($code,$codes);
			$remain=$total-$index;
			self::log("current newvalue task {$index}=>{$code},remain {$remain}",true);
		}
		self::log("All FINISHED");

	}

	/**
	 * 更新  更新资产配置，基金涨幅 ,所有基金，执行较慢（2小时）
	 */
	function task5($goto=0)
	{
		$codes=DB::getData("SELECT DISTINCT code FROM `fund_info` ORDER BY `code` ");
		$total=count($codes);
		self::log("Total Task {$total}");
		foreach ($codes as $index => $item)
		{
			if($index<$goto)continue;
			$code=trim($item['code']);
			$this->assets($code);
			$remain=$total-$index;
			self::log("current asset task {$index}=>{$code},remain {$remain}",true);
		}
	}

	public function __destruct()
	{
		self::log("Work Finished !!!",true);
	}

// wangyu change start
	/**
	 * syn hushen300 into stock/test_sotck
	 * 由于数据库较小，直接全部拷贝
	 */
	function syn_hushen300() {
		$mysql_tbname = "stock";
		$mysql_fields = ["date", "open", "close", "high", "low", "deal", "volumn"];

		$mssql_tbname = "IDX_MKT_QUOTATION";
		$mssql_fields = ["TRADINGDATE", "OPENPRICE", "CLOSEPRICE", "HIGHPRICE", "LOWPRICE", "AMOUNT", "VOLUME"];

		$mysql_id_array = ["date"];
		$mssql_id = "TRADINGDATE";
		$addtion_where = "SYMBOL = '000300'";
		$pre_msdata_proc_function = null;

		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array, $mssql_id, $addtion_where, $pre_msdata_proc_function);
	}



	/**
	 * syn table fund_info in mysql
	 * source DB: mssql
	 * source table:  Fund_Maininfo, Fund_UnitClassInfo, Fund_FundManager,Fund_Prospectuses,
	 *                Fund_ShareChange, Fund_FIN_INDEX
	 */
	function syn_mysql_fund_info() {
		$sql="exec [dbo].graspFundInfo";
		$ret = self::runRootSql($sql);
		self::log("ret: {$ret}");
		//DB::runSql("TRUNCATE table test_fund_info");

		$mysql_tbname = "fund_info";
		$mysql_fields = ["name", "fullname", "code", "fund_type", "start_date", "status_desc", "exchange_status", "company", "manager", "rate_manage", "rate_hold", "scale_first", "mount_new", "bank", "scale_new", "invest_goal", "invest_range", "invest_plan", "invest_gain", "invest_risk"];
		$mssql_tbname = "table_test_res";
		$mssql_fields = ["SHORTNAME", "FULLNAME", "SYMBOL", "CATEGORY", "INCEPTIONDATE", "FUNDSTATUS", "trade status", "FUNDCOMPANYNAME", "ManagerName", "MANAGEMENTFEE", "CUSTODIANFEE", "INCEPTIONTNA", "EndDateShares", "CUSTODIAN", "TotalTNA", "INVESTMENTGOAL", "INVESTMENTSCOPE", "STRATEGY", "invest return", "RISKDESCRIPTION"];
		$mysql_id_array = ["name", "fullname", "code", "fund_type"];
		$mssql_id = "SYMBOL";
		$addtion_where = null;
		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array, $mssql_id, $addtion_where, 'self::pre_process_info');

		$this->syn_fund_info_from_net();
	}

	function pre_process_info($data) {
		echo "pre_process_info\n";
		$new_data = [];
		foreach ($data as $index => $item) {

			if ($item['MANAGEMENTFEE'] != null) {
				if ($item['MANAGEMENTFEE'] < 0) {
					$item['MANAGEMENTFEE'] = "0".$item['MANAGEMENTFEE'];
				}
				$item['MANAGEMENTFEE'] =  $item['MANAGEMENTFEE']."%";
			} else {
				$item['MANAGEMENTFEE'] = "0%";
			}

			if ($item['CUSTODIANFEE'] != null) {
				if ($item['CUSTODIANFEE'] < 0) {
					$item['CUSTODIANFEE'] = "0".$item['CUSTODIANFEE'];
				}
				$item['CUSTODIANFEE'] = (float) $item['CUSTODIANFEE']."%";
			} else {
				$item['CUSTODIANFEE'] = "0";
			}
			if ($item['INCEPTIONTNA'] == null) {
				$item['INCEPTIONTNA']  = -1;
			} else {	
				$tmp_float = (float) $item['INCEPTIONTNA'];
				$tmp_float = $tmp_float / 100000000;
				$tmp_float = number_format($tmp_float, 2, '.', '');
				$item['INCEPTIONTNA'] =$tmp_float."亿";
			}
			if ($item['EndDateShares'] == null) {
				$item['EndDateShares'] = -1;
			} else {
				$tmp_float = (float) $item['EndDateShares'];
				$tmp_float = $tmp_float / 10000;
				$tmp_float = number_format($tmp_float, 2, '.', '');
				$item['EndDateShares'] =$tmp_float."万份";
			}
			if ($item['TotalTNA'] == null) {
				$item['TotalTNA']  = $item['TotalTNA'];
			} else {
				$tmp_float = (float) $item['TotalTNA'];
				$tmp_float = $tmp_float / 100000000;
				$tmp_float = number_format($tmp_float, 2, '.', '');
				$item['TotalTNA'] =$tmp_float."亿";
			}
			$item['trade status'] = "trade status";
			array_push($new_data, $item);
		}
		return $new_data;
	}

	function syn_fund_info_from_net() {
		// contruct url
		$url_list = $this->contruct_fund_info_urls();
		$insert_count = 0;
		$sql = "";
		$margin = 50;
		foreach ($url_list as $url) {
			$this->execute_syn_fund_info($url[1], $url[0], $insert_count, $sql, $margin);
		}
		if ($insert_count > 0) {
			echo "do sql:\n";
			$sql = $sql." ON DUPLICATE KEY UPDATE exchange_status=VALUES(exchange_status)";
			var_dump($sql);
			$ret=DB::runSql($sql);
		}
	}

	function contruct_fund_info_urls() {
		// get symbol array
		$sql = "select code from test_fund_info";
		$data = DB::getData($sql);
		$prefix = "http://www.howbuy.com//fund//ajax//gmfund//fundsummary.htm?jjdm=";
		$url_list = [];
		foreach ($data as $item) {
			$tmp_list = [];
			$tmp_s = $prefix;
			$tmp_s = $tmp_s.$item["code"];
			array_push($tmp_list, $item["code"]);
			array_push($tmp_list, $tmp_s);
			array_push($url_list, $tmp_list);
		}
		return $url_list;
	}

	function execute_syn_fund_info($url, $fund_code, &$insert_count, &$sql, $margin) {
		$html_data = file_get_contents($url);
		$info_list = [];
		$flag = $this->parse_one_fund_info($html_data, $info_list);
		if ($flag == false) {
			return;
		}
		//echo "info list\n";
		//var_dump($info_list);

		if ($insert_count != 0) {

		} else {
			$sql = "INSERT INTO `fund_info` (`code`, `exchange_status`) values ";
		}
		if (substr($sql, -1) != " ") {
			$sql = $sql . ", ";
		}
		$sql = $sql . "('{$fund_code}', '{$info_list["exchange_status"]}')";

		if (($insert_count % $margin) == ($margin - 1)) {
			echo "do sql\n";
			echo "wangyu split--\n";
			var_dump($sql);
			$sql = $sql . " ON DUPLICATE KEY UPDATE exchange_status=VALUES(exchange_status)";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count++;
		}

	}

	function parse_one_fund_info($html_data, &$info_list){
		$field = "exchange_status";
		$info_list[$field] = "";
		$ret = preg_match_all('/交易状态([\s\S\w\W\d\D]*?)<\/tr>/', $html_data, $cur_data);
		if ($ret > 0) {
			$data = $cur_data[0][0];
			$start_p = strpos($data, "<span>", 0);
			$end_p = 0;
			$s1 = $this->get_patten_string_right_pos($data, "<span>", "</span>", $start_p, $end_p);
			var_dump($s1);
			if ($s1 ===false) {
				$s1 = "";
			} else {
				$s1 = trim($s1, "<span>");
				$s1 = trim($s1, "<\/span>");
				$s1 = trim($s1);
			}

			$start_p = strpos($data, "<span>", $end_p+1);
			$end_p = 0;
			$s2 = $this->get_patten_string_right_pos($data, "<span>", "</span>", $start_p, $end_p);
			var_dump($s2);
			if ($s2 ===false) {
				$s2 = "";
			} else {
				$s2 = trim($s2, "<span>");
				$s2 = trim($s2, "<\/span>");
				$s2 = trim($s2);
			}

			$start_p = strpos($data, "<span>", $end_p+1);
			$end_p = 0;
			$s3 = $this->get_patten_string_right_pos($data, "<span>", "</span>", $start_p, $end_p);
			var_dump($s3);
			echo "end\n";
			if ($s3 ===false) {
				$s3 = "";
			} else {
				$s3 = trim($s3, "<span>");
				$s3 = trim($s3, "<\/span>");
				$s3 = trim($s3);
			}
			

			$info_list[$field] = $s1." ".$s2." ".$s3;
			var_dump($info_list);
			return true;
		}
		return false;
	}


/*
	function syn_mysql_fund_info() {
		$sql="exec [dbo].graspFundInfo";
		$ret = self::runRootSql($sql);
		self::log("ret: {$ret}");
		if ($ret >= 0) {
			DB::runSql("TRUNCATE table test_fund_info");
			$sql = 'select * from [dbo].[table_test_res] ORDER BY SYMBOL';
			$data = self::getData($sql);
			$sql = "INSERT into `test_fund_info` (`name`, `fullname`, `code`, `fund_type`, `start_date`, `status_desc`, `exchange_status`, `company`, `manager`, `rate_manage`, `rate_hold`, `scale_first`, `mount_new`, `bank`, `scale_new`, `invest_goal`, `invest_range`, `invest_plan`, `invest_gain`, `invest_risk`) values";
			$total = count($data);
			self::log("total count: {$total}");
			foreach ($data as $index => $item) {
				self::log("index: {$index}", false);
				if ( (($index % 50) != 1 && $index != 0) || $index == 1){
					$sql = $sql . ",";
				}
				$sql = $sql . " (";
				if ($item['MANAGEMENTFEE'] != null) {
					$MANAGEMENTFEE = (float) $item['MANAGEMENTFEE'];
				} else {
					$MANAGEMENTFEE = 1;
				}
				if ($item['CUSTODIANFEE'] != null) {
					$CUSTODIANFEE = (float) $item['CUSTODIANFEE'];
				} else {
					$CUSTODIANFEE = -1;
				}
				if ($item['INCEPTIONTNA'] != null) {
					$INCEPTIONTNA = $item['INCEPTIONTNA'];
				} else {
					$INCEPTIONTNA = -1;
				}
				if ($item['EndDateShares'] != null) {
					$EndDateShares = $item['EndDateShares'];
				} else {
					$EndDateShares = -1;
				}
				if ($item['TotalTNA'] != null) {
					$TotalTNA = $item['TotalTNA'];
				} else {
					$TotalTNA = -1;
				}
				$sql = $sql . "'{$item['SHORTNAME']}', '{$item['FULLNAME']}', '{$item['SYMBOL']}', '{$item['CATEGORY']}', '{$item['INCEPTIONDATE']}', '{$item['FUNDSTATUS']}', 'trade status', '{$item['FUNDCOMPANYNAME']}', '{$item['ManagerName']}', '{$MANAGEMENTFEE}%', '{$CUSTODIANFEE}%', {$INCEPTIONTNA}, {$EndDateShares}, '{$item['CUSTODIAN']}', {$TotalTNA}, '{$item['INVESTMENTGOAL']}', '{$item['INVESTMENTSCOPE']}', '{$item['STRATEGY']}', 'invest return', '{$item['RISKDESCRIPTION']}'";

				/*
				$sql = $sql . "1, '{$item['SHORTNAME']}',
				'{$item['FULLNAME']}',
				'{$item['SYMBOL']}',
				'{$item['CATEGORY']}',
				'{$item['INCEPTIONDATE']}',
				'{$item['FUNDSTATUS']}',
				'trade status',
				'{$item['FUNDCOMPANYNAME']}',
				'{$item['ManagerName']}',
        {$item['MANAGEMENTFEE']},
        {$item['CUSTODIANFEE']},
        {$item['INCEPTIONTNA']},
        {$item['EndDateShares']},
        '{$item['CUSTODIAN']}',
        {$item['TotalTNA']},
        '{$item['INVESTMENTGOAL']}',
        '{$item['INVESTMENTSCOPE']}',
        '{$item['STRATEGY']}',
        'invest return',
        '{$item['RISKDESCRIPTION']}'";
				*/
				/*

				$sql = $sql . ")";

				if ( (($index % 50) == 0 || $total -1 == $index ) && $index != 0) {
					// run sql
					$sql = $sql . ";";
					$ret=DB::runSql($sql);
					if ($ret < 1) {
						self::log("Error in syn_mysql_fund_info runsql, index={$index}",false);
					} else {
						self::log("modify rows: '{$ret}'");
					}
					$sql = "INSERT into `test_fund_info` (`name`, `fullname`, `code`, `fund_type`, `start_date`, `status_desc`, `exchange_status`, `company`, `manager`, `rate_manage`, `rate_hold`, `scale_first`, `mount_new`, `bank`, `scale_new`, `invest_goal`, `invest_range`, `invest_plan`, `invest_gain`, `invest_risk`) values";
				}
			}

		}

	}
*/
	/**
	 * syn table fund_stock in mysql
	 * source DB: mssql
	 * source table:  FUND_PORTFOLIO_STOCK(基金投组股票明细)
	 *
	 * 测试目的：原程序必须在windows中运行 否则FUND_PORTFOLIO_STOCK会出现乱码的情况
	 *           并且运行时好内存和耗时都较大，
	 *           此处修改针对这个情况，做修正
	 */
	function syn_mysql_fund_stock() {

		echo "syn_mysql_fund_stock start\n";
		ini_set('memory_limit','512M');

		$start_time=microtime(true);

		$sql = "SELECT top 1 UPDATEID FROM [dbo].[FUND_PORTFOLIO_STOCK] ORDER BY UPDATEID desc";
		$update_id_array = self::getData($sql);
		if (count($update_id_array) > 0) {
			$max_update_id = (int) ($update_id_array[0]['UPDATEID']);
		} else {
			$max_update_id = 0;
		}
		$margin = 10000;

		var_dump($max_update_id);
		echo "get target updateid start\n";

		/*
		 * memory consume too large
		$sql="SELECT `updateid` FROM `fund_stock` order by updateid";
		$target_data = DB::getData($sql);
		$target_size = count($target_data);
		*/
		$sql="SELECT `updateid` FROM `test_fund_stock` order by updateid desc limit 1";
		$target_id_array = DB::getData($sql);
		if (count($target_id_array) > 0) {
			$max_target_id = $target_id_array[0]['updateid'];
		} else {
			$max_target_id = -1;
		}
		echo "max target id: "."$max_target_id"."\n";
		$target_margin = 100000;
		$target_id_start = 0;
		$target_id_end = $target_id_start + $target_margin;
		$target_data = [];
		while ($target_id_start <= $max_target_id) {
			$sql = "SELECT `updateid` FROM `test_fund_stock` where updateid>={$target_id_start} and updateid<{$target_id_end} order by updateid";
			$target_part_id = DB::getData($sql);
			foreach ($target_part_id as $item) {
				array_push($target_data, $item['updateid']);
			}
			$target_id_start += $target_margin;
			$target_id_end += $target_margin;
		}
		$target_size = count($target_data);

		echo "get target updateid end "."$target_size"."\n";

		$source_id_start = 0;
		$source_id_end = $source_id_start + $margin;
		$target_start = 0;
		$hit_count = 0;
		$missing_count = 0;
		$error_count = 0;
		$insert_margin = 100;
		while ($source_id_start <= $max_update_id) {
			list($target_start, $part_hit, $part_miss, $part_error) = $this->syn_mysql_fund_stock_partial($source_id_start, $source_id_end,	$target_data, $target_start, $target_size, $insert_margin);
			$hit_count += $part_hit;
			$missing_count += $part_miss;
			$error_count += $part_error;
			$source_id_start += $margin;
			$source_id_end += $margin;
		}

		$end_time=microtime(true);
		$expire_time = $end_time - $start_time;
		echo "hit: "."$hit_count"."\n";
		echo "missing: "."$missing_count"."\n";
		echo "error: "."$error_count"."\n";
		if(substr_count($expire_time,"E")){
			$float_total = floatval(substr($expire_time,5));
			$total = $float_total/100000;
			echo "syn_mysql_fund_stock time: "."$total".'秒';
		}

	}

	function syn_mysql_fund_stock_partial($souce_id_start, $source_id_end, $target_data, $target_start, $target_size, $insert_margin) {

		echo "syn_mysql_fund_stock_partial start"."$souce_id_start"." "."$source_id_end"." "."$target_start";

		// get data from
		$sql = "SELECT MASTERFUNDCODE, STOCKNAME, SYMBOL, PROPORTION, SHARES, MARKETVALUE, UPDATETIME, STARTDATE, ENDDATE, UPDATEID FROM [dbo].[FUND_PORTFOLIO_STOCK] where UPDATEID >= '{$souce_id_start}' and UPDATEID < '{$source_id_end}' ORDER BY UPDATEID";
		$source_data = self::getData($sql);
		$source_size = count($source_data);

		echo "souce_size: "."$source_size"."\n";

		$missing_count = 0;
		$hit_count = 0;
		$error_count = 0;
		$pos_target = $target_start;
		$pos_source = 0;
		$sql = "";
		$insert_count = 0;
		for (; $pos_source < $source_size; ) {
			for (; $pos_target < $target_size; ) {
				$source_id = $source_data[$pos_source]['UPDATEID'];
				$target_id = $target_data[$pos_target];
				if ($source_id == $target_id) {
					$pos_source ++;
					$pos_target ++;
					$hit_count ++;
					break;
				} else if ($source_id < $target_id) {
					// need to insert
					// add data
					self::execute_syn_fund_stock($sql, $source_data[$pos_source], $insert_count, $insert_margin);
					$pos_source ++;
					$missing_count ++;
					break;
				} else {
					// error , should not happen
					$pos_target ++;
					$error_count ++;
				}
			}
			if ($pos_target == $target_size) {
				break;
			}
		}

		for ( ; $pos_source < $source_size; $pos_source ++) {
			$missing_count ++;
			self::execute_syn_fund_stock($sql, $source_data[$pos_source], $insert_count, $insert_margin);
		}
		if ($insert_count > 0) {
			echo "do sql:\n";
			$ret=DB::runSql($sql);
		}

		echo "syn_mysql_fund_stock end: "."$pos_target"." "."$hit_count"." "."$missing_count"." "."$error_count"."\n";
		return array($pos_target, $hit_count, $missing_count, $error_count);

	}

	function execute_syn_fund_stock(&$sql, $item, &$insert_count, $margin) {
		// add sql
		if ($insert_count != 0) {
			$sql = $sql.", ";
		} else {
			$sql = "INSERT INTO `test_fund_stock` (`fund_code`,`code`,`name`,`percent`,`mount`,`price`,`update_date`,`start_date`,`end_date`,`updateid`) VALUES ";
		}
		$stock_name = $item['STOCKNAME'];
		$stock_name = str_replace("'", "''", $stock_name);
		$sql = $sql."( '{$item['MASTERFUNDCODE']}', '{$item['SYMBOL']}', '$stock_name', {$item['PROPORTION']}, ";
		if ($item['SHARES'] == null) {
			$sql = $sql."null, {$item['MARKETVALUE']}, '{$item['UPDATETIME']}', '{$item['STARTDATE']}', '{$item['ENDDATE']}', {$item['UPDATEID']})";
		} else {
			$sql = $sql."{$item['SHARES']}, {$item['MARKETVALUE']}, '{$item['UPDATETIME']}', '{$item['STARTDATE']}', '{$item['ENDDATE']}', {$item['UPDATEID']})";
		}
		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql:\n";
			$ret=DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	/** parse_manager_from tian tian ji jin wang------------------------------------
	 * syn mssql table: fund_manager_funds, the manager info for every funds
	 * basic struct is grasp from tiantian ji jin wang, lack of benchmark, rank, and the info of retired manager, fund size
	 * is not proper for unit
	 * addition info is from howbuy, but lack of the start_date and the end_date
	 * the start date and end date is from mssql
	 */

	function syn_fund_manager_funds_from_TT() {

		//DB::runSql("TRUNCATE table test_fund_manager_funds_TT");
		// generate manager info url
		$url="http://fund.eastmoney.com/manager/";
		$html_data = file_get_contents($url);

		// get table
		$url_list = [];
		//$ret = preg_match_all('/<tb([\s\S\w\W\d\D]*?)ody>/', $html_data, $match_tables);
		$p1 = stripos($html_data, "<tbody>");
		$p2 = stripos($html_data, "<\/tbody>");
		$match_tables = substr($html_data, $p1, $p2-$p1);
		$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $match_tables, $arrays);
		$this->construct_manager_url_TT($url_list, $arrays[0]);

		// get manager info from url and store it into table
		$insert_count = 0;
		$margin = 50;
		$sql = "";
		$sz = count($url_list);
		echo "size: {$sz}\n";
		//var_dump($url_list);
		foreach ($url_list as $url) {
			$this->execute_syn_fund_manager_info_TT($url[0], $url[1], $url[2], $sql, $insert_count, $margin);
		}
		if ($insert_count > 0) {
			echo "do sql:\n";
			//var_dump($sql);
			$sql = $sql." ON DUPLICATE KEY UPDATE payback=VALUES(payback), average=VALUES(average), rank=VALUES(rank)";
			$ret=DB::runSql($sql);
		}

		// end

	}

	function construct_manager_url_TT(&$url_list, $array) {

		foreach ($array as $item) {
			//var_dump($item);
			$ret = preg_match_all('/href="([\s\S\w\W\d\D]*?)</', $item, $part);
			if ($ret > 0) {
				//echo "wangyu 2\n";
				$tmp_list = [];
				$ret = preg_match_all('/".*"/', $part[0][0], $url);
				if ($ret>0) {
					$single_url = $url[0][0];
					//echo "{$single_url}\n";
					$len = strlen($single_url);
					$single_url = substr($single_url, 1, $len-2);
					array_push($tmp_list, $single_url);

					$p1 = strrpos($single_url, "/");
					$p2 = strrpos($single_url, ".");
					//echo "{$p1}, {$p2}\n";
					if ($p1 > 0 && $p2 > 0 && $p2 > $p1) {
						$manager_id = substr($single_url, $p1+1, $p2-$p1-1);
					} else {
						$manager_id = "NA";
					}
					array_push($tmp_list, $manager_id);
				}

				$ret = preg_match_all('/">.*</', $part[0][0], $name);
				if ($ret>0) {
					$manager_name = $name[0][0];
					//echo "{$single_url}\n";
					$len = strlen($manager_name);
					$manager_name = substr($manager_name, 2, $len-3);
					array_push($tmp_list, $manager_name);
				}
				array_push($url_list, $tmp_list);
			}
		}
	}

	function execute_syn_fund_manager_info_TT($url, $manager_id, $manager_name, &$sql, &$insert_count, $margin) {
		// get information
		var_dump($manager_name);
		$html_data = file_get_contents($url);
		$info_list = [];
		$flag = $this->parse_one_manager_info_TT($html_data, $info_list);
		if ($flag == false) {
			return;
		}
		//echo "info list\n";
		//var_dump($info_list);

		if ($insert_count != 0) {

		} else {
			$sql = "INSERT INTO `fund_manager_funds` (`fund_code`, `fund_name`, `manager_code`, `manager_name`, `fund_type`, `start_date`, `end_date`,`payback`, `average`, `rank`, `avatar`) values ";
		}
		foreach($info_list as $info) {
			if (substr($sql, -1) != " ") {
				$sql = $sql.", ";
			}
			$sql = $sql."('{$info["fund_code"]}', '{$info["short_name"]}', '{$manager_id}', '{$manager_name}', '{$info["type"]}', '{$info["start_date"]}', '{$info["end_date"]}', '{$info["pay_back"]}','{$info["benchmark"]}','{$info["rank"]}', '{$info["avatar"]}')";
		}

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			echo "wangyu split--\n";
			var_dump($sql);
			$sql = $sql." ON DUPLICATE KEY UPDATE payback=VALUES(payback), average=VALUES(average), rank=VALUES(rank), avatar=VALUES(avatar)";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	function parse_one_manager_info_TT($html_data, &$info_list) {
		// get manager's photo
		$ret = preg_match_all('/photo([\s\S\w\W\d\D]*?)\/>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/src="([\s\S\w\W\d\D]*?)"/', $cur_data[0][0], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = 5;
				$avatar = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$avatar = "--";
			}
		} else {
			$avatar = "--";
		}

		//parser 现任基金业绩与排名详情
		$ret = preg_match_all('/现任基金业绩与排名详情([\s\S\w\W\d\D]*?)<\/table>/', $html_data, $cur_data);
		if ($ret == 0) {
			return false;
		}
		$ret = preg_match_all('/<tbody>([\s\S\w\W\d\D]*?)<\/tbody>/', $cur_data[0][0], $cur_data);
		$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $cur_data[0][0], $funds_list);
		$funds_list = $funds_list[0];
		$cur_info_list = [];
		foreach ($funds_list as $fund) {
			$ret = preg_match_all('/<td([\s\S\w\W\d\D]*?)<\/td>/', $fund, $items);
			$items = $items[0];
			if (count($items) < 5) {
				continue;
			}

			$tmp_array = [];

			// id
			$ret = preg_match_all('/href=([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[0], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["fund_code"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["fund_code"] = "NA";
			}

			// name
			$ret = preg_match_all('/href=([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[1], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["short_name"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["short_name"] = "--";
			}

			// type
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[2], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["type"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["type"] = "--";
			}

			// bench_mark
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[11], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["benchmark"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["benchmark"] = "--";
			}

			// rank
			$ret = preg_match_all('/<span([\s\S\w\W\d\D]*?)</', end($items), $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmpS_1 = substr($tmpS, $firstP, $len-1-$firstP);
				$ret = preg_match_all('/\|<\/span>([\s\S\w\W\d\D]*?)</', end($items), $res_string);
				if ($ret > 0) {
					$tmpS = $res_string[0][0];
					$len = strlen($tmpS);
					$firstP = stripos($tmpS, ">") + 1;
					$tmpS_2 = substr($tmpS, $firstP, $len-1-$firstP);
					$tmpS = $tmpS_1."|";
					$tmpS = $tmpS.$tmpS_2;
					$tmp_array["rank"] = $tmpS;
				}
			} else {
				$tmp_array["rank"] = "--";
			}


			$tmp_array["avatar"] = $avatar;

			array_push($cur_info_list, $tmp_array);
		}


		// paser 管理过的基金一览
		$ret = preg_match_all('/管理过的基金一览([\s\S\w\W\d\D]*?)<\/table>/', $html_data, $hist_data);
		$ret = preg_match_all('/<tbody>([\s\S\w\W\d\D]*?)<\/tbody>/', $hist_data[0][0], $hist_data);
		$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $hist_data[0][0], $funds_list);
		$funds_list = $funds_list[0];
		$hist_info_list = [];
		foreach ($funds_list as $fund) {
			$ret = preg_match_all('/<td([\s\S\w\W\d\D]*?)<\/td>/', $fund, $items);
			$items = $items[0];
			if (count($items) < 9) {
				continue;
			}

			$tmp_array = [];

			// id
			$ret = preg_match_all('/href=([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[0], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["fund_code"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["fund_code"] = "NA";
			}

			// start_date
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[5], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["start_date"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["start_date"] = "--";
			}

			// end_date
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[6], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["end_date"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["end_date"] = "--";
			}

			// pay_back
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[8], $res_string);
			if ($ret > 0) {
				$tmpS = $res_string[0][0];
				$len = strlen($tmpS);
				$firstP = stripos($tmpS, ">") + 1;
				$tmp_array["pay_back"] = substr($tmpS, $firstP, $len-1-$firstP);
			} else {
				$tmp_array["pay_back"] = "--";
			}
			array_push($hist_info_list, $tmp_array);
		}

		$cur_id_list = [];
		// compare
		$cur_size = count($cur_info_list);
		$hist_size = count($hist_info_list);
		//var_dump($cur_info_list);
		//var_dump($hist_info_list);
		for ($cur_pos=0; $cur_pos < $cur_size; $cur_pos ++) {
			//var_dump($cur_info_list[$cur_pos]);
			$cur_id = $cur_info_list[$cur_pos]["fund_code"];
			//var_dump($cur_id);
			if ($cur_id == "NA" || $cur_id == "--") {
				continue;
			}
			for ($hist_pos=0; $hist_pos < $hist_size; $hist_pos ++) {
				//var_dump($hist_info_list[$hist_pos]);
				$hist_id = $hist_info_list[$hist_pos]["fund_code"];
				//var_dump($hist_id);
				if ($hist_id == "NA" || $hist_id == "--") {
					continue;
				}

				if ($hist_id == $cur_id) {
					if (in_array($hist_id, $cur_id_list)){
						break;
					}
					//echo "hit\n";
					$tmp_array = $cur_info_list[$cur_pos];
					$tmp_array1 = $hist_info_list[$hist_pos];
					$tmp_array["start_date"] = $tmp_array1["start_date"];
					$tmp_array["end_date"] = $tmp_array1["end_date"];
					$tmp_array["pay_back"] = $tmp_array1["pay_back"];
					//var_dump($tmp_array);
					$cur_info_list[$cur_pos] = $tmp_array;
					break;
				}
			}

			array_push($info_list, $cur_info_list[$cur_pos]);
			array_push($cur_id_list, $cur_id);

		}

		return true;
	}

	// ------------------------------------------------------------------------------

	/**
	 * update table: fund_manager_funds
	 * start_date, end_date: get from sql server
	 */
	function syn_fund_manager_funds() {

		DB::runSql("TRUNCATE table test_fund_manager_funds");
		// generate manager info url
		$url="http://www.howbuy.com/fund/manager/";
		$html_data = file_get_contents($url);

		// get table
		$url_list = [];
		$ret = preg_match_all('/<table([\s\S\w\W\d\D]*?)<\/table>/', $html_data, $match_tables);
		$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $match_tables[0][0], $arrays);
		$this->construct_manager_url($url_list, $arrays[0]);

		$ret = preg_match_all('/<textarea([\s\S\w\W\d\D]*?)<\/textarea>/', $html_data, $match_tables);
		foreach($match_tables[0] as $items) {
			//echo "wangyu\n";
			//var_dump($items);
			$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $items, $arrays);
			$this->construct_manager_url($url_list, $arrays[0]);
		}
		//var_dump($url_list);
		echo "wangyu split --\n";

		// get manager info from url and store it into table
		$insert_count = 0;
		$margin = 50;
		$sql = "";
		foreach ($url_list as $url) {
			$this->execute_syn_fund_manager_info($url[0], $url[1], $url[2], $sql, $insert_count, $margin);
		}
		if ($insert_count > 0) {
			echo "do sql:\n";
			$ret=DB::runSql($sql);
		}

		// end

	}

	function construct_manager_url(&$url_list, $array) {

		foreach ($array as $item) {
			//var_dump($item);
			$ret = preg_match_all('/<td><a href="([\s\S\w\W\d\D]*?) target="_blank([\s\S\w\W\d\D]*?)</', $item, $part);
			if ($ret > 0) {
				//echo "wangyu 2\n";
				$tmp_list = [];
				$ret = preg_match_all('/"\/.*\/"/', $part[0][0], $url);
				if ($ret>0) {
					$single_url = $url[0][0];
					//echo "{$single_url}\n";
					$len = strlen($single_url);
					if (substr($single_url, 1, 1) == "/") {
						$single_url = "http://www.howbuy.com".substr($single_url, 1, $len-2);
						array_push($tmp_list, $single_url);
					}
					$p1 = strrpos($single_url, "manager/");
					$len = strlen($single_url);
					$manager_id = substr($single_url, $p1+8, $len - $p1 - 9);
					array_push($tmp_list, $manager_id);
				}

				$ret = preg_match_all('/">.*</', $part[0][0], $name);
				if ($ret>0) {
					$manager_name = $name[0][0];
					$len = strlen($manager_name);
					$manager_name = substr($manager_name, 2, $len-3);
					array_push($tmp_list, $manager_name);
				}
				array_push($url_list, $tmp_list);
			}
		}

	}

	function execute_syn_fund_manager_info($url, $manager_code, $manager_name, &$sql, &$insert_count, $margin) {
		// get information
		$html_data = file_get_contents($url);
		$info_list = [];
		$flag = $this->parse_one_manager_info($html_data, $info_list);
		if ($flag == false) {
			echo "parse_one_manager_info error\n";
			return;
		}
		//echo "info list\n";
		//var_dump($info_list);

		if ($insert_count != 0) {

		} else {
			$sql = "INSERT INTO `test_fund_manager_funds` (`fund_code`, `fund_name`, `manager_code`, `manager_name`, `fund_type`, `payback`, `fund_size`, `maxlos`, `average`, `rank`) values ";
		}
		foreach($info_list as $info) {
			if (substr($sql, -1) != " ") {
				$sql = $sql.", ";
			}
			$sql = $sql."('{$info["fund_code"]}', '{$info["short_name"]}', '{$manager_code}', '{$manager_name}', '{$info["type"]}','{$info["pay_back"]}','{$info["fund_size"]}','{$info["draw_back"]}','{$info["benchmark"]}','{$info["rank"]}')";
		}

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			var_dump($sql);
			echo "wangyu split--\n";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	/**
	 * one url maps one manager info
	 */
	function parse_one_manager_info($html_data, &$info_list) {
		// get_name
		$ret = preg_match_all('/<title>([\s\S\w\W\d\D]*?) /', $html_data, $name_data);
		$manager_name = "";
		if ($ret > 0) {
			$tmpS = $name_data[0][0];
			$manager_name = substr($tmpS, 7, strlen($tmpS) - 8);
		}
		echo "manager_name {$manager_name}\n";

		$ret = preg_match_all('/基金概况([\s\S\w\W\d\D]*?)<\/table>/', $html_data, $extra_table_data);
		//echo "extra_table_data[0][0]";
		//var_dump($extra_table_data[0][0]);
		if (0 == $ret) {
			//var_dump($html_data);
			echo "no current fund\n";
		} else {
			$ret = preg_match_all('/<table([\s\S\w\W\d\D]*?)<\/table>/', $extra_table_data[0][0],  $table_data);
			$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $table_data[0][0], $fund_list);
			$fund_list = $fund_list[0];
			// 删除一个行标题栏
			array_shift($fund_list);
			foreach ($fund_list as $fund) {
				$ret = preg_match_all('/<td([\s\S\w\W\d\D]*?)<\/td>/', $fund, $items);
				$items = $items[0];
				$size = count($items);
				//echo "single item:\n";
				//var_dump($items);
				$tmp_array = [];
				for ($pos = 0; $pos < $size; $pos++) {
					switch ($pos) {
						case 0: // parse short name
							$ret = preg_match_all('/id=([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							//echo "pos=0 ";
							//var_dump($res_string);
							//var_dump($ret);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								//var_dump($tmpS);
								//var_dump($len);
								//var_dump($firstP);
								//var_dump(substr($tmpS, $firstP, $len-1-$firstP));
								$tmp_array["short_name"] = substr($tmpS, $firstP, $len-1-$firstP);
							}
							$ret = preg_match_all('/href=".*"/', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, "fund/") + 5;
								$tmp_array["fund_code"] = substr($tmpS, $firstP, $len-2-$firstP);
							} else {
								$tmp_array['fund_code'] = "NA";
							}
							//var_dump($tmp_array);
							//echo "pos =0 end \n";
							break;
						case 1: // parse type
							$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_array["type"] = substr($tmpS, 1, $len-2);
							}
							break;
						case 2: // parse size
							$ret = preg_match_all('/c333">([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_array["fund_size"] = trim(substr($tmpS, 6, $len-7));
							}
							break;
						case 3: // parse time
							$ret = preg_match_all('/c333">([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_array["time"] = substr($tmpS, 6, $len-7);
							}
							break;
						case 4: // parse max back draw
							$ret = preg_match_all('/span([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								$tmp_array["draw_back"] = substr($tmpS, $firstP, $len-1-$firstP);
							} else {
								$tmp_array["draw_back"] = "--";
							}
							break;
						case 5: // parse pay back
							$ret = preg_match_all('/span([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								$tmp_array["pay_back"] = substr($tmpS, $firstP, $len-1-$firstP);
							} else {
								$tmp_array["pay_back"] = "--";
							}
							break;
						case 6: // parse benchmark average
							$ret = preg_match_all('/span([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								$tmp_array["benchmark"] = substr($tmpS, $firstP, $len-1-$firstP);
							} else {
								$tmp_array["benchmark"] = "--";
							}
							break;
						case 7: // parse rank
							//echo "pos = 7";
							$ret = preg_match_all('/c333">([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_S1 = substr($tmpS, 6, $len-7);
								$ret = preg_match_all('/c999\'>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
								if ($ret > 0) {
									$tmpS = $res_string[0][0];
									$len = strlen($tmpS);
									$tmp_S2 = substr($tmpS, 6, $len-7);
									$tmpS = $tmp_S1.$tmp_S2;
									$tmp_array["rank"] = $tmpS;
								} else {
									$tmp_array["rank"] = "--";
								}
							} else {
								$tmp_array["rank"] = "--";
							}
							//var_dump($tmp_array);
							//echo "pos = 7 end\n";
							break;
						/*
                        case 8: // parse fund_code
                            $ret = preg_match_all('/fundCode=.*"/', $items[$pos], $res_string);
                            if ($ret > 0) {
                                $tmpS = $res_string[0][0];
                                $len = strlen($tmpS);
                                $firstP = stripos($tmpS, "=") + 1;
                                $tmp_array["fund_code"] = substr($tmpS, $firstP, $len-1-$firstP);
                            } else {
                                $tmp_array['fund_code'] = "NA";
                            }
                        */

					}

				}
				$tmp_array["manager_name"] = $manager_name;
				array_push($info_list, $tmp_array);
				//var_dump($tmp_array);
			}
		}

		// parse history fund
		echo "enter history\n";
		$startP = strpos($html_data, "<div class=\"history_content");
		if ($startP == false) {
			return true;
		}
		$endP = $this->get_pattern_pos($html_data, "<div", "</div", $startP+1);
		//var_dump($endP);
		if ($endP == false) {
			return true;
		}
		// delete titile
		$startP = strpos($html_data, "<tr", $startP + 10);
		if ($startP  == false) {
			return true;
		}
		//var_dump($startP);
		$startP = strpos($html_data, "<tr", $startP + 10);
		if ($startP  == false) {
			return true;
		}
		//var_dump($startP);
		$hist_data = substr($html_data, $startP - 1, $endP - $startP);
		$startP = strpos($hist_data, "<tr", 0);
		//echo "hist: data:\n";
		//var_dump($hist_data);
		//var_dump($startP);
		while ($startP !== false) {
			$next_endP = self::get_pattern_pos($hist_data, "<tr", "</tr", $startP+1);
			//var_dump($endP);
			if ($next_endP === false) {
				break;
			}
			$target_data = substr($hist_data, $startP, $next_endP - $startP + 1);
			echo "target_data: \n";
			//var_dump($target_data);
			// parse company
			$tmpS = $this->get_patten_string($target_data, "<td", "</td", 0, $endP);
			//var_dump($tmpS);
			//var_dump($endP);
			$tmpS = $this->get_patten_string($tmpS, "href=", "<", 0, $endP);
			$tmpS = $this->get_patten_string($tmpS, ">", "<", 0, $endP);
			$company_name = substr($tmpS, 1, strlen($tmpS) - 2);

			// parse  after
			$startP = $endP;
			$tmpS = $this->get_patten_string($target_data, "<td", "</td", $startP, $endP);
			//echo "part manager data: \n";
			//var_dump($tmpS);
			$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $tmpS, $fund_list);
			$fund_list = $fund_list[0];

			foreach ($fund_list as $fund) {
				$ret = preg_match_all('/<td([\s\S\w\W\d\D]*?)<\/td>/', $fund, $items);
				$items = $items[0];
				$size = count($items);
				//echo "single item:\n";
				//var_dump($items);
				$tmp_array = [];
				for ($pos = 0; $pos < $size; $pos++) {
					switch ($pos) {
						case 0: // parse short name
							$ret = preg_match_all('/id=([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							//echo "pos=0 ";
							//var_dump($res_string);
							//var_dump($ret);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								//var_dump($tmpS);
								//var_dump($len);
								//var_dump($firstP);
								//var_dump(substr($tmpS, $firstP, $len-1-$firstP));
								$tmp_array["short_name"] = substr($tmpS, $firstP, $len - 1 - $firstP);
							}
							$ret = preg_match_all('/href="([\s\S\w\W\d\D]*?)"/', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, "fund/") + 5;
								$tmp_array["fund_code"] = substr($tmpS, $firstP, $len - 2 - $firstP);
							} else {
								$tmp_array['fund_code'] = "NA";
							}
							//var_dump($tmp_array);
							//echo "pos =0 end \n";
							break;
						case 1: // parse type
							$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_array["type"] = substr($tmpS, 1, $len - 2);
							}
							break;
						case 3: // parse time
							$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_array["time"] = substr($tmpS, 6, $len - 7);
							}
							break;

						case 4: // parse pay back
							$ret = preg_match_all('/span([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								$tmp_array["pay_back"] = substr($tmpS, $firstP, $len - 1 - $firstP);
							} else {
								$tmp_array["pay_back"] = "--";
							}
							break;
						case 5: // parse benchmark average
							$ret = preg_match_all('/span([\s\S\w\W\d\D]*?)>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$firstP = stripos($tmpS, ">") + 1;
								$tmp_array["benchmark"] = substr($tmpS, $firstP, $len - 1 - $firstP);
							} else {
								$tmp_array["benchmark"] = "--";
							}
							break;
						case 6: // parse rank
							//echo "pos = 7";
							$ret = preg_match_all('/c333">([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
							if ($ret > 0) {
								$tmpS = $res_string[0][0];
								$len = strlen($tmpS);
								$tmp_S1 = substr($tmpS, 6, $len - 7);
								$ret = preg_match_all('/c999\'>([\s\S\w\W\d\D]*?)</', $items[$pos], $res_string);
								if ($ret > 0) {
									$tmpS = $res_string[0][0];
									$len = strlen($tmpS);
									$tmp_S2 = substr($tmpS, 6, $len - 7);
									$tmpS = $tmp_S1 . $tmp_S2;
									$tmp_array["rank"] = $tmpS;
								} else {
									$tmp_array["rank"] = "--";
								}
							} else {
								$tmp_array["rank"] = "--";
							}
							//var_dump($tmp_array);
							//echo "pos = 7 end\n";
							break;
						/*
                        case 8: // parse fund_code
                            $ret = preg_match_all('/fundCode=.*"/', $items[$pos], $res_string);
                            if ($ret > 0) {
                                $tmpS = $res_string[0][0];
                                $len = strlen($tmpS);
                                $firstP = stripos($tmpS, "=") + 1;
                                $tmp_array["fund_code"] = substr($tmpS, $firstP, $len-1-$firstP);
                            } else {
                                $tmp_array['fund_code'] = "NA";
                            }
                        */

					}

				}
				$tmp_array["manager_name"] = $manager_name;
				$tmp_array["fund_size"] = "--";
				$tmp_array["draw_back"] = "--";
				//echo "hist parse result: \n";
				//var_dump($tmp_array);
				array_push($info_list, $tmp_array);
			}

			$startP = strpos($hist_data, "<tr", $next_endP+1);
		}

		return true;

	}

	/*
 	 * update table test_fund_manager_funds mastercode,
	 * used for start time and end time update
	 * from mssql to sql
 	 */
	function syn_fund_manager_fund_mastercode () {
		// get  $source_data
		$sql = 'select SYMBOL, MASTERFUNDCODE from [dbo].[FUND_FUNDCODEINFO] ORDER BY SYMBOL';
		$source_data = self::getData($sql);
		$source_size = count($source_data);

		// get  $get target code ids
		$sql = 'SELECT fund_code from test_fund_manager_funds ORDER BY fund_code';
		$target_id_aray = DB::getData($sql);
		$target_size = count($target_id_aray);

		// check for match, then update
		$source_pos = 0;
		$target_pos = 0;
		$insert_count = 0;
		$sql = "";
		$id_list = "";
		$margin = 50;
		$source_miss = 0;
		$target_miss = 0;
		for (; $source_pos < $source_size; ) {
			for (; $target_pos < $target_size; ) {
				$source_id = $source_data[$source_pos]["SYMBOL"];
				$target_id = $target_id_aray[$target_pos]["fund_code"];

				if ($target_id == $source_id) {
					// do it
					self::execute_syn_fund_manager_funds_fund_mastercode(
						$sql, $id_list, $source_data[$source_pos], $insert_count, $margin
					);
					echo "{$source_id}, {$source_data[$source_pos]["MASTERFUNDCODE"]}, {$target_id_aray[$target_pos]["fund_code"]}\n";
					$source_pos ++;
					$target_pos ++;
					break;
				} else if ($source_id < $target_id) {
					// source miss
					$target_miss ++;

					$source_pos ++;
					break;
				} else {
					// target miss
					$source_miss ++;
					$target_pos ++;
				}
			}
			if ($target_pos >= $target_size) {
				break;
			}
		}

		// do remain
		if ($insert_count > 0) {
			echo "do sql:\n";
			$sql = $sql." END WHERE fund_code in ({$id_list}) ";
			$ret=DB::runSql($sql);
		}

		echo "source missing : ";
		echo "{$source_miss} \n";
		echo "target missing: ";
		echo "{$target_miss} \n";
	}

	function execute_syn_fund_manager_funds_fund_mastercode(&$sql, &$id_list, $item, &$insert_count, $margin) {

		if ($insert_count != 0) {
			$sql = $sql." ";
			$id_list = $id_list.",";
		} else {
			$sql = "update test_fund_manager_funds set master_fund_code = case ";
		}

		$sql = $sql."WHEN fund_code = {$item["SYMBOL"]} THEN '{$item["MASTERFUNDCODE"]}'";
		$id_list = $id_list." {$item["SYMBOL"]}";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql = $sql." END WHERE fund_code in ({$id_list}) ";
			$ret = DB::runSql($sql);
			$sql = "";
			$id_list = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	/**
	 * update table: fund_manager_funds,
	 * update start time and end time from mssql
	 */
	function syn_fund_manager_funds_SE_time () {
		// get  $source_data
		echo "get source data";
		$sql = 'select MASTERFUNDCODE, FULLNAME, SERVICESTARTDATE, SERVICEENDDATE from [dbo].[FUND_FUNDMANAGER] ORDER BY MASTERFUNDCODE, FULLNAME';
		$source_data = self::getData($sql);
		$source_size = count($source_data);

		// get  $get target code ids
		echo "get target data";
		$sql = 'SELECT `master_fund_code`, `manager_name` from test_fund_manager_funds  ORDER BY `master_fund_code`, `manager_name`';
		$target_data = DB::getData($sql);
		$target_size = count($target_data);
		/*
		echo "target: ";
		var_dump($target_data);
		echo "source: ";
		var_dump($source_data);
		*/
		// check for match, then update
		$insert_count = 0;
		$sql = "";
		$margin = 50;
		$source_miss = 0;
		$target_miss = 0;
		$source_start = 0;
		$source_end = -1;
		$target_start = 0;
		$target_end = -1;
		$sql_start = "";
		$sql_end = "";
		$symbol_list = "";
		$name_list = "";
		for (; $source_start < $source_size; ) {
			$this->get_start_and_end_pos($source_data, "MASTERFUNDCODE", $source_size, $source_start, $source_end);
			for (; $target_start < $target_size; ) {
				$this->get_start_and_end_pos($target_data, "master_fund_code", $target_size, $target_start, $target_end);
				echo "source: {$source_start}, {$source_end}; target: {$target_start}, {$target_end}  \n ";
				$source_id = $source_data[$source_start]["MASTERFUNDCODE"];
				$target_id = $target_data[$target_start]["master_fund_code"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_fund_manager_funds_SE_time_per_fund($sql_start, $sql_end, $symbol_list, $name_list, $insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin);

					if ($source_id == '000545') {
						var_dump($source_data[$source_start]);
						var_dump($source_data[$source_end]);
						var_dump($target_data[$target_start]);
						var_dump($target_data[$target_end]);
					}
					echo "hit {$source_id}, {$source_data[$source_start]["SERVICESTARTDATE"]}, {$target_data[$target_start]["master_fund_code"]}\n";
					$source_start = $source_end + 1;
					$target_start = $target_end + 1;
					break;
				} else if ($source_id < $target_id) {
					// target miss
					$target_miss ++;
					echo "target miss; source id : {$source_id}\n";

					$source_start = $source_end + 1;
					break;
				} else {
					// source miss
					$source_miss ++;
					echo "source miss; target id : {$target_id}\n";
					$target_start = $target_end + 1;
				}
			}
			if ($target_start >= $target_size) {
				break;
			}
		}

		// do remain
		if ($insert_count > 0) {
			echo "do sql:\n";
			$sql_start = $sql_start." END WHERE master_fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$sql_end = $sql_end." END WHERE master_fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$ret = DB::runSql($sql_start);
			$ret = DB::runSql($sql_end);
		}

		echo "source missing : ";
		echo "{$source_miss} \n";
		echo "target missing: ";
		echo "{$target_miss} \n";
	}

	function syn_fund_manager_funds_SE_time_per_fund(&$sql_start, &$sql_end, &$symbol_list, &$name_list, &$insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin) {
		$source_pos = $source_start;
		$target_pos = $target_start;
		for (; $source_pos < $source_end + 1; $source_pos ++) {
			for ($target_pos = $target_start; $target_pos < $target_end + 1; $target_pos ++) {
				$source_id = $source_data[$source_pos]["FULLNAME"];
				$target_id = $target_data[$target_pos]["manager_name"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_fund_manager_funds_SE_time_per_item($sql_start, $sql_end, $symbol_list, $name_list, $insert_count, $source_data[$source_pos], $margin);

					echo "{$source_id}, {$source_data[$source_pos]["SERVICESTARTDATE"]}, {$target_data[$target_pos]["manager_name"]}\n";
					break;
				}
			}
		}
	}

	function syn_fund_manager_funds_SE_time_per_item(&$sql_start, &$sql_end, &$symbol_list, &$name_list, &$insert_count, $item, $margin) {
		echo "in loop\n";
		// warning: add hoc: 韩会永：duplicate for 001003 in howbuy

		if ($insert_count != 0) {
			$sql_start = $sql_start." ";
			$sql_end = $sql_end." ";
			$name_list = $name_list.",";
			$symbol_list = $symbol_list.",";
		} else {
			$sql_start = "update ignore test_fund_manager_funds set start_date = case ";
			$sql_end = "update ignore test_fund_manager_funds set end_date = case ";
		}
		if ( strlen($item["SERVICESTARTDATE"]) > 2 ){
			$start_time = substr($item["SERVICESTARTDATE"], 0, 10);
		} else {
			$start_time = $item["SERVICESTARTDATE"];
		}

		if ($item["SERVICEENDDATE"] == null) {
			$end_time = '至今';
		} else if ( $item["SERVICEENDDATE"] > 10 ) {
			$end_time = substr($item["SERVICEENDDATE"], 0, 10);
		} else {
			$end_time = $item["SERVICEENDDATE"];
		}

		$sql_start = $sql_start." WHEN master_fund_code = {$item["MASTERFUNDCODE"]} and  manager_name = '{$item["FULLNAME"]}' THEN '{$start_time}'";
		$sql_end = $sql_end." WHEN master_fund_code = {$item["MASTERFUNDCODE"]} and  manager_name = '{$item["FULLNAME"]}' THEN '{$end_time}'";
		$symbol_list = $symbol_list." '{$item["MASTERFUNDCODE"]}'";
		$name_list = $name_list." '{$item["FULLNAME"]}'";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql_start = $sql_start." END WHERE master_fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$sql_end = $sql_end." END WHERE master_fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$ret = DB::runSql($sql_start);
			$ret = DB::runSql($sql_end);
			var_dump($sql_start);
			$sql_start = "";
			$sql_end = "";
			$name_list = "";
			$symbol_list = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	// ------------------------------------------------------------------------------

	/**
	 * syn info from howbuy to Tian tian
	 *
	 */
	function syn_fund_manager_funds_TT_from_How() {
		//$sql = "select fund_code, manager_name, fund_size, maxlos, average, rank, payback from test_fund_manager_funds order by fund_code";
		$sql = "select * from test_fund_manager_funds order by fund_code";
		$source_data = DB::getData($sql);
		$source_size = count($source_data);


		$sql = "select fund_code, manager_name from fund_manager_funds order by fund_code";
		$target_data = DB::getData($sql);
		$target_size = count($target_data);

		// check for match, then update
		$source_pos = 0;
		$target_pos = 0;
		$insert_count = 0;
		$margin = 50;
		$source_miss = 0;
		$target_miss = 0;
		$source_start = 0;
		$source_end = -1;
		$target_start = 0;
		$target_end = -1;
		$sql = "";
		$symbol_list = "";
		$name_list = "";
		for (; $source_start < $source_size; ) {
			$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);
			for (; $target_start < $target_size; ) {
				$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
				$source_id = $source_data[$source_start]["fund_code"];
				$target_id = $target_data[$target_start]["fund_code"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_TT_from_How_per_fund($sql, $symbol_list, $name_list, $insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin);

					echo "{$source_id}, {$source_data[$source_pos]["manager_name"]}, {$target_data[$target_pos]["fund_code"]}\n";
					$source_start = $source_end + 1;
					$target_start = $target_end + 1;
					break;
				} else if ($source_id < $target_id) {
					// target miss
					$target_miss ++;
					$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);

					$source_start = $source_end + 1;
					break;
				} else {
					// source miss
					$source_miss ++;
					$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
					$target_start = $target_end + 1;
				}
			}
			if ($target_start >= $target_size) {
				break;
			}
		}

		// do remain
		if ($insert_count > 0) {
			echo "do sql\n";
			$sql[0] = $sql[0]." END";
			$sql[1] = $sql[1]." END";
			$sql[2] = $sql[2]." END";
			$sql[3] = $sql[3]." END";
			$sql[4] = $sql[4]." END";
			//$sql[5] = $sql[5]." END";
			//$sql[6] = $sql[6]." END";
			$final_sql = "update ignore fund_manager_funds set ";
			$final_sql = $final_sql.$sql[0];
			$final_sql = $final_sql.$sql[1];
			$final_sql = $final_sql.$sql[2];
			$final_sql = $final_sql.$sql[3];
			$final_sql = $final_sql.$sql[4];
			//$final_sql = $final_sql.$sql[5];
			//$final_sql = $final_sql.$sql[6];
			$final_sql = $final_sql." WHERE fund_code in ({$symbol_list}) and manager_name in ({$name_list})";

			var_dump($final_sql);
			$ret = DB::runSql($final_sql);
			$sql = "";
			$name_list = "";
			$symbol_list = "";
			$insert_count = 0;
		}

		echo "source missing : ";
		echo "{$source_miss} \n";
		echo "target missing: ";
		echo "{$target_miss} \n";
	}

	function syn_TT_from_How_per_fund(&$sql, &$symbol_list, &$name_list, &$insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin) {
		$source_pos = $source_start;
		$target_pos = $target_start;
		for (; $source_pos < $source_end + 1; $source_pos ++) {
			for ($target_pos = $target_start; $target_pos < $target_end + 1; $target_pos ++) {
				$source_id = $source_data[$source_pos]["manager_name"];
				$target_id = $target_data[$target_pos]["manager_name"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_TT_from_How_per_item($sql, $symbol_list, $name_list, $insert_count, $source_data[$source_pos], $margin);

					echo "{$source_id}, {$source_data[$source_pos]["fund_code"]}, {$target_data[$target_pos]["manager_name"]}\n";
					break;
				}
			}

			if ($target_pos == $target_end + 1) {
				// insert source
				// only do once; not improve
				$tmp_item = $source_data[$source_pos];
				$tmp_sql = "INSERT INTO `fund_manager_funds` (`fund_code`, `fund_name`, `manager_code`, `manager_name`, `fund_type`, `start_date`, `end_date`,`payback`, `average`, `rank`, `avatar`) VALUES ('{$tmp_item["fund_code"]}', '{$tmp_item["fund_name"]}', '{$tmp_item["manager_code"]}', '{$tmp_item["manager_name"]}', '{$tmp_item["fund_type"]}', '{$tmp_item["start_date"]}', '{$tmp_item["end_date"]}', '{$tmp_item["payback"]}', '{$tmp_item["average"]}', '{$tmp_item["rank"]}', '--') ON DUPLICATE KEY UPDATE payback=VALUES(payback), average=VALUES(average), rank=VALUES(rank), start_date=VALUES(start_date), end_date=VALUES(end_date);";
				echo "do tmp_sql:\n";
				var_dump($tmp_sql);
				DB::runSql($tmp_sql);
			}


		}
	}


	function syn_TT_from_How_per_item(&$sql, &$symbol_list, &$name_list, &$insert_count, $item, $margin) {
		if ($insert_count != 0) {
			$name_list = $name_list.",";
			$symbol_list = $symbol_list.",";
		} else {
			//$sql = "update ignore test_fund_manager_funds_TT set manager_code = case ";
			$sql[0] = " fund_size = case ";
			$sql[1] = ", maxlos = case ";
			$sql[2] = ", average = case ";
			$sql[3] = ", rank = case ";
			$sql[4] = ", payback = case ";
			//$sql[5] = ", start_date = case ";
			//$sql[6] = ", end_date = case ";
		}

		// fund size
		$sql[0] = $sql[0]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["fund_size"]}'";

		// maxlos
		$sql[1] = $sql[1]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["maxlos"]}'";

		// average
		$sql[2] = $sql[2]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["average"]}'";

		// rank
		$sql[3] = $sql[3]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["rank"]}'";

		// payback
		$sql[4] = $sql[4]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["payback"]}'";

		// start_date
		//$sql[5] = $sql[5]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["start_date"]}'";

		// end_date
		//$sql[6] = $sql[6]." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["manager_name"]}' THEN '{$item["end_date"]}'";

		$symbol_list = $symbol_list." '{$item["fund_code"]}'";
		$name_list = $name_list." '{$item["manager_name"]}'";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql[0] = $sql[0]." END";
			$sql[1] = $sql[1]." END";
			$sql[2] = $sql[2]." END";
			$sql[3] = $sql[3]." END";
			$sql[4] = $sql[4]." END";
			//$sql[5] = $sql[5]." END";
			//$sql[6] = $sql[6]." END";
			$final_sql = "update ignore fund_manager_funds set ";
			$final_sql = $final_sql.$sql[0];
			$final_sql = $final_sql.$sql[1];
			$final_sql = $final_sql.$sql[2];
			$final_sql = $final_sql.$sql[3];
			$final_sql = $final_sql.$sql[4];
			//$final_sql = $final_sql.$sql[5];
			//$final_sql = $final_sql.$sql[6];
			$final_sql = $final_sql." WHERE fund_code in ({$symbol_list}) and manager_name in ({$name_list})";

			var_dump($final_sql);
			$ret = DB::runSql($final_sql);
			$sql = "";
			$name_list = "";
			$symbol_list = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	// ----------------------------- fund_manager_funds end ----------------------------------- //


	// ------------------------------ fund_manager start ------------------------------------ //
	// construct manager id map between howmany and tian tian
	function construct_manager_id_map () {
		DB::runSql("TRUNCATE table test_fund_manager_id_map");
		$sql = "select manager_code, fund_code, manager_name, avatar from fund_manager_funds order by fund_code";
		$source_data = DB::getData($sql);
		$source_size = count($source_data);


		$sql = "select fund_code, manager_name, manager_code from test_fund_manager_funds order by fund_code";
		$target_data = DB::getData($sql);
		$target_size = count($target_data);

		// check for match, then update
		$source_pos = 0;
		$target_pos = 0;
		$insert_count = 0;
		$sql = "";
		$margin = 50;
		$source_miss = 0;
		$target_miss = 0;
		$source_start = 0;
		$source_end = -1;
		$target_start = 0;
		$target_end = -1;
		$sql = "";
		$symbol_list = [];
		$name_list = "";
		for (; $source_start < $source_size; ) {
			$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);
			for (; $target_start < $target_size; ) {
				$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
				$source_id = $source_data[$source_start]["fund_code"];
				$target_id = $target_data[$target_start]["fund_code"];

				if ($target_id == $source_id) {
					// do it
					$this->construct_manager_id_map_per_fund($sql, $symbol_list, $name_list, $insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin);

					echo "{$source_id}, {$source_data[$source_pos]["manager_name"]}, {$target_data[$target_pos]["fund_code"]}\n";
					$source_start = $source_end + 1;
					$target_start = $target_end + 1;
					break;
				} else if ($source_id < $target_id) {
					// target miss
					$target_miss ++;
					$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);

					$source_start = $source_end + 1;
					break;
				} else {
					// source miss
					$source_miss ++;
					$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
					$target_start = $target_end + 1;
				}
			}
			if ($target_start >= $target_size) {
				break;
			}
		}

		// do remain
		if ($insert_count > 0) {
			echo "do sql\n";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		}

		echo "source missing : ";
		echo "{$source_miss} \n";
		echo "target missing: ";
		echo "{$target_miss} \n";

	}

	function construct_manager_id_map_per_fund(&$sql, &$symbol_list, &$name_list, &$insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin) {
		$source_pos = $source_start;
		$target_pos = $target_start;
		for (; $source_pos < $source_end + 1; $source_pos ++) {
			for ($target_pos = $target_start; $target_pos < $target_end + 1; $target_pos ++) {
				$source_id = $source_data[$source_pos]["manager_name"];
				$target_id = $target_data[$target_pos]["manager_name"];

				if ($target_id == $source_id) {
					// do it
					$this->construct_manager_id_map_per_item($sql, $symbol_list, $name_list, $insert_count, $source_data[$source_pos], $target_data[$target_pos], $margin);

					echo "{$source_id}, {$source_data[$source_pos]["manager_code"]}, {$target_data[$target_pos]["manager_name"]}\n";
					break;
				}
			}
		}
	}

	function construct_manager_id_map_per_item(&$sql, &$symbol_list, &$name_list, &$insert_count, $source_item, $target_item, $margin) {
		var_dump($symbol_list);
		if (in_array($source_item["manager_code"], $symbol_list)) {
			return;
		}

		if ($insert_count != 0) {
			$sql = $sql.", ";
		} else {
			$sql = "INSERT INTO `test_fund_manager_id_map` (`tt_manager_id`, `avatar`, `how_manager_id`)  values  ";
		}

		$sql = $sql." ('{$source_item["manager_code"]}', '{$source_item["avatar"]}', '{$target_item["manager_code"]}')";
		array_push($symbol_list, $source_item["manager_code"]);

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	/**
	 * syn fund manager from howbuy
	 * the manager info grasped from howbuy
	 * but the manager code is follow the tian tian ji jin
	 */
	function syn_fund_manager() {

		//DB::runSql("TRUNCATE table test_fund_manager");
		// generate manager info url
		$url="http://www.howbuy.com/fund/manager/";
		$html_data = file_get_contents($url);

		// get table
		$url_list = [];
		$ret = preg_match_all('/<table([\s\S\w\W\d\D]*?)<\/table>/', $html_data, $match_tables);
		$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $match_tables[0][0], $arrays);
		$this->construct_manager_url($url_list, $arrays[0]);

		$ret = preg_match_all('/<textarea([\s\S\w\W\d\D]*?)<\/textarea>/', $html_data, $match_tables);
		foreach($match_tables[0] as $items) {
			//echo "wangyu\n";
			//var_dump($items);
			$ret = preg_match_all('/<tr([\s\S\w\W\d\D]*?)<\/tr>/', $items, $arrays);
			$this->construct_manager_url($url_list, $arrays[0]);
		}
		//var_dump($url_list);
		$sz = count($url_list);
		echo "url list size: {$sz}";
		echo "wangyu split --\n";

		// get manager info from url and store it into table
		$insert_count = 0;
		$margin = 50;
		$sql = "";
		// update url id
		$sql = "SELECT `tt_manager_id`, `avatar`, `how_manager_id` from test_fund_manager_id_map";
		$url_map_list = DB::getData($sql);
		//var_dump($url_map_list);
		$sz = count($url_map_list);
		echo "url list map size: {$sz}";
		$url_list_sz = count($url_list);
		echo "url list size: {$url_list_sz}";
		for ($pos=0; $pos < $url_list_sz; $pos++) {
			$url_list[$pos][3] = "--";
		}
		for ($pos=0; $pos < $url_list_sz; $pos++) {
			$url = $url_list[$pos];
			foreach ($url_map_list as $url_map) {
				if ($url[1] == $url_map["how_manager_id"]) {
					$url_list[$pos][1] = $url_map["tt_manager_id"];
					$url_list[$pos][3] = $url_map["avatar"];
					echo $url[1];
					echo " hit\n";
					break;
				}
			}
		}

		var_dump($url_list);
		foreach ($url_list as $url) {
			$this->execute_syn_fund_manager($url[0], $url[1], $url[2], $url[3], $sql, $insert_count, $margin);
		}
		if ($insert_count > 0) {
			echo "do sql:\n";
			$sql = $sql." ON DUPLICATE KEY UPDATE avatar=VALUES(avatar), work_start=VALUES(work_start), company_code=VALUES(company_code), rate=VALUES(rate), major=VALUES(major), intro=VALUES(intro)";
			var_dump($sql);
			$ret=DB::runSql($sql);
		}
		// end
	}

	function execute_syn_fund_manager($url, $manager_id, $manager_name, $avatar, &$sql, &$insert_count, $margin) {
		// get information
		$html_data = file_get_contents($url);
		$info = [];
		$flag = $this->parse_one_manager_table($html_data, $info);
		if ($flag == false) {
			return;
		}
		if ($manager_name == "曾宇") {
			return;
		}
		//echo "info list\n";
		//var_dump($info_list);

		if ($insert_count != 0) {
			$sql = $sql.', ';
		} else {
			$sql = "INSERT INTO `fund_manager` (`code`, `name`, `avatar`, `work_start`, `work_company`, `company_code`, `rate`, `major`, `intro`) values ";
		}

		$sql = $sql . "('{$manager_id}', '{$manager_name}', '{$avatar}', '{$info["work_start"]}', '{$info["work_company"]}', '{$info["company_code"]}', '{$info["rate"]}', '{$info["major"]}', '{$info["intro"]}')";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql = $sql." ON DUPLICATE KEY UPDATE avatar=VALUES(avatar), work_start=VALUES(work_start), work_company=VALUES(work_company), company_code=VALUES(company_code), rate=VALUES(rate), major=VALUES(major), intro=VALUES(intro)";
			var_dump($sql);
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	function parse_one_manager_table($html_data, &$info) {
		// img, avatar
		$info["avatar"] = "--";
		$ret = preg_match_all('/manager_info_left([\s\S\w\W\d\D]*?)<\/div>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/src="([\s\S\w\W\d\D]*?)"/', $cur_data[0][0], $cur_data);
			if ($ret > 0) {
				$tmpS = $cur_data[0][0];
				$len = strlen($tmpS);
				$info["avatar"] = substr($tmpS, 5, $len - 6);
			}
		}

		// rate
		$info["rate"] = "--";
		$ret = preg_match_all('/好买综合评分([\s\S\w\W\d\D]*?)<\/span>/', $html_data, $cur_data);
		if ($ret > 0) {
			$tmpS = $cur_data[0][0];
			$firstP = stripos($tmpS, "score\">");
			$len = strlen($tmpS);
			$tmpS = substr($tmpS, $firstP+7, $len - $firstP - 7);
			$firstP = stripos($tmpS, "<span>");
			if ($firstP != false) {
				$len = strlen($tmpS);
				$tmpS_1 = substr($tmpS, 0, $firstP);
				$tmpS_2 = substr($tmpS, $firstP + 6, $len - $firstP - 6 - 7);
				$tmpS = trim($tmpS_1.$tmpS_2, "<span>");
			}
			$info["rate"] = trim($tmpS);
		}

		// company
		$info["work_company"] = "--";
		$info["company_code"] = "--";
		$ret = preg_match_all('/当前所在公司([\s\S\w\W\d\D]*?)<\/li>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/target=\'_blank\'>([\s\S\w\W\d\D]*?)</', $cur_data[0][0], $tmp_data);
			if ($ret > 0) {
				$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $tmp_data[0][0], $tmp_data);
				if ($ret > 0) {
					$tmpS = $tmp_data[0][0];
					$len = strlen($tmpS);
					$info["work_company"] = substr($tmpS, 1, $len - 2);
				}
			}

			$ret = preg_match_all('/href="([\s\S\w\W\d\D]*?)"/', $cur_data[0][0], $cur_data);
			if ($ret > 0) {
				$tmpS = $cur_data[0][0];
				$len = strlen($tmpS);
				$tmpS = substr($tmpS, 0, $len-2);
				$firstP = strrpos($tmpS, "/");
				$info["company_code"] = substr($tmpS, $firstP + 1, $len - 1 - $firstP);
			}
		}

		// major
		$info["major"] = "--";
		$ret = preg_match_all('/最擅长的基金类型([\s\S\w\W\d\D]*?)<\/li>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $cur_data[0][0], $cur_data);
			if ($ret > 0) {
				$tmpS = $cur_data[0][0];
				$len = strlen($tmpS);
				$info["major"] = substr($tmpS, 1, $len-2);
			}
		}

		// work_start
		$info["work_start"] = "--";
		$ret = preg_match_all('/首次任职时间<\/td>([\s\S\w\W\d\D]*?)<\/td>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/<td([\s\S\w\W\d\D]*?)<\/td>/', $cur_data[0][0], $cur_data);
			if ($ret > 0) {
				$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $cur_data[0][0], $cur_data);
				if ($ret > 0) {
					$tmpS = $cur_data[0][0];
					$len = strlen($tmpS);
					$tmpS = substr($tmpS, 1, $len-2);
					$info["work_start"] = $this->getDateTime($tmpS);
				}
			}
		}

		// intro
		$info["intro"] = "--";
		$ret = preg_match_all('/"des_con">([\s\S\w\W\d\D]*?)<\/div>/', $html_data, $cur_data);
		if ($ret > 0) {
			$ret = preg_match_all('/>([\s\S\w\W\d\D]*?)</', $cur_data[0][0], $cur_data);
			if ($ret > 0) {
				$tmpS = $cur_data[0][0];
				$len = strlen($tmpS);
				$info["intro"] = str_replace("'", "''", trim(substr($tmpS, 1, $len-2)));
			}
		}
		return true;
	}

	// ------------------------------ fund_manager end ------------------------------------ //

	// ------------------ build the table for relative bonus and split --------------------//
	// 涉及到逻辑计算，暂时不适合模块化优化
	// used for update fund_value change ratio
	// it will update all the elements
	/**
	 * CREATE TABLE `test_fund_relative_split_and_divide` (
	 * `id` int(11) NOT NULL AUTO_INCREMENT
	 * `code` varchar(16) DEFAULT NULL,
	 * `relative_split_ratio` float(16,6) DEFAULT NULL,
	 * )
	 * takes about 3 hours for construct_relative_bonus_split_per_fund 
	 * for every fund
	 */
	function construct_relative_bonus_split() {
		//DB::runSql("TRUNCATE table test_fund_manager_bonus_split");
		$this->construct__relative_bonus_split_skeleton();
		//$this->update_relative_bonus_split_skeleton();

		$sql = "select DISTINCT(code) from test_fund_manager_bonus_split";
		$code_list = DB::getData($sql);
		foreach ($code_list as $code) {
			var_dump($code);
			$this->construct_relative_bonus_split_per_fund($code["code"]);
		}

	}

	function construct_relative_bonus_split_per_fund($code) {
		// construct split
		$sql = "SELECT date_cal, split_percent FROM fund_split WHERE code = {$code} ORDER BY date_cal";
		var_dump($sql);
		$split_data = DB::getData($sql);
		$sz = count($split_data);

		$sql = "SELECT per, date_divid FROM fund_bonus WHERE code = {$code}  ORDER BY date_divid";
		var_dump($sql);
		$bonus_data = DB::getData($sql);

		if ($sz > 0) {
			$pre_bonus = 0;
			$date1 = $split_data[0]["date_cal"];
			$cur_split = 1;
			$sql = "update test_fund_manager_bonus_split set split_percent = {$cur_split} WHERE code = {$code} and create_date < '{$date1}'";
			var_dump($sql);
			DB::runSql($sql);
			$this->construct_relative_bonus($bonus_data, $code, -1, $date1, $pre_bonus, $cur_split);

			for ($pos = 0; $pos < $sz-1; $pos ++) {
				$date1 = $split_data[$pos]["date_cal"];
				$date2 = $split_data[$pos+1]["date_cal"];
				$cur_split = $cur_split * $split_data[$pos]["split_percent"];
				$sql = "update test_fund_manager_bonus_split set split_percent = {$cur_split} WHERE code = {$code} and create_date >= '{$date1}' and create_date < '{$date2}'";
				var_dump($sql);
				DB::runSql($sql);
				$pre_bonus = $pre_bonus * $split_data[$pos]["split_percent"];
				$this->construct_relative_bonus($bonus_data, $code, $date1, $date2, $pre_bonus, $cur_split);
			}
			$date2 = $split_data[$sz-1]["date_cal"];
			$cur_split = $cur_split * $split_data[$sz-1]["split_percent"];
			$sql = "update test_fund_manager_bonus_split set split_percent = {$cur_split} WHERE code = {$code} and create_date >= '{$date2}'";
			var_dump($sql);
			DB::runSql($sql);
			$pre_bonus = $pre_bonus * $split_data[$pos]["split_percent"];
			$this->construct_relative_bonus($bonus_data, $code, $date2, -1, $pre_bonus, $cur_split);
		} else {
			$sql = "update test_fund_manager_bonus_split set split_percent = 1 WHERE code = {$code}";
			var_dump($sql);
			DB::runSql($sql);

			$pre_bonus = 0;
			$this->construct_relative_bonus($bonus_data, $code, -1, -1, $pre_bonus, 1);
		}

	}

	/**
	 * pre_bonus: already mulitied by current ratio
	 */
	function construct_relative_bonus($data, $code, $start_date, $end_date, &$pre_bonus, $ratio, &$sql = "", &$insert_count = 0, $margin = 1) {
		// get_first_data and last_data
		echo "---------------\n";
		var_dump($data);
		var_dump($code);
		var_dump($start_date);
		var_dump($end_date);
		var_dump($pre_bonus);
		var_dump($ratio);
		var_dump($sql);
		var_dump($insert_count);
		var_dump($margin);
		echo "---------------\n\n";
		$sz = count($data);
		if ($sz == 0) {
			//echo "construct_relative_bonus: sz = 0\n";
			$first_pos = -1;
			$end_pos = -1;
		} else {
			if ($start_date == -1) {
				$first_pos = 0;
				$first_flag = true;
			} else {
				$first_flag = false;
				for ($pos=0; $pos<$sz; $pos++) {
					if ( $start_date <= $data[$pos]["date_divid"] ) {
						break;
					}
				}
				if ($pos == $sz) {
					$pos = -1;
				}
				$first_pos = $pos;
			}

			if ($end_date = -1) {
				$end_pos = $sz-1;
				$end_flag = true;
			} else {
				$end_flag = false;
				for ($pos=0; $pos<$sz; $pos++) {
					if ( $start_date >= $data[$pos]["date_divid"] ) {
						break;
					}
				}
				$pos --;
				$end_pos = $pos;
			}
		}

		var_dump($data);
		var_dump($start_date);
		var_dump($end_date);
		var_dump($first_pos);
		var_dump($end_pos);

		if ($first_pos == -1 && $end_pos == -1) {
			$sql = $sql."update test_fund_manager_bonus_split set per = {$pre_bonus}  WHERE code = {$code};";
			$this->run_sql($sql, $insert_count, $margin);
			return;
		}
		if ($first_pos == -1 || $end_pos == -1) {
			$bonus = $pre_bonus;
			$sql = $sql."update test_fund_manager_bonus_split set per = {$bonus}  WHERE code = {$code} and ";
			if ($start_date != -1) {
				$sql = $sql."create_date >= '{$start_date}' ";
				if ($end_date != -1) {
					$sql = $sql." and create_date < '{$end_date}';";
				} else {
					$sql = $sql.";";
				}
			} else {
				$sql = $sql."create_date < '{$end_date}';";
			}
			//var_dump($sql);
			//$ret = DB::runSql($sql);
			$this->run_sql($sql, $insert_count, $margin);
			return;
		}

		// update first
		$bonus = $pre_bonus;
		$date1 = $data[$first_pos]["date_divid"];
		$sql = $sql."update test_fund_manager_bonus_split set per = {$bonus}  WHERE code = {$code} AND create_date < '{$date1}'";
		if ($start_date != -1) {
			$sql = $sql." and create_date >= '{$start_date}'";
		}
		$sql = $sql.";";
		//var_dump($sql);
		//$ret = DB::runSql($sql);

		// update middle
		for ($pos = $first_pos; $pos <= $end_pos - 1; $pos++) {
			$bonus = $bonus + $ratio * $data[$pos]["per"];
			$date1 = $data[$pos]["date_divid"];
			$date2 = $data[$pos+1]["date_divid"];
			$sql = $sql."update test_fund_manager_bonus_split set per = {$bonus}  WHERE code = {$code} AND create_date >= '{$date1}' and create_date < '{$date2}';";
			//var_dump($sql);
			//$ret = DB::runSql($sql);
		}

		// update last
		$date2 = $data[$end_pos]["date_divid"];
		$bonus = $bonus + $ratio * $data[$end_pos]["per"];
		$sql = $sql."update test_fund_manager_bonus_split set per = {$bonus}  WHERE code = {$code} AND  create_date >= '{$date2}'";
		if ($end_date != -1) {
			$sql = $sql." AND create_date < {$end_date}";
		}
		$sql.";";
		//var_dump($sql);
		//$ret = DB::runSql($sql);
		$this->run_sql($sql, $insert_count, $margin);

		$pre_bonus = $bonus;
	}

	function construct__relative_bonus_split_skeleton() {
		ini_set('memory_limit','512M');
		$start_code = 0;
		$margin = 1000;
		$end_code = $margin + $start_code;

		while (intval($start_code) < 1000000) {
			$start_s = strval($start_code);
			$end_s = strval($end_code);
			while (strlen($start_s) < 6) {
				$start_s = "0".$start_s;
			}
			while (strlen($end_s) < 6) {
				$end_s = "0".$end_s;
			}
			$sql = "SELECT code, create_date FROM fund_value WHERE code >= {$start_s} and code < {$end_s}";
			//var_dump($sql);
			$data = DB::getData($sql);
			self::construct__relative_bonus_split_skeleton_partial($data);
			$start_code = $start_code + $margin;
			$end_code = $end_code + $margin;

		}
	}

	function construct__relative_bonus_split_skeleton_partial($data) {
		$sql = "";
		$margin = 200;
		$insert_count = 0;
		foreach ($data as $item) {
			$this->construct__relative_bonus_split_skeleton_sql($sql, $item, $insert_count, $margin);
		}

		if ($insert_count > 0) {
			//echo "do sql\n";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		}
	}

	function construct__relative_bonus_split_skeleton_sql(&$sql, $item, &$insert_count, $margin) {
		if ($insert_count != 0) {
			$sql = $sql.", ";
		} else {
			$sql = "INSERT IGNORE INTO `test_fund_manager_bonus_split` (`code`, `create_date`)  values  ";
		}

		$sql = $sql." ('{$item["code"]}', '{$item["create_date"]}')";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			//echo "do sql\n";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	function update_relative_bonus_split_skeleton() {
		// update
		$sql = "select DISTINCT(code) from fund_value";
		$code_list = DB::getData($sql);
		$margin = 500;
		$sql = "";
		$insert_count = 0;
		foreach ($code_list as $code) {
			var_dump($code);
			$this->update_relative_bonus_split_skeleton_per_fund($code["code"], $sql, $insert_count, $margin);
		}
		if ($insert_count > 0) {
			$ret = DB::runSql($sql);
		}
	}

	function update_relative_bonus_split_skeleton_per_fund($symbol, &$sql, &$insert_count, $margin) {
		$tmp_sql = "SELECT create_date from fund_value where code = '{$symbol}' order by `create_date` DESC LIMIT 10";
		$data = DB::getData($tmp_sql);
		$sz = count($data);
		if ($sz == 0) {
			return;
		}
		$sql = $sql."INSERT IGNORE INTO `test_fund_manager_bonus_split` (`code`, `create_date`) VALUES ('{$symbol}', '{$data[0]["create_date"]}')";
		for ($pos = 1; $pos < $sz; $pos++) {
			$sql = $sql . ", ";
			$sql = $sql." ('{$symbol}', '{$data[$pos]["create_date"]}')";
		}
		$sql = $sql.";";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			// do sql
			//var_dump($sql);
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count++;
		}

	}


	/**
	 * update table test_relative_bonus_split
	 * version2: speed up
	 * assumption: less than 10days for not update, no two splits or bonus in 10 days\
	 * update_relative_bonus_split_skeleton: 80s, 
	 * construct_relative_bonus_split_single_fund_array: 10s
	 * update_relative_bonus_split_per_fund: 40s
	 * improvement: combine mutiple sql into on query
	 *
	 *
	 */
	function update_relative_bonus_split () {
		// construct addition data in table
		$date1 = date('y-m-d h:i:s',time());
		$this->update_relative_bonus_split_skeleton();
		$date2 = date('y-m-d h:i:s',time());
		echo "update_relative_bonus_split_skeleton, start: {$date1}; end: {$date2}\n";

		// construct array for symbols need update
		$date1 = date('y-m-d h:i:s',time());
		$fund_info_array = [];
		$sql = "select DISTINCT(code) from test_fund_manager_bonus_split";
		$code_list = DB::getData($sql);
		foreach ($code_list as $code) {
			//var_dump($code);
			$this->construct_relative_bonus_split_single_fund_array($fund_info_array, $code["code"]);
		}
		$date2 = date('y-m-d h:i:s',time());
		echo "construct_relative_bonus_split_single_fund_array, start: {$date1}; end: {$date2}\n";

		$date1 = date('y-m-d h:i:s',time());
		var_dump($fund_info_array);
		$this->update_relative_bonus_split_from_fund_info_array($fund_info_array);
		$date2 = date('y-m-d h:i:s',time());
		echo "update_relative_bonus_split_per_fund, start: {$date1}; end: {$date2}\n";
	}

	function construct_relative_bonus_split_single_fund_array(&$target_array, $code) {
		$sql = "SELECT * FROM test_fund_manager_bonus_split WHERE code = '{$code}' ORDER BY create_date DESC LIMIT 10";
		$data = DB::getData($sql);
		$tmp_array = [];
		$date_array = [];
		foreach ($data as $item) {
			if ($item["per"] != null) {
				//var_dump($item);
				$tmp_array["code"] = $code;
				$tmp_array["per"] = $item["per"];
				$tmp_array["split_percent"] = $item["split_percent"];
				$date_array = array_reverse($date_array);
				$tmp_array["create_date"] = $date_array;
				array_push($target_array, $tmp_array);
				break;
			} else {
				array_push($date_array, $item["create_date"]);
			}
		}
	}

	function update_relative_bonus_split_from_fund_info_array($info_arraay) {
		$insert_count = 0;
		$margin = 100;
		$sql = "";
		foreach ($info_arraay as $item) {
			$this->update_relative_bonus_split_per_fund($item, $sql, $insert_count, $margin);
		}
		if ($insert_count > 0) {
			$ret = DB::runSql($sql);
		}
	}

	function update_relative_bonus_split_per_fund($fund_info, &$target_sql, &$insert_count, $margin) {
		$code = $fund_info["code"];
		$data_array = $fund_info["create_date"];
		if ($data_array == null || count($data_array) == 0) {
			return;
		}
		$cur_date = $data_array[0];

		// construct split
		$sql = "SELECT date_cal, split_percent FROM fund_split WHERE code = '{$code}' ORDER BY date_cal desc";
		//var_dump($sql);
		$split_data = DB::getData($sql);
		$split_sz = count($split_data);

		$sql = "SELECT per, date_divid FROM fund_bonus WHERE code = '{$code}' ORDER BY date_divid";
		//var_dump($sql);
		$bonus_data = DB::getData($sql);
		$bonus_sz = DB::getData($sql);


		/*
		var_dump($fund_info);
		var_dump($data_array);
		var_dump($cur_date);
		var_dump($split_data);
		var_dump($split_sz);
		echo "\n";
		*/

		$pre_bonus = $fund_info["per"];
		$cur_split = $fund_info["split_percent"];
		$split_sql = "";
		if ($split_sz > 0 && $split_data[$split_sz-1]["date_cal"] >= $cur_date) {
			$item = $split_data[$split_sz-1];
			$split_sql = $split_sql."update test_fund_manager_bonus_split set split_percent = {$cur_split} where code = '{$code}' and create_date >= '{$cur_date}' and create_date < '{$item["date_cal"]}';";
			$target_sql = $target_sql.$split_sql;
			$this->construct_relative_bonus($bonus_data, $code, $cur_date, $item["date_cal"], $pre_bonus, $cur_split, $target_sql, $insert_count, $margin);
			$cur_split = $cur_split = $cur_split * $item["split_percent"];
			$split_sql = "update test_fund_manager_bonus_split set split_percent = {$cur_split} where code = '{$code}' and create_date >= '{$item["date_cal"]}';";
			$target_sql = $target_sql.$split_sql;
			$this->construct_relative_bonus($bonus_data, $code, $item["date_cal"], -1, $pre_bonus, $cur_split, $target_sql, $insert_count, $margin);
		} else {
			$split_sql = $split_sql."update test_fund_manager_bonus_split set split_percent = {$cur_split} where code = '{$code}' and create_date >= '{$cur_date}';";
			$target_sql = $target_sql.$split_sql;
			$this->construct_relative_bonus($bonus_data, $code, $cur_date, -1, $pre_bonus, $cur_split, $target_sql, $insert_count, $margin);
		}
	}

	// ----------------------------------- fund_split_bonus end --------------------------------------//

	// ----------------------------------- update fund_bond  -----------------------------------------//
	function syn_fund_bond () {
		$sql = "exec  [dbo].constructMysqlFundBond";
		$ret = self::runRootSql($sql);
		$sql = "TRUNCATE TABLE fund_bond";
		//$ret = DB::runSql($sql);

		$mysql_tbname = "fund_bond";
		$mysql_fields = ["fund_code", "bond_code", "bond_name", "percent", "mount", "market_value", "amortized_cost", "rank", "start_date", "end_date"];
		$mysql_id = "fund_code";

		$mssql_tbname = "mysql_fund_bond";
		$mssql_fields = ["fund_code", "SYMBOL", "FULLNAME", "PROPORTION", "SHARES", "MARKETVALUE", "AMORITIZEDCOST", "RANK", "STARTDATE", "ENDDATE"];
		$mssql_id = "FUNDCODE";

		// new a table in mssql for fund_bond whith fund_code
		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id, $mssql_id);


	}

	// ----------------------------------- update fund_bond end --------------------------------------//

	// ----------------------------------- update fund_rate ------------------------------------------//
	/**
	 * update fund rating for each fund from tian tian ji jin
	 */
	function syn_fund_rating_data() {
		$target_url_list = [];
		$this->generate_fund_rating_url($target_url_list);
		var_dump($target_url_list);

		foreach($target_url_list as $url) {
			$this->syn_fund_rating_data_url($url[0], $url[1], $url[2]);
		}

	}

	function generate_fund_rating_url(&$target_url_list) {
		$url_list = [["fundrating_1", "海通证券"], ["fundrating_2", "招商证券"], ["fundrating_3", "上海证券"], ["fundrating_4", "济安证券"]];
		foreach ($url_list as $url) {
			$target_url = "http://fund.eastmoney.com/data/".$url[0].".html";
			$this->generate_fund_rating_url_per_company($target_url_list, $target_url, $url[1]);
		}

	}

	function generate_fund_rating_url_per_company(&$url_list, $url, $company) {
		$html_data = file_get_contents($url);
		$ret = preg_match_all('/<span>评级日期([\s\S\w\W\d\D]*?)<\/select>/', $html_data, $data);
		$ret = preg_match_all('/<option([\s\S\w\W\d\D]*?)<\/option>/', $data[0][0], $data);
		$date_list = $data[0];
		$url = substr($url, 0, strlen($url) - 5);
		foreach ($date_list as $data) {
			preg_match_all('/>([\s\S\w\W\d\D]*?)</', $data, $tmpS);
			$tmpS = substr($tmpS[0][0], 1, strlen($tmpS[0][0]) - 2);
			$tmp_url = $url."_".$tmpS.".html";
			$tmpV = [];
			array_push($tmpV, $tmp_url);
			array_push($tmpV, $tmpS);
			array_push($tmpV, $company);
			array_push($url_list, $tmpV);

		}
	}

	function syn_fund_rating_data_url($url, $date, $company) {
		echo "syn_fund_rating_data_url: {$url}, {$date}, {$company}\n";
		
		$html = file_get_contents($url);
		$ret = preg_match_all('/fundinfos = "([\s\S\w\W\d\D]*?)"/', $html, $data);
		if ($ret == false) {
			return;
		}
		var_dump($data);
		$sz = strlen($data[0][0]);
		$res_string = substr($data[0][0], 13, $sz - 1 - 13);
		$parts = explode('|', $res_string);
		$sz = count($parts);
		$start_pos = 0;
		$margin = 50;

		$insert_count = 0;
		$margin = 50;
		$sql = "";
		while ($start_pos + 7 < $sz) {
			echo "$start_pos: {$start_pos}\n";
			$this->syn_fund_rating_data_per_fund($parts, $insert_count, $sql, $start_pos, $margin, $date, $company);
			$start_pos = $start_pos + 20;
		}

		if ($insert_count > 0) {
			echo "do sql:\n";
			$sql = $sql." ON DUPLICATE KEY UPDATE star=VALUES(star),star_change=VALUES(star_change)";
			$ret=DB::runSql($sql);
		}

	}

	function syn_fund_rating_data_per_fund(&$data, &$insert_count, &$sql, $start_pos, $margin, $date, $company) {

		if ($insert_count == 0) {
			$sql = "INSERT INTO `fund_star_rate` (`code`, `fund_name`, `fund_type`, `securities_company`, `star`, `star_change`, `rate_date`) VALUES ";
		} else {
			$sql = $sql.",";
		}

		$fund_code = $data[$start_pos];
		if (!empty($fund_code) && substr($fund_code, 0, 1) == "_" ) {
			$fund_code = substr($fund_code, 1, strlen($fund_code) - 1);
		}
		$fund_name = $data[$start_pos + 1];
		$fund_type = $data[$start_pos + 2];
		$fund_star = $data[$start_pos + 7];
		$fund_star_change = $data[$start_pos + 8];
		$sql = $sql."('{$fund_code}', '{$fund_name}', '{$fund_type}', '{$company}', '{$fund_star}', '{$fund_star_change}', '{$date}')";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			$sql = $sql." ON DUPLICATE KEY UPDATE star=VALUES(star),star_change=VALUES(star_change)";
			echo "do sql:\n";
			var_dump($sql);
			$ret=DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}

	// ----------------------------------- update fund_rate end --------------------------------------//

	// ----------------------------------- update fund stock and bond total percent ------------------//
	function update_fund_stock_bond_total_percent() {
		$sql="exec [dbo].graspFundStockBondPercentInfo";
		$ret = self::runRootSql($sql);
		if ($ret ===false) {
			return;
		}

		// stock
		$mysql_tbname = "fund_stock_bond_percent";
		$mysql_fields = ["fund_code", "stock_percent", "start_date", "end_date"];
		$mssql_tbname = "table_stock_percent";
		$mssql_fields = ["SYMBOL", "FAIRVALUETONAV", "STARTDATE", "ENDDATE"];
		$mysql_id_array = ["fund_code", "start_date", "end_date"];
		$mssql_id = "SYMBOL";
		$addtion_where = null;
		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array, $mssql_id, $addtion_where);

		// bond
		$mysql_tbname = "fund_stock_bond_percent";
		$mysql_fields = ["fund_code", "bond_percent", "start_date", "end_date"];
		$mssql_tbname = "table_bond_percent";
		$mssql_fields = ["SYMBOL", "FAIRVALUETONAV", "STARTDATE", "ENDDATE"];
		$mysql_id_array = ["fund_code", "start_date", "end_date"];
		$mssql_id = "SYMBOL";
		$addtion_where = null;
		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array, $mssql_id, $addtion_where);
		

		// total asset
		$mysql_tbname = "fund_stock_bond_percent";
		$mysql_fields = ["fund_code", "total_asset_percent", "start_date", "end_date"];
		$mssql_tbname = "table_total_asset_percent";
		$mssql_fields = ["SYMBOL", "TotalAsset", "STARTDATE", "ENDDATE"];
		$mysql_id_array = ["fund_code", "start_date", "end_date"];
		$mssql_id = "SYMBOL"; 
		$addtion_where = null;
		$this->syn_mssql_2_mysql($mysql_tbname, $mysql_fields, $mssql_tbname, $mssql_fields, $mysql_id_array, $mssql_id, $addtion_where);
		
	}


	// ----------------------------------- update fund stock and bond total percent end ---------------//

	// ----------------------------------- update fund notice ----------------------------------------//
	function syn_fund_notice() {
		// get symbol
		$sql = "select DISTINCT(code) from fund_info";
		$symbol_list = DB::getData($sql);

		foreach ($symbol_list as $item) {
			$this->syn_fund_notice_per_fund($item["code"]);
			break;
		}

	}

	function syn_fund_notice_per_fund($code) {
		$per = 1000;
		http://fund.eastmoney.com/f10/F10DataApi.aspx?type=jjgg&code=340006&page=1&per=1000&class=0&rt=0.09834829764440656
		for ($count = 1; $count < 20; $count++) {
			// construct url
			$url = "http://fund.eastmoney.com/f10/F10DataApi.aspx?type=jjgg&code="
					.$code."&page=".$count."&per=".$per."&rt=0.09834829764440656";
			$html_data = file_get_contents($url);
			var_dump($url);
			if ( preg_match_all('/<tbody>([\s\S]*?)<\/tbody>/',$html_data, $match_data) == 0) {
				break;
			}
			$body_data = $match_data[0][0];
			if (preg_match_all('/<tr>([\s\S]*?)<\/tr>/',$body_data, $match_data) == 0) {
				break;
			}
			$item_array = $match_data[1];
			$res_array = [];
			var_dump($item_array);
			foreach ($item_array as $item) {
				//var_dump($item);
				$ret = preg_match_all('/<td([\s\S]*?)<\/td>/',$item, $match_data);
				if ($ret == 0) {
					continue;
				}
				// title,url
				$tmp_array = [];
				if ( preg_match_all('/a href=\'([\s\S]*?)\'>/', $match_data[1][0], $tmp_match_data) != 0) {
					$tmp_array["url"] = trim($tmp_match_data[1][0]);
					// get content
					$content_data = file_get_contents($tmp_array["url"]);
					$pos = strpos($content_data, "<div id=\"jjggzwcontent\">");
					if ($pos === false) {

					} else {
						$end_pos = 0;
						$content_total_data = get_patten_string_right_pos($content_data, "<div id=\"jjggzwcontent\">", "</div>", $pos, $end_pos);
						if ($content_total_data === false) {

						} else {
							var_dump($content_total_data);
							$content = "";
							if ( preg_match_all('/<div id="jjggzwcontentt"><span>([\s\S]*?)<\/span>/', $content_total_data, $tmp_match_data) != 0) {
								$content = $content.trim($tmp_match_data[1][0])."\n";
							}
							if ( preg_match_all('/<pre>([\s\S]*?)<a class/', $content_total_data, $tmp_match_data) != 0) {
								$content = $content.trim($tmp_match_data[1][0]);
							}
							$tmp_array["content"] = $content;
						}
					}
				}

				if ( preg_match_all('/html\'>([\s\S]*?)<\/td>/',$match_data[0][0], $tmp_match_data) != 0) {
					$tmp_array["title"] = trim($tmp_match_data[1][0]);
				}

				if ($ret > 1) {
					$tmp_array["notice_type"] = trim($match_data[1][1], ">");
				}
				if ($ret > 2) {
					$tmp_array["create_date"] = trim($match_data[1][2], ">");
				}
				array_push($res_array, $tmp_array);
			}
			var_dump($res_array);
			
		}

	}

	// ----------------------------------- update fund notice end ------------------------------------//


	/*
 * takes about 12min
 */
	function update_fund_manager_funds() {
		// parse basic info from tian tian jijin
		$this->syn_fund_manager_funds_from_TT();

		// parse addition info from howbuy into another table
		$this->syn_fund_manager_funds();

		// combine both table
		$this->syn_fund_manager_funds_TT_from_How();
	}

	/*
	 * takes about 13min
	 */
	function update_fund_manager() {
		$this->update_fund_manager_funds();
		$this->construct_manager_id_map();
		$this->syn_fund_manager();
	}

	// ------------------------------------ not used -----------------------------------//
	/**
	 * update the fund_size field in table--fund_manager_funds
	 *
	 * row nums is about 5k, can be store in memory
	 */
	function syn_fund_manager_funds_fund_size() {
		$sql="exec [dbo].graspFundInfo";
		$ret = self::runRootSql($sql);
		self::log("ret: {$ret}");
		if ($ret >= 0) {
			// get  $source_data
			$sql = 'select SYMBOL, TOTALTNA from [dbo].[table_test_res] ORDER BY SYMBOL';
			$source_data = self::getData($sql);
			$source_size = count($source_data);

			// get  $get target code ids
			$sql = 'SELECT fund_code from test_fund_manager_funds ORDER BY fund_code';
			$target_id_aray = DB::getData($sql);
			$target_size = count($target_id_aray);

			// check for match, then update
			$source_pos = 0;
			$target_pos = 0;
			$insert_count = 0;
			$sql = "";
			$id_list = "";
			$margin = 50;
			$source_miss = 0;
			$target_miss = 0;
			$pre_source_id = -1;
			$pre_target_id = -1;
			for (; $source_pos < $source_size; ) {
				for (; $target_pos < $target_size; ) {
					$source_id = $source_data[$source_pos]["SYMBOL"];
					$target_id = $target_id_aray[$target_pos]["fund_code"];

					if ($target_id == $source_id) {
						// do it
						self::execute_syn_fund_manager_funds_fund_size(
							$sql, $id_list, $source_data[$source_pos], $insert_count, $margin
						);
						//echo "{$source_id}, {$source_data[$source_pos]["TOTALTNA"]}, {$target_id_aray[$target_pos]["fund_code"]}\n";
						$pre_source_id = $source_id;
						$source_pos ++;
						$pre_target_id = $target_id;
						$target_pos ++;
						break;
					} else if ($source_id < $target_id) {
						// source miss
						if ($pre_source_id != $source_id) {
							$target_miss ++;
						}

						$pre_source_id = $source_id;
						$source_pos ++;
						break;
					} else {
						// target miss
						if ($pre_target_id != $target_id) {
							$source_miss ++;
						}
						$pre_target_id = $target_id;
						$target_pos ++;
					}

					if ($target_pos >= $target_size) {
						break;
					}
				}
			}

			// do remain
			if ($insert_count > 0) {
				echo "do sql:\n";
				$sql = $sql." END WHERE fund_code in ({$id_list}) ";
				$ret=DB::runSql($sql);
			}

			echo "source missing : ";
			echo "{$source_miss} \n";
			echo "target missing: ";
			echo "{$target_miss} \n";
		}
	}

	function execute_syn_fund_manager_funds_fund_size(&$sql, &$id_list, $item, &$insert_count, $margin) {
		if (empty($item["TOTALTNA"])) {
			return;
		}

		if ($insert_count != 0) {
			$sql = $sql." ";
			$id_list = $id_list.",";
		} else {
			$sql = "update test_fund_manager_funds set fund_size = case ";
		}

		$sql = $sql."WHEN fund_code = {$item["SYMBOL"]} THEN {$item["TOTALTNA"]}";
		$id_list = $id_list." {$item["SYMBOL"]}";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql = $sql." END WHERE fund_code in ({$id_list}) ";
			$ret = DB::runSql($sql);
			$sql = "";
			$id_list = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}


	function syn_fund_manager_funds_manager_code() {
		$sql = "select manager_code, fund_code, name from wangyu_test order by fund_code";
		$source_data = DB::getData($sql);
		$source_size = count($source_data);


		$sql = "select fund_code, manager_name from test_fund_manager_funds order by fund_code";
		$target_data = DB::getData($sql);
		$target_size = count($target_data);

		// check for match, then update
		$source_pos = 0;
		$target_pos = 0;
		$insert_count = 0;
		$sql = "";
		$margin = 50;
		$source_miss = 0;
		$target_miss = 0;
		$source_start = 0;
		$source_end = -1;
		$target_start = 0;
		$target_end = -1;
		$sql = "";
		$symbol_list = "";
		$name_list = "";
		for (; $source_start < $source_size; ) {
			$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);
			for (; $target_start < $target_size; ) {
				$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
				$source_id = $source_data[$source_start]["fund_code"];
				$target_id = $target_data[$target_start]["fund_code"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_fund_manager_funds_manager_code_per_fund($sql, $symbol_list, $name_list, $insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin);

					echo "{$source_id}, {$source_data[$source_pos]["name"]}, {$target_data[$target_pos]["fund_code"]}\n";
					$source_start = $source_end + 1;
					$target_start = $target_end + 1;
					break;
				} else if ($source_id < $target_id) {
					// target miss
					$target_miss ++;
					$this->get_start_and_end_pos($source_data, "fund_code", $source_size, $source_start, $source_end);

					$source_start = $source_end + 1;
					break;
				} else {
					// source miss
					$source_miss ++;
					$this->get_start_and_end_pos($target_data, "fund_code", $target_size, $target_start, $target_end);
					$target_start = $target_end + 1;
				}
			}
			if ($target_start >= $target_size) {
				break;
			}
		}

		// do remain
		if ($insert_count > 0) {
			echo "do sql\n";
			$sql = $sql." END WHERE fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$ret = DB::runSql($sql);
			$sql = "";
			$insert_count = 0;
		}

		echo "source missing : ";
		echo "{$source_miss} \n";
		echo "target missing: ";
		echo "{$target_miss} \n";

	}

	function syn_fund_manager_funds_manager_code_per_fund(&$sql, &$symbol_list, &$name_list, &$insert_count, $source_data, $source_start, $source_end, $target_data, $target_start, $target_end, $margin) {
		$source_pos = $source_start;
		$target_pos = $target_start;
		for (; $source_pos < $source_end + 1; $source_pos ++) {
			for ($target_pos = $target_start; $target_pos < $target_end + 1; $target_pos ++) {
				$source_id = $source_data[$source_pos]["name"];
				$target_id = $target_data[$target_pos]["manager_name"];

				if ($target_id == $source_id) {
					// do it
					$this->syn_fund_manager_funds_manager_code_per_item($sql, $symbol_list, $name_list, $insert_count, $source_data[$source_pos], $margin);

					echo "{$source_id}, {$source_data[$source_pos]["manager_code"]}, {$target_data[$target_pos]["manager_name"]}\n";
					break;
				}
			}
		}
	}

	function syn_fund_manager_funds_manager_code_per_item(&$sql, &$symbol_list, &$name_list, &$insert_count, $item, $margin) {
		if ($insert_count != 0) {
			$sql = $sql." ";
			$name_list = $name_list.",";
			$symbol_list = $symbol_list.",";
		} else {
			$sql = "update test_fund_manager_funds set manager_code = case ";
		}

		$sql = $sql." WHEN fund_code = '{$item["fund_code"]}' and  manager_name = '{$item["name"]}' THEN '{$item["manager_code"]}'";
		$symbol_list = $symbol_list." '{$item["fund_code"]}'";
		$name_list = $name_list." '{$item["name"]}'";

		if ( ($insert_count % $margin) == ($margin - 1) ) {
			echo "do sql\n";
			$sql = $sql." END WHERE fund_code in ({$symbol_list}) and manager_name in ({$name_list})";
			$ret = DB::runSql($sql);
			$sql = "";
			$name_list = "";
			$symbol_list = "";
			$insert_count = 0;
		} else {
			$insert_count ++;
		}
	}


	function wangyu_test_2() {
		$sql="exec [dbo].graspFundInfo";
		$ret = self::runRootSql($sql);
		$sql = 'select * from [dbo].[table_test_res] ORDER BY SYMBOL';
		$data = self::getData($sql);
		var_dump($data[0]["CUSTODIANFEE"]);
		$convert_string = ($data[0]["CUSTODIANFEE"]);
		var_dump($convert_string);
		$convert_int = (float) ($convert_string);
		var_dump($convert_int);

		$convert_string = "0".$convert_string;
		var_dump($convert_string);
		$convert_int = (float) ($convert_string);
		var_dump($convert_int);
	}

	function wangyu_test() {
		/*
		$sql = "select count(*) from test_fund_stock";
    echo "  blank: ";
		$ret = DB::getData($sql);
		var_dump($ret);
		var_dump($ret);
		$sql = "select `column_name` from `information_schema`.`columns` where `table_name`='test_fund_stock'";
		$ret = DB::getData($sql);
		var_dump($ret);
		*/
		$source_table = "fund_info";
		$target_table = "test_fund_info";
		$order_id = "code";
		$is_value = false;
		$exclude_array=["id", "status_desc", "fund_type", "invest_gain", "update_date", "name", "fullname", "start_date", "exchange_status", "company", "manager"];
		$this->compareTable($source_table, $target_table, $order_id, $is_value, $exclude_array);


	}

	function update_for_one_range() {
		//$symbol = "000574";
		//self::countRange($symbol);
		//echo 'Current PHP version: ' . phpversion();
		$sql = "update test_php_pdo set a = 1;update test_php_pdo set b = 2";
		$ret = DB::runSql($sql);
		var_dump($ret);

	}

	public function bonusfix_for_one_per()
	{
		$symbol = "000574";
		$i = $j = 0;
		$sql = "SELECT * FROM `fund_bonus` where code = '{$symbol}'";
		var_dump($sql);
		$data = DB::getData("SELECT * FROM `fund_bonus` where code = '{$symbol}'");
		foreach ($data as $item) {
			var_dump($item);
			$id = $item['id'];
			$bonus = $item['bonus'];
			$oldPer = $item['per'];
			if (preg_match_all('/.*?10.*?([\d\.]+)/', $bonus, $matches)) {
				$tmp = $matches[1][0];
				$per = round(($tmp / 10), 4);
				if ($per == 0) {
					exit("Zero Found {$per} {$id}");
				}
				if ($oldPer != $per) {
					$i++;
					DB::runSql("UPDATE `fund_bonus` SET `per`='{$per}' WHERE id={$id}");
					self::log("{$id} FIXED {$oldPer}=>{$per}");
				} else {
					$j++;
					self::log("{$id} IS ALREADY NEW");
				}
			} else {
				exit("not Match {$id}");
			}
		}
		$num = count($data);
		self::log("FIXED {$i}, OK {$j} ,Total {$num}");
	}

	public function syn_sing_fund_wangyu() {
		$symbol = '486002';
		//$this->syncSingle($symbol, false);
		$sql = "select distinct(code) from test_fund_manager_bonus_split where split_percent is NULL or per is NULL;";
		$data = DB::getData($sql);
		var_dump($data);
		foreach ($data as $item) {
			$symbol = $item["code"];
			$this->construct_relative_bonus_split_per_fund($symbol);
		}

	}


// wangyu change end
}




