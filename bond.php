<?php

/**
* bond 中证全债
*/
class bond
{
	const URL='http://www.csindex.com.cn/sseportal/csiportal/zq/bondshow.do?type=1';
	
	function __construct()
	{
		
	}

	function index()
	{
		$this->work();
	}

	function work()
	{
		$date=self::getStart();
		$datetime1 = new DateTime($date);
		$datetime2 = new DateTime(date('Y-m-d'));
		$interval = $datetime1->diff($datetime2);
		$days=$interval->format('%a');
		self::log("Work Start At {$date}, All {$days} Days");
		while ($days>0)
		{
			$days--;
			$getDate=date('Y-m-d',strtotime("-{$days}day"));
			$html=Curl::post(self::URL,array('date_time'=>$getDate),60);
			self::log("Get Date {$getDate}");
			$this->getInfo($getDate,$html);
		}
	}

	private static function getStart()
	{
		$sql="SELECT `date` FROM `bond` ORDER BY `date` DESC LIMIT 1";
		$date=DB::getVar($sql);
		return $date?$date:'2003-01-01';
	}

	private function getInfo($date,$html)
	{
		$pattern='/H11001[\s\S]+?中证全债[\s\S]+?([\w-]{10})[\s\S]+?(\d+\.\d{2})[\s\S]+?(-?\d+\.\d{2})[\s\S]+?((?<!")\d{1,9}(?!"))[\s\S]+?((?<!")\d{1,9}(?!"))[\s\S]+?(\d+\.\d{2})[\s\S]+?(\d+\.\d{2})[\s\S]+?(\d+\.\d{2})/';
		$html=iconv('GBK','UTF-8',$html);
		if(preg_match_all($pattern,$html,$matches))
		{
			if(strlen($matches[0][0])>999)
			{
				file_put_contents('1.tmp',$html);
				exit(self::log("Date {$date} Match Error "));
			}
			$data=array(
					'date'=>$matches[1][0],
					'value'=>$matches[2][0],
					'range'=>$matches[3][0],
					'volumn'=>$matches[4][0],
					'deal'=>$matches[5][0],
					'fixed'=>$matches[6][0],
					'stand'=>$matches[7][0],
					'profit'=>$matches[8][0],
				);
			return $this->store($data);
		}
		return self::log("Date {$date} Not Match, Length ".strlen($html));
	}
	private function store($data)
	{
		$id=Database::insertData('bond',$data);
		self::log("Add Data {$data['date']} Ok {$id}");
	}
	private static function log($msg)
	{
		echo date('H:i:s').' '.$msg.PHP_EOL;
	}
}