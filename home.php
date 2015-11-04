<?php

/***
*
* 框架 http://github.com/suconghou/mvc
* 要求PHP5.4+
* /home/article  列表页 , 传送 get 参数 page 取分页
* /home/article/381  详情页
* /home/rsync 同步数据
* 
* 可以继承base类或者其他控制器类,也可以继承模型类,也可以什么都不继承
* 
*/
class home
{

	
	function __construct()
	{
		//开启跨域
        // header('Access-Control-Allow-Origin: *',true);
        // header('Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept',true);
	}
	/**
	 * home 是默认的控制器
	 * index 时默认的方法
	 */
	function index()
	{
		var_dump($_POST);
	}
	function test()
	{
		app::log("test");
	}
	function phpinfo()
	{
		phpinfo();
	}

	//文章列表页,文章详情页数据,数据表 [SIMU]
	function article($id=null)
	{
		$page=isset($_GET['page'])?intval($_GET['page']):1;
		$pageSize=isset($_GET['pagesize'])?intval($_GET['pagesize']):10;
		if($id)
		{
			$id=intval($id);
			$sql="SELECT * FROM SIMU WHERE ID ={$id}";
			$article=DB::getLine($sql);
			if($article)
			{
				self::json(array('code'=>0,'data'=>$article));
			}
			else
			{
				self::json(array('code'=>-1,'msg'=>'没有找到这篇文章哦!'));
			}
		}
		else
		{
			$offset=($page-1)*$pageSize;
			$sql="SELECT * FROM SIMU ORDER BY UPDATEID LIMIT {$offset},{$pageSize}";
			$data=DB::getData($sql);
			if($data)
			{
				foreach ($data as &$item)
				{
					unset($item['CONTENT']);
				}
				$total=DB::getVar("SELECT COUNT(1) FROM SIMU");
				$total=ceil($total/$pageSize);
				self::json(array('code'=>0,'page'=>$page,'total'=>$total,'data'=>$data));
			}
			else
			{
				self::json(array('code'=>-1,'msg'=>'哇,你已经全部都看完了!'));
			}
		}
	}
	private static function json($data)
	{
		exit(json_encode($data,JSON_UNESCAPED_UNICODE));
	}

	//同步新闻数据
	function rsync($password=null)
	{

		$mssql = new PDO("odbc:Driver={SQL Server};Server=10.7.17.92;Database=GTA_QIA_QDB;",'funddev','123456789Aa');
		$rs = $mssql->query("SELECT TOP 20 * FROM [DBO].[NEWS_NEWSINFO] ORDER BY UPDATEID DESC"); 
		$data=$rs->fetchAll(PDO::FETCH_ASSOC);
		$ret=array();
		foreach ($data as $item)
		{
			$updateId= iconv("GBK", "UTF-8", $item['UPDATEID']);
			$title=iconv("GBK", "UTF-8", $item['TITLE']);
			$summary=iconv("GBK", "UTF-8", $item['NEWSSUMMARY']);
			$content=iconv("GBK", "UTF-8", $item['NEWSCONTENT']);
			$declareDate=iconv("GBK", "UTF-8", $item['DECLAREDATE']);
			$sql="INSERT INTO SIMU (UPDATEID,TITLE,SUMMARY,CONTENT,DECLAREDATE) VALUES($updateId,'{$title}','{$summary}','{$content}','{$declareDate}') ON DUPLICATE KEY UPDATE DECLAREDATE='{$declareDate}' ";
			$ret[]=DB::runSql($sql);
		}
		echo implode(',',$ret).PHP_EOL;
	}

	/**
	 * 注意输出图片之前之后都不能有任何其他输出
	 */
	function img()
	{
		header("Content-type: image/gif",true);
		echo base64_decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7");
	}


	function Error404($msg=null)
	{
		exit('404 ERROR FOUND:'.$msg);
	}


	function Error500($msg=null)
	{
		exit('500 ERROR FOUND:'.$msg);
	}
	



}

