<?php

/**
* tag ok
*/
class tag
{
    const URL='http://fundact.eastmoney.com/zhutijj/home/index?isapp=false&from=singlemessage&isappinstalled=1';


    function __construct()
    {
       
    }

    function index()
    {
        $this->setTag();
    }
    
    private static function log($msg)
    {
        echo date('H:i:s').'  '.$msg.PHP_EOL;
    }

    /**
     * 执行大约10分钟,无副作用,消耗网络较大,二次执行带有缓存,无需每天执行(每周执行即可)
     */
    function setTag()
    {
        $cachefile='tagcache.tmp';
        if(is_file($cachefile))
        {
            $result=unserialize(file_get_contents($cachefile));
            (is_array($result)&&!empty($result))||exit('error cache data');
        }
        else
        {
            $html=Curl::get(self::URL);
            $pattern='/\s+class="bggreen"\s+data-tp="(\w+)"\s+data-tpname="(.+?)"/';
            $result=array();
            if(preg_match_all($pattern, $html, $matches))
            {
               $tp=array_combine($matches[2],$matches[1]);
               foreach ($tp as $name => $id)
               {
                    $result[$name]=$this->getMore($name,$id);
               }
            }
            file_put_contents($cachefile,serialize($result));
        }
        $this->toDb($result);
    }

    private function getMore($value,$id)
    {
        $pattern='/<span\s+class="code">(\d+)<\/span>/';
        $allId=array();
        for ($page=1; $page <200 ; $page++)
        {
            $url="http://fundact.eastmoney.com/zhutijj/home/table?tp={$id}&rs=SYL_3Y&sort=SYL_3Y&sorttype=desc&pageindex={$page}";
            $html=Curl::get($url);
            if(empty(trim($html)))
            {
                self::log("{$id} Total Page : ".($page-1));
                break;
            }
            self::log("Get  {$id} At Page {$page} Ok");
            if(preg_match_all($pattern,$html,$matches))
            {
               $allId=array_merge($allId,$matches[1]);
            }
        }
        return $allId;
    }

    //save to db
    private function toDb($result)
    {
        foreach ($result as $name => $item)
        {
            foreach ($item as $code)
            {
                $origin=DB::getLine("SELECT * FROM `invest_product` WHERE code='{$code}'");
                $comments=explode(',',$origin['comment']);
                $comments=array_filter($comments,function($value){return trim($value);});
                if(!in_array($name,$comments) && !empty($origin))
                {
                    $comments[]=$name;
                    $comments=implode(',',$comments);
                    $sql="UPDATE `invest_product` SET comment='{$comments}' WHERE code ='{$code}' ";
                    $ret=DB::runSql($sql);
                    self::log("Updated {$code},Result:{$ret}");
                }
                else
                {
                    self::log("Code {$code} Already Update-To-Date");
                }
            }
        }
    }


}