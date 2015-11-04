<?php



/**
* 
*/
class fund
{
    
    function __construct()
    {
        
    }

    function index()
    {
        $this->range();
    }

    private static function log($msg)
    {
        echo date('Y-m-d H:i:s').' '.$msg.PHP_EOL;
    }
    
    /**
     * 可以多次执行,无影响,但不需要
     * 内存,时间消耗 较小,20分钟左右
     */
    function range()
    {
        $sql="SELECT `code` FROM `fund_info` WHERE fund_type='理财型' or fund_type='货币型' ORDER BY `code` ";
        $data=DB::getData($sql);
        //需要计算的基金有哪些
        foreach ($data as $item)
        {
            $code=$item['code'];
            $sql="SELECT id,`range`,unit FROM `fund_value` WHERE `code`='{$code}' ORDER BY create_date";
            $origin=DB::getData($sql);
            $prevUnit=null;
            self::log("update for {$code}");
            foreach ($origin as $i=>$line)
            {
                if($i==0)
                {
                    $range='0.00%';
                }
                else
                {
                    $range=sprintf('%.2f',$prevUnit/100).'%';
                }
                $sql="UPDATE `fund_value` SET `range`='{$range}' WHERE id={$line['id']}";
                $ret=DB::runSql($sql);
                $prevUnit=$line['unit'];
            }
        }
        self::log('finished');
    }

    /**
     * 计算基金的万分收益
     * range=前一天的unit*100%
     * !!!!!!已弃用
     */
    function range2()
    {
        $sql="SELECT  DISTINCT `code` FROM `fund_value` WHERE `range` ='' ORDER BY `code`";
        $code=DB::getData($sql);
        self::log('total num '.count($code));
        $fillINDEX=array();
        foreach ($code as $c)
        {
            $c=$c['code'];
            $sql="SELECT id,`range`,unit FROM `fund_value` WHERE `code`='{$c}' ORDER BY create_date";
            $data=DB::getData($sql);
            $id=$this->tryFillIndex($data,$c);
            if($id&&is_numeric($id))
            {
                $fillINDEX[]=$id;
            }
        }
        if(!empty($fillINDEX))
        {
            $str=implode(',',$fillINDEX);
            $sql="UPDATE `fund_value` SET `range`='0.00%' WHERE id IN ({$str}) and `range`='' ";
            $ret=DB::runSql($sql);
            self::log('fill index num '.count($fillINDEX).' result: '.$ret);
        }
        self::log('finished');
    }

    function tryFillIndex($data,$code)
    {
        $catch=array();
        $firstRange=$data[0]['range'];
        $data2=array_filter($data,function($item)
        {
            return !empty($item['range']);
        });
        $c1=count($data);
        $c2=count($data2);
        if($c1==$c2)
        {
            self::log($code.' all have values');
        }
        else
        {
            if(($c1-1==$c2) &&empty($firstRange))
            {
                self::log($code.' to fill index ');
                return $data[0]['id'];
            }
            else
            {
                self::log($code.' has much empty');
            }
        }
    }

    function fillRange($data,$code)
    {

    }
}