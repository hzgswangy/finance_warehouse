<?php

ini_set('display_errors',E_ALL);
    class hashThread extends Thread {
    private $max = 1;
    private $index = 0;
    function __construct($max, $index)
    {
        $this->max = $max;
        $this->index = $index;
    }
    public function run()
    {
        for ($i=1; $i<=$this->max; $i++)
        {
            md5($i);
        }
        echo "Thread #{$this->index} finished\r\n";
    }
    }
    $thread_count = 8;
    $start_time = microtime(true);
    for($i=1; $i<=$thread_count; $i++)
    {
    $thread[$i] = new hashThread(1e4, $i);
    $thread[$i]->start();
    }
    echo "Done in: " . round(microtime(true) - $start_time, 2) . " seconds";