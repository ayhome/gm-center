<?php
namespace ayhome\center;

use think\Db;
class Base
{
  protected $Agents,$Tasks;
  protected $onlineCli;

  protected $load_size = 8192; //最多载入任务数量
  protected $tasks_size = 1024; //同时运行任务最大数量
  protected $rebot = 126; //同时挂载worker数量
  protected $worker_num = 4; //worker进程数量
  protected $task_num = 4; //task进程数量

  public $crontabKey = 'crontab-list';
  public $taskKey = 'task-list';

  public function onTask($server, $task_id, $from_id, $data)
  {
    $this->show('onTask');
  }

  public function onFinish($server, $task_id, $data)
  {
    $this->show('onFinish');
  }


  

  public function loadAgents()
  {
    $map['status'] = 1;
    Db::name('agents')->where($map)->select();
  }

  public function getClient($fd ='',$ip = '')
  {
    $onlineCli = cache('onlineCli');
    if ($fd) {
      $cli = $onlineCli[$fd];
      return $cli;
    }elseif ($ip) {
      foreach ($onlineCli as $fd => $cli) {
        if ($cli['remote_ip'] == $ip) {
          return $cli;
        }
      }
    }
    return false;
    
  }

  public function removeClient($fd ='',$ip = '')
  {
    unset($this->onlineCli[$fd]);
    cache('onlineCli',$this->onlineCli);
  }

  public function getActiveAgent($value='')
  {
    # code...
  }

  public function loadTasks()
  {
    $start = 0;
    $key = 'task-list';
    while (true) {
      $map['status'] = 1;
      $tasks = Db::name('task')->limit($start,100)->select();
      if (empty($tasks)) {
        break;
      }
      //先获取缓存里面的list 
      $taskList = cache($this->taskKey);
      foreach ($tasks as $task) {
        //判断任务队列数量
        $taskSize = count($taskList);
        if ($taskSize > $this->load_size) {
          return true;
        }
        $taskList[$task['id']] = $task;
      }
      //覆盖缓存
      cache($this->taskKey,$taskList);
      $start += 100;
    }
    
    return true;
    # code...
  }

  public function getCrons()
  {
    $crons = cache($this->crontabKey);

    if (count($crons) <= 0) {
      return [];
    }
    $min = date("YmdHi");

    $data = array();
    $ndata = array();
    foreach ($crons as $k => $task) {
      if ($min == $task["minute"]) {
        if (time() == $task["sec"] && $task["runStatus"] == 0) {
          $data[$k] = $task;
        }else{
          $ndata[] = $task;
        }
      }
    }
    cache($this->crontabKey,$ndata);
    
    return $data;
    # code...
  }

  //清理完成的任务
  public function checkCron($value='')
  {
    //清理完成任务
    $this->cleanCrons();

    $tasks = cache($this->taskKey);
    // print_r($tasks);
    $taskSize = count($tasks);
    $time = time();

    $crons = array();
    foreach ($tasks as $id => $task) {
      //当任务状态不正常 则不继续
      if ($task["status"] == -1) continue;
      $ret = ParseCrontab::parse($task["rule"], $time);
      if ($ret === false) {
        $this->show(ParseCrontab::$error);
      } elseif (!empty($ret)) {
        $min = date("YmdHi");
        $time = strtotime(date("Y-m-d H:i"));
        foreach ($ret as $sec) {
          if (count($taskSize) > $this->tasks_size){
            $this->show('checkTasks fail ,because tasks size Max');
            break;
          }
          $k = $this->generateRunId();
          $v = array();
          $v = $task;
          $v['minute'] = $min;
          $v['sec'] = $time + $sec;
          $v['id'] = $id;
          $v['guid'] = $k;
          $v['runStatus'] = 0;
          $crons[$k] = $v;
        }
      }
    }
    cache($this->crontabKey,$crons);
    return true;
    # code...
  }

  public function cleanCrons($value='')
  {
    $tasks = cache($this->crontabKey);
    $taskSize = count($tasks);
    if ($taskSize < 1) return true;

    $minute = date("YmdHi");
    $data = array();
    foreach ($tasks as $id => $task) {
      //运行成功和运行失败的
      if ($task["runStatus"] == 4 || $task["runStatus"] == 5) {
        $data[$id] = $task;
        continue;
      } else {
        if (intval($minute) > intval($task["minute"]) + 5) {
          $runStatus = $task["runStatus"];
          $ids[] = $id;
          //超时的
          if ($runStatus == 1 || $runStatus == 2 || $runStatus == -1 ) {
            $ids2[] = $task["id"];
            $task['execNum'] = $task['execNum'] -1;
            $data[$id] = $task;
          }else{
            $task['sec'] < time();
            $data[$id] = $task;
          }
        }
      }

    }
    cache($this->crontabKey,$data);
    return true;
  }

  public function checkTask($value='')
  {
    # code...
  }

  public function findTask($value='')
  {
    # code...
  }

  public function register($serv, $src_worker_id, $data)
  {
    # code...
  }


  public function show($info='',$data = '')
  {
    $time = date('Y-m-d H:i:s');
    if ($data) {
      $d = $this->decode($data);
      $info .="\t".$d;
      # code...
    }
    echo "{$time}\t{$info}\n";
  }


  public function addCronLog($cron='')
  {
    // 100 读取任务  200 任务下发  300 接收到任务 400 开始执行 500 任务执行完成  
    // -100 任务读取失败  -200 任务下发失败 -300 任务执行失败 
    if ($cron['run_code'] == 100) {
      $cron['title'] = '读取任务成功';
    }elseif ($cron['run_code'] == 200) {
      $cron['title'] = '任务下发成功';
    }elseif ($cron['run_code'] == 300) {
      $cron['title'] = '任务接收成功';
    }elseif ($cron['run_code'] == 400) {
      $cron['title'] = '开始执行';
    }elseif ($cron['run_code'] == 500) {
      $cron['title'] = '任务执行完成';
    }elseif ($cron['run_code'] == -100) {
      $cron['title'] = '任务读取失败';
    }elseif ($cron['run_code'] == -200) {
      $cron['title'] = '任务下发失败';
    }elseif ($cron['run_code'] == -300) {
      $cron['title'] = '任务执行失败';
    }


    $cron['run_id'] = $cron['guid'];
    $cron['task_type'] = $cron['type'];
    $cron['task_id'] = $cron['id'];
    $cron['task_name'] = $cron['name'];
    unset($cron['id']);


    Db::name('task_logs')->insert($cron);


    # code...
  }


  public function generateRunId($length = 8)
  {
    $code = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    $rand = $code[rand(0,25)] .strtoupper(dechex(date('m'))) .date('d').substr(time(),-5) .substr(microtime(),2,5) .sprintf('%02d',rand(0,99));
    for( $a = md5( $rand, true ), $s = '0123456789ABCDEFGHIJKLMNOPQRSTUV', $d = '', $f = 0; $f < $length; $g = ord( $a[ $f ] ), $d .= $s[ ( $g ^ ord( $a[ $f + 8 ] ) ) - $g & 0x1F ], $f++ );
    return $d;
  }

  //编码
  public function encode($data='')
  {
    return json_encode($data);
    # code...
  }

  //解码
  public function decode($data='')
  {
    return json_decode($data,true);
    # code...
  }
}