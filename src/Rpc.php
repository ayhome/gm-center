<?php
namespace ayhome\center;

use think\Db;
use think\Cache;

use Group\Plugin\Rpc\Hprose;
use Group\App\App;
use swoole_http_server;
class Rpc extends Base
{
  protected $server_type;
  protected $server;
  protected $cacheDir;
  protected $config;

  public function __construct($opt = '')
  {
    if ($opt['host']) {
      $this->host = $opt['host'];
    }
    if ($opt['port']) {
      $this->port = $opt['port'];
    }
    if ($opt['debug']) {
      $this->debug = $opt['debug'];
    }


    Cache::init($this->redis_cfg);
    
    $this->cacheDir = './runtime';

    cache('onlineCli',null);
    $this->loadAgents();
    $this->loadTasks();
  }

  public function init()
  {
    \swoole_set_process_name(sprintf('gm-server:%s', 'center'));
    $this->checkStatus();

    $pid = posix_getpid();
    $this->mkDir($this->cacheDir."/center.pid");
    file_put_contents($this->cacheDir."/center.pid", $pid);

    $this->server = new \swoole_server($this->host, $this->port, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);

    $config = [
          //'daemonize' => true,
          'task_worker_num'=>4,
          'worker_num' => 4,    //worker process num
          // 'backlog' => 128,   //listen backlog
          'dispatch_mode' => 4,


          // 'max_request' => 2000,
          // 'heartbeat_idle_time' => 30,
          // 'heartbeat_check_interval' => 10,
          // 'package_body_offset' => 0,
          // 'open_length_check' => 0,
      ];

    $this->server->set($config);

    $this->server->on('WorkerStart', array($this, 'OnWorkerStart'));
    $this->server->on('Connect', array($this, 'onConnect'));
    $this->server->on('Receive', array($this, 'onReceive'));
    $this->server->on("Close",array($this, 'onClose'));
    $this->server->on("Shutdown",array($this, 'onShutdown'));
    $this->server->on("Start",array($this, 'onStart'));


    $this->server->on('Task', array($this, 'onTask'));
    $this->server->on('Finish', array($this, 'onFinish'));
    $this->server->on("PipeMessage",array($this, 'onPipeMessage'));


    
    $r = $this->server->start();

  }

  public function onStart($value='')
  {
    $info = "#{$this->host}:{$this->port}\t [服务上线成功]";
    $this->show($info);
  }

  public function onWorkerStart($server, $worker_id)
  {
    $worker_num =  4;
    $load_tasks =  0;
    $get_tasks = 1;
    $exec_tasks = 2;
    $manager_tasks = 3;
    if ($server->taskworker) {
      if ($worker_id == ($worker_num + $load_tasks)) {
        //准点载入任务
        // $after_time = (60 - date("s")) * 1000;
        $after_time = 1000;
        $server->after($after_time, function () use ($server) {
          $this->checkCron();
          $server->tick(60000, function () use ($server) {
            $this->checkCron();
          });
        });
      }
      if ($worker_id == $worker_num + $get_tasks) {
        $server->tick(500, function () use ($server) {
          $tasks = $this->getCrons();
          if (!empty($tasks)) {
            $ret = $server->sendMessage($tasks, ($worker_num + $manager_tasks));

          }
        });
      }
    }
    // print_r($worker_id."\n");
  }

  public function onConnect($server, $fd, $from_id)
  {
    $cliInfo = $server->connection_info($fd);
    $cliInfo['client_fd'] = $fd;
    $this->onlineCli = cache('onlineCli');
    $this->onlineCli[$fd] = $cliInfo;
    cache('onlineCli',$this->onlineCli);
    $info = "{$fd}#{$cliInfo['remote_ip']}\t [上线]";
    $this->show($info);
  }

  public function onReceive($server, $fd, $from_id, $data)
  {

    $cliInfo = $this->getClient($fd);
    $data = $this->decode($data);
    $data['cliInfo'] = $cliInfo;


    $worker_id = 1 - $serv->worker_id;
    $server->sendMessage($data, $worker_id);


    


  }


  public function onPipeMessage($serv, $src_worker_id, $data)
  {
    $onlineCli = cache('onlineCli');

    if ($data['task'] == 'register') {
      unset($map);
      $cliInfo = $data['cliInfo'];
      $map['ip'] = $cliInfo['remote_ip'];
      $agent = Db::name('agents')->where($map)->find();
      if ($agent['status'] == 1) {
        $info = "{$fd}#{$cliInfo['remote_ip']}\t[注册成功] ";
        $vo['utime'] = time();
        Db::name('agents')->where($map)->update($vo);
      }else{
        $info = "{$fd}#{$cliInfo['remote_ip']}\t[注册失败] ";
      }
      $this->show($info);
    }elseif ($data['code'] == 2 && $data['task']) {
      
      $cliInfo = $data['cliInfo'];

      $cron = $data['task'];

      // $this->addCronLog($data['task']);

      $info = "{$fd}#{$cliInfo['remote_ip']}\t [任务执行完成#{$cron['guid']}]";
      $this->show($info,$data);
    }else{

      foreach ($data as $guid => $cron) {
        $cli = $this->getClient('',$cron['agent_ip']);
        if ($cli) {
          $cron['run_code'] = 100;
          $this->addCronLog($cron);


          $ret['task'] = $cron;
          $ret['code'] = 0;
          $dd = array();
          $dd = $this->encode($ret);
          $r = $this->server->send($cli['client_fd'],$dd);
          $info = "{$cli['client_fd']}#{$cli['remote_ip']}\t [发送任务]#{$cron['guid']}";
          $this->show($info);


          if ($r) {
            $cron['run_code'] = 200;
            $this->addCronLog($cron);
          }else{
            $cron['run_code'] = -200;
            $this->addCronLog($cron);
          }

          
        }
      }


    }
  }

  public function onShutdown($server = '')
  {
    cache('onlineCli',null);
  }

  public function onClose($server, $fd, $from_id)
  {
    $cliInfo = $this->getClient($fd);
    $onlineCli = cache('onlineCli');
    $ndata = array();
    foreach ($onlineCli as &$key) {
      if ($key['client_fd'] == $fd) {
        unset($onlineCli[$key['client_fd']]);
      }
    }
    cache('onlineCli',$onlineCli);
    if ($cliInfo) {
      $info = "{$fd}#{$cliInfo['remote_ip']}\t [下线了]";
      $this->show($info);
    }
  }

  public function checkStatus()
  {   
    $args = getopt('s:');
    if(isset($args['s'])) {
      switch ($args['s']) {
        case 'reload':
          $pid = file_get_contents($this->cacheDir."/pid");
          echo "当前进程".$pid."\n";
          echo "热重启中\n";
          if ($pid) {
              if (swoole_process::kill($pid, 0)) {
                swoole_process::kill($pid, SIGUSR1);
              }
          }
          echo "重启完成\n";
          swoole_process::daemon();
          break;
        default:
            break;
      }
      exit;
    }
  }

  private function mkDir($dir)
  {
    $parts = explode('/', $dir);
    $file = array_pop($parts);
    $dir = '';
    foreach ($parts as $part) {
      if (!is_dir($dir .= "$part/")) {
           mkdir($dir);
      }
    }
  }

  
}
