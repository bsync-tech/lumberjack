package main

import (
	util "common/util_v2"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"common/mysignal"
	_ "qxb_cus_binlog_2_file/version"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/natefinch/lumberjack"
)

var (
	err         error
	httpUnixSrv *http.Server

	sig *mysignal.Signal
	gcf Config

	lm  *loggerManger
	drl *delayRotateLogger
	ru  *recoupUpload
)

var (
	myjson = jsoniter.ConfigCompatibleWithStandardLibrary
)

func finality() {
	defer log.Flush()
	log.Infof("[%d] start to shutdown unixSrv...", os.Getpid())
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ru.Stop()
	lm.Stop()
	if drl != nil {
		drl.Stop()
	}

	log.Infof("[%d] unixSrv Stop", os.Getpid())
	for len(buffer) != 0 {
		time.Sleep(500 * time.Millisecond)
		log.Debugf("waiting for task buffer winding up %d", len(buffer))
	}
	log.Infof("[%d] clear buffer tasks finish", os.Getpid())

	os.Exit(-1)
}

func init() {
	defer func() {
		if err := recover(); err != nil {
			log.Criticalf("[%d] Init recover: %v", os.Getgid(), err)
			time.Sleep(time.Millisecond * 500)
			os.Exit(1)
		}
	}()
	if err = util.ParseConf(&gcf); err != nil {
		log.Criticalf("[%d] ParseConf: %v", os.Getpid(), err)
		panic(fmt.Sprintf("parse conf err: %v", err))
	}

	log.Infof("[%d] log config file path: %s", os.Getpid(), gcf.LogCfgPath)
	log.Infof("sqlitedb %s", gcf.Sqlitedb)
	lumberjack.Init(gcf.Sqlitedb)

	if err = util.NewLogger(gcf.LogCfgPath); err != nil {
		log.Criticalf("[%d] NewLogger: %v", os.Getpid(), err)
		panic(fmt.Sprintf("new logger err: %v", err))
	}

	if err = checkConf(&gcf); err != nil {
		panic(fmt.Sprintf("check config err: %v", err))
	}

	lm = NewLoggerManager()
	if err = lm.reloadHistoryFiles(gcf.LogFilePath); err != nil {
		panic(fmt.Sprintf("reload history files err: %v", err))
	}

	if gcf.DelayRotate.Enable {
		drl = NewDelayRotateLogger(gcf.DelayRotate.MinFiles, gcf.DelayRotate.MinDelay, gcf.DelayRotate.MaxDelay)
	}

	ru = &recoupUpload{}

	initUpstream()
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("panic: %v", err)
			log.Flush()
			time.Sleep(time.Millisecond * 500)
			os.Exit(-1)
		}
		log.Flush()
	}()

	if gcf.UseTest {
		config, _ := json.Marshal(gcf)
		log.Infof("config: %s", config)
	}

	if gcf.UseTest {
		lm.dump()
	}

	var httpSrvWg util.WaitGroupWrapper

	router := gin.Default()
	router.GET("/", ServeHTTP)

	srv := &http.Server{
		Addr:    gcf.HttpAddr,
		Handler: router,
	}

	httpSrvWg.Wrap(func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil {
			log.Errorf("listen: %s\n", err)
		}
	})
	// 系统启动后，先查看文件夹中，
	// 是否有文件，有，则判断文件类型
	// 1. 基本文件，保存在logger_manager中，待继续写入(done)
	// 2. 已经完成写入的文件，则调用上传接口，上传文件(这里，时间间隔尽量长一些)
	// 定时检查是否有文件没有上传成功
	ru.ReUploadLoop(&httpSrvWg, time.Duration(gcf.History.RecoupInterval), gcf.LogFilePath, false, gcf.History.ValidSuffix)

	// 开启强制文件rotate goroutine
	lm.forceRotateTimerLoop(&httpSrvWg, time.Duration(gcf.LogFileRotateTimerInterval),
		time.Duration(gcf.LogFileRotateCycle))

	// 如果需要延迟rotate，则开启goroutine
	if gcf.DelayRotate.Enable {
		drl.RotateLoop(&httpSrvWg)
	}
	// 消费缓冲队列
	httpSrvWg.Wrap(consumeLineTasks)
	// 性能监控
	// go http.ListenAndServe(":6789", nil)

	httpSrvWg.Wait()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Infof("Shutdown Server ...")
	finality()
}
