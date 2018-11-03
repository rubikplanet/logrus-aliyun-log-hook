package aliyunloghook

import (
	"sync"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"
)

type AliyunLogHook struct {
	mu              sync.RWMutex
	endpoint        string
	accessKeyID     string
	accessKeySecret string
	projectName     string
	logstoreName    string
	topic           string
	source          string
	Project         *sls.LogProject
	Logstore        *sls.LogStore
}

func NewAliyunLogHook(endpoint string, projectName string, logstoreName string, accessKeyID string, accessKeySecret string, topic string, source string) *AliyunLogHook {
	hook := &AliyunLogHook{}
	hook.endpoint = endpoint
	hook.accessKeyID = accessKeyID
	hook.accessKeySecret = accessKeySecret
	hook.projectName = projectName
	hook.logstoreName = logstoreName
	hook.topic = topic
	hook.source = source
	slsProject, _ := sls.NewLogProject(hook.projectName, hook.endpoint, hook.accessKeyID, hook.accessKeySecret)
	hook.Project = slsProject
	slsLogstore, _ := sls.NewLogStore(hook.logstoreName, hook.Project)
	hook.Logstore = slsLogstore
	return hook
}

func (hook *AliyunLogHook) Fire(entry *logrus.Entry) error {
	hook.mu.RLock()
	defer hook.mu.RUnlock()

	logRecord := &sls.Log{
		Time:     proto.Uint32(uint32(entry.Time.Unix())),
		Contents: []*sls.LogContent{},
	}

	level := &sls.LogContent{
		Key:   proto.String("level"),
		Value: proto.String(entry.Level.String()),
	}
	logRecord.Contents = append(logRecord.Contents, level)

	message := &sls.LogContent{
		Key:   proto.String("message"),
		Value: proto.String(entry.Message),
	}
	logRecord.Contents = append(logRecord.Contents, message)

	lg := &sls.LogGroup{
		Topic:  proto.String(hook.topic),
		Source: proto.String(hook.source),
		Logs:   []*sls.Log{logRecord},
	}
	hook.Logstore.PutLogs(lg)
	return nil
}

func (hook *AliyunLogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
