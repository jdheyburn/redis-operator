package service

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-logr/logr"

	redisv1beta1 "github.com/jdheyburn/redis-operator/api/v1beta1"
	"github.com/jdheyburn/redis-operator/client/k8s"
	"github.com/jdheyburn/redis-operator/client/redis"
	"github.com/jdheyburn/redis-operator/util"
)

// RedisClusterHeal defines the intercace able to fix the problems on the redis clusters
type RedisClusterHeal interface {
	MakeMaster(ip string, redisCluster *redisv1beta1.RedisCluster) error
	SetOldestAsMaster(redisCluster *redisv1beta1.RedisCluster) error
	SetMasterOnAll(masterIP string, redisCluster *redisv1beta1.RedisCluster) error
	NewSentinelMonitor(ip string, monitor string, redisCluster *redisv1beta1.RedisCluster) error
	RestoreSentinel(ip string, redisCluster *redisv1beta1.RedisCluster) error
	SetSentinelCustomConfig(ip string, redisCluster *redisv1beta1.RedisCluster) error
	SetRedisCustomConfig(ip string, redisCluster *redisv1beta1.RedisCluster) error
}

// RedisClusterHealer is our implementation of RedisClusterCheck intercace
type RedisClusterHealer struct {
	k8sService  k8s.Services
	redisClient redis.Client
	logger      logr.Logger
}

// NewRedisClusterHealer creates an object of the RedisClusterChecker struct
func NewRedisClusterHealer(k8sService k8s.Services, redisClient redis.Client, logger logr.Logger) *RedisClusterHealer {
	return &RedisClusterHealer{
		k8sService:  k8sService,
		redisClient: redisClient,
		logger:      logger,
	}
}

func (r *RedisClusterHealer) MakeMaster(ip string, rc *redisv1beta1.RedisCluster) error {

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	return r.redisClient.MakeMaster(ip, password)
}

// SetOldestAsMaster puts all redis to the same master, choosen by order of appearance
func (r *RedisClusterHealer) SetOldestAsMaster(rc *redisv1beta1.RedisCluster) error {
	ssp, err := r.k8sService.GetStatefulSetPods(rc.Namespace, util.GetRedisName(rc))
	if err != nil {
		return err
	}
	if len(ssp.Items) < 1 {
		return errors.New("number of redis pods are 0")
	}

	// Order the pods so we start by the oldest one
	sort.Slice(ssp.Items, func(i, j int) bool {
		return ssp.Items[i].CreationTimestamp.Before(&ssp.Items[j].CreationTimestamp)
	})

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	newMasterIP := ""
	for _, pod := range ssp.Items {
		if newMasterIP == "" {
			newMasterIP = pod.Status.PodIP
			r.logger.V(2).Info(fmt.Sprintf("new master is %s with ip %s", pod.Name, newMasterIP))
			if err := r.redisClient.MakeMaster(newMasterIP, password); err != nil {
				return err
			}
		} else {
			r.logger.V(2).Info(fmt.Sprintf("making pod %s slave of %s", pod.Name, newMasterIP))
			if err := r.redisClient.MakeSlaveOf(pod.Status.PodIP, newMasterIP, password); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetMasterOnAll puts all redis nodes as a slave of a given master
func (r *RedisClusterHealer) SetMasterOnAll(masterIP string, rc *redisv1beta1.RedisCluster) error {
	ssp, err := r.k8sService.GetStatefulSetPods(rc.Namespace, util.GetRedisName(rc))
	if err != nil {
		return err
	}

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	for _, pod := range ssp.Items {
		if pod.Status.PodIP == masterIP {
			r.logger.V(2).Info(fmt.Sprintf("ensure pod %s is master", pod.Name))
			if err := r.redisClient.MakeMaster(masterIP, password); err != nil {
				return err
			}
		} else {
			r.logger.V(2).Info(fmt.Sprintf("making pod %s slave of %s", pod.Name, masterIP))
			if err := r.redisClient.MakeSlaveOf(pod.Status.PodIP, masterIP, password); err != nil {
				return err
			}
		}
	}
	return nil
}

// NewSentinelMonitor changes the master that Sentinel has to monitor
func (r *RedisClusterHealer) NewSentinelMonitor(ip string, monitor string, rc *redisv1beta1.RedisCluster) error {
	r.logger.V(2).Info("sentinel is not monitoring the correct master, changing...")
	quorum := strconv.Itoa(int(getQuorum(rc)))

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	return r.redisClient.MonitorRedis(ip, monitor, quorum, password)
}

// RestoreSentinel clear the number of sentinels on memory
func (r *RedisClusterHealer) RestoreSentinel(ip string, rc *redisv1beta1.RedisCluster) error {
	r.logger.V(2).Info(fmt.Sprintf("restoring sentinel %s...", ip))

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	return r.redisClient.ResetSentinel(ip, password)
}

// SetSentinelCustomConfig will call sentinel to set the configuration given in config
func (r *RedisClusterHealer) SetSentinelCustomConfig(ip string, rc *redisv1beta1.RedisCluster) error {
	if len(rc.Spec.Sentinel.CustomConfig) == 0 {
		return nil
	}
	r.logger.V(2).Info(fmt.Sprintf(fmt.Sprintf("setting the custom config on sentinel %s: %v", ip, rc.Spec.Sentinel.CustomConfig)))

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	return r.redisClient.SetCustomSentinelConfig(ip, rc.Spec.Sentinel.CustomConfig, password)
}

// SetRedisCustomConfig will call redis to set the configuration given in config
func (r *RedisClusterHealer) SetRedisCustomConfig(ip string, rc *redisv1beta1.RedisCluster) error {

	password, err := k8s.GetRedisPassword(r.k8sService, rc)
	if err != nil {
		return err
	}

	if len(rc.Spec.Config) == 0 && len(password) == 0 {
		return nil
	}

	// TODO JH significance in this being commented out?
	//if len(auth.Password) != 0 {
	//	rc.Spec.Config["requirepass"] = auth.Password
	//	rc.Spec.Config["masterauth"] = auth.Password
	//}

	r.logger.V(2).Info(fmt.Sprintf("setting the custom config on redis %s: %v", ip, rc.Spec.Config))

	return r.redisClient.SetCustomRedisConfig(ip, rc.Spec.Config, password)
}
