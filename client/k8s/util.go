package k8s

import (
	"fmt"

	redisv1beta1 "github.com/jdheyburn/redis-operator/api/v1beta1"
)

func GetRedisPassword(s Services, rc *redisv1beta1.RedisCluster) (string, error) {

	if rc.Spec.Auth.SecretPath == "" {
		// no auth settings specified, return blank password
		return "", nil
	}

	secret, err := s.GetSecret(rc.ObjectMeta.Namespace, rc.Spec.Auth.SecretPath)
	if err != nil {
		return "", err
	}

	if password, ok := secret.Data["password"]; ok {
		return string(password), nil
	}

	return "", fmt.Errorf("secret \"%s\" does not have a password field", rc.Spec.Auth.SecretPath)
}
