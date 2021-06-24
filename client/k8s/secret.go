package k8s

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Secret interacts with k8s to get secrets
type Secret interface {
	GetSecret(namespace, name string) (*corev1.Secret, error)
}

// SecretService is the secret service implementation using API calls to kubernetes.
type SecretsOption struct {
	client client.Client
	logger logr.Logger
}

func NewSecret(kubeClient client.Client, logger logr.Logger) *SecretsOption {
	logger = logger.WithValues("service", "k8s.secret")
	return &SecretsOption{
		client: kubeClient,
		logger: logger,
	}
}

func (s *SecretsOption) GetSecret(namespace, name string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}

	err := s.client.Get(context.TODO(),
		types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		}, secret)

	if err != nil {
		return nil, err
	}

	return secret, err
}
