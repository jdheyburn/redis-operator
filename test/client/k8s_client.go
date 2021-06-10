package client

// func NewK8sClient(config *rest.Config) (client.Client, error) {
// 	scheme := scheme.Scheme
// 	err := apis.AddToScheme(scheme)

// 	mapper, err := apiutil.NewDiscoveryRESTMapper(config)
// 	if err != nil {
// 		return nil, err
// 	}
// 	options := client.Options{
// 		Scheme: scheme,
// 		Mapper: mapper,
// 	}
// 	cli, err := client.New(config, options)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return cli, nil
// }
