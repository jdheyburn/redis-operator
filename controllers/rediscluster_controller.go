package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	redisv1beta1 "github.com/jdheyburn/redis-operator/api/v1beta1"
	"github.com/jdheyburn/redis-operator/client/k8s"
	"github.com/jdheyburn/redis-operator/client/redis"
	"github.com/jdheyburn/redis-operator/controllers/clustercache"
	"github.com/jdheyburn/redis-operator/controllers/service"
	"github.com/jdheyburn/redis-operator/util"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const ReconcileTime = 60 * time.Second

var (
	controllerFlagSet *pflag.FlagSet
	// maxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run. Defaults to 4.
	maxConcurrentReconciles int
	// reconcileTime is the delay between reconciliations. Defaults to 60s.
	reconcileTime int

	log = ctrl.Log.WithName("controller_rediscluster")
)

func init() {
	controllerFlagSet = pflag.NewFlagSet("controller", pflag.ExitOnError)
	controllerFlagSet.IntVar(&maxConcurrentReconciles, "ctr-maxconcurrent", 4, "the maximum number of concurrent Reconciles which can be run. Defaults to 4.")
	controllerFlagSet.IntVar(&reconcileTime, "ctr-reconciletime", 60, "")
}

func FlagSet() *pflag.FlagSet {
	return controllerFlagSet
}

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new RedisCluster Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
// func Add(mgr manager.Manager) error {
// 	return add(mgr, newReconciler(mgr))
// }

// newReconciler returns a new reconcile.Reconciler
func newHandler(mgr manager.Manager) *RedisClusterHandler {
	log.Info("Creating new handler")

	// Create kubernetes service.
	k8sService := k8s.New(mgr.GetClient(), log)

	// Create the redis clients
	redisClient := redis.New()

	// Create internal services.
	rcService := service.NewRedisClusterKubeClient(k8sService, log)
	rcChecker := service.NewRedisClusterChecker(k8sService, redisClient, log)
	rcHealer := service.NewRedisClusterHealer(k8sService, redisClient, log)

	return &RedisClusterHandler{
		k8sServices: k8sService,
		rcService:   rcService,
		rcChecker:   rcChecker,
		rcHealer:    rcHealer,
		metaCache:   new(clustercache.MetaMap),
		eventsCli:   k8s.NewEvent(mgr.GetEventRecorderFor("redis-operator"), log),
		logger:      log,
	}

	// return &RedisClusterReconciler{client: mgr.GetClient(), scheme: mgr.GetScheme(), handler: handler}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Complete(r); err != nil {
		return err
	}

	r.handler = newHandler(mgr)

	return nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
// func add(mgr manager.Manager, r reconcile.Reconciler) error {
// 	// Create a new controller
// 	c, err := controller.New("rediscluster-controller", mgr, controller.Options{Reconciler: r,
// 		MaxConcurrentReconciles: maxConcurrentReconciles})
// 	if err != nil {
// 		return err
// 	}

// 	Pred := predicate.Funcs{
// 		UpdateFunc: func(e event.UpdateEvent) bool {
// 			// returns false if redisCluster is ignored (not managed) by this operator.
// 			if !shoudManage(e.MetaNew) {
// 				return false
// 			}
// 			log.WithValues("namespace", e.MetaNew.GetNamespace(), "name", e.MetaNew.GetName()).V(5).Info("Call UpdateFunc")
// 			// Ignore updates to CR status in which case metadata.Generation does not change
// 			if e.MetaOld.GetGeneration() != e.MetaNew.GetGeneration() {
// 				log.WithValues("namespace", e.MetaNew.GetNamespace(), "name", e.MetaNew.GetName()).
// 					Info("Generation change return true", "old", e.ObjectOld, "new", e.ObjectNew)
// 				return true
// 			}
// 			return false
// 		},
// 		DeleteFunc: func(e event.DeleteEvent) bool {
// 			// returns false if redisCluster is ignored (not managed) by this operator.
// 			if !shoudManage(e.Meta) {
// 				return false
// 			}
// 			log.WithValues("namespace", e.Meta.GetNamespace(), "name", e.Meta.GetName()).Info("Call DeleteFunc")
// 			metrics.ClusterMetrics.DeleteCluster(e.Meta.GetNamespace(), e.Meta.GetName())
// 			// Evaluates to false if the object has been confirmed deleted.
// 			return !e.DeleteStateUnknown
// 		},
// 		CreateFunc: func(e event.CreateEvent) bool {
// 			// returns false if redisCluster is ignored (not managed) by this operator.
// 			if !shoudManage(e.Meta) {
// 				return false
// 			}
// 			log.WithValues("namespace", e.Meta.GetNamespace(), "name", e.Meta.GetName()).Info("Call CreateFunc")
// 			return true
// 		},
// 	}

// 	// Watch for changes to primary resource RedisCluster
// 	err = c.Watch(&source.Kind{Type: &redisv1beta1.RedisCluster{}}, &handler.EnqueueRequestForObject{}, Pred)
// 	if err != nil {
// 		return err
// 	}

// 	//ownerPred := predicate.Funcs{
// 	//	UpdateFunc: func(e event.UpdateEvent) bool {
// 	//		return false
// 	//	},
// 	//	DeleteFunc: func(e event.DeleteEvent) bool {
// 	//		log.WithValues("namespace", e.Meta.GetNamespace(), "kind", e.Object.GetObjectKind().GroupVersionKind().Kind, "name", e.Meta.GetName()).
// 	//			V(3).Info("dependent resource delete")
// 	//		// Evaluates to false if the object has been confirmed deleted.
// 	//		return !e.DeleteStateUnknown
// 	//	},
// 	//	CreateFunc: func(e event.CreateEvent) bool {
// 	//		return false
// 	//	},
// 	//}
// 	//
// 	//// Watch for changes to redisCluster StatefulSet secondary resources
// 	//err = c.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
// 	//	IsController: true,
// 	//	OwnerType:    &redisv1beta1.RedisCluster{},
// 	//}, ownerPred)
// 	//if err != nil {
// 	//	return err
// 	//}
// 	//
// 	//// Watch for changes to redisCluster Deployment secondary resources
// 	//err = c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
// 	//	IsController: true,
// 	//	OwnerType:    &redisv1beta1.RedisCluster{},
// 	//}, ownerPred)
// 	//if err != nil {
// 	//	return err
// 	//}

// 	return nil
// }

// var _ reconcile.Reconciler = &RedisClusterReconciler{}

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client.Client
	Scheme *runtime.Scheme

	handler *RedisClusterHandler
}

// Reconcile reads that state of the cluster for a RedisCluster object and makes changes based on the state read
// and what is in the RedisCluster.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)
	reqLogger.Info("Reconciling RedisCluster")

	// Fetch the RedisCluster instance
	instance := &redisv1beta1.RedisCluster{}
	err := r.Client.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		reqLogger.Info("Error when retrieving namespaced name", "namespacedName", req.NamespacedName)
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			reqLogger.Info("RedisCluster delete")
			instance.Namespace = req.NamespacedName.Namespace
			instance.Name = req.NamespacedName.Name
			r.handler.metaCache.Del(instance)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	reqLogger.V(5).Info(fmt.Sprintf("RedisCluster Spec:\n %+v", instance))

	if err = r.handler.Do(instance); err != nil {
		if err.Error() == needRequeueMsg {
			return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
		}
		reqLogger.Error(err, "Reconcile handler")
		return ctrl.Result{}, err
	}

	if err = r.handler.rcChecker.CheckSentinelReadyReplicas(instance); err != nil {
		reqLogger.Info(err.Error())
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: time.Duration(reconcileTime) * time.Second}, nil
}

func shoudManage(meta v1.Object) bool {
	if v, ok := meta.GetAnnotations()[util.AnnotationScope]; ok {
		if util.IsClusterScoped() {
			return v == util.AnnotationClusterScoped
		}
	} else {
		if !util.IsClusterScoped() {
			return true
		}
	}
	return false
}
