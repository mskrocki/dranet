package podnet

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/multi-network-api/apis/v1alpha1"

	"k8s.io/cloud-provider-gcp/pkg/controllermetrics"
	"k8s.io/klog/v2"
	podnetworkclientset "sigs.k8s.io/multi-network-api/pkg/client/clientset/versioned"
	podnetworkfactory "sigs.k8s.io/multi-network-api/pkg/client/informers/externalversions"
	podnetworkinformer "sigs.k8s.io/multi-network-api/pkg/client/informers/externalversions/apis/v1alpha1"
)

const (
	finalizer     = "k8s.io/mn-dranet"
	workqueueName = "podnetwork"
)

type DranetData struct {
	Name string `json:"name,omitempty"`
}

type Controller struct {
	podNetworkInformer        podnetworkinformer.PodNetworkInformer
	podNetworkClientset       podnetworkclientset.Interface
	queue                     workqueue.RateLimitingInterface
	podNetworkInformerFactory podnetworkfactory.SharedInformerFactory

	podNetworks map[string]*DranetData
	lock        *sync.Mutex
}

func NewPodNetworkController(
	podNetworkInformer podnetworkinformer.PodNetworkInformer,
	podNetworkClientset podnetworkclientset.Interface,
	podNetworkInformerFactory podnetworkfactory.SharedInformerFactory,
	lock *sync.Mutex,
	podNetworksMap map[string]*DranetData,
) *Controller {

	c := &Controller{
		podNetworkInformerFactory: podNetworkInformerFactory,
		podNetworkClientset:       podNetworkClientset,
		podNetworkInformer:        podNetworkInformer,
		queue:                     workqueue.NewRateLimitingQueueWithConfig(workqueue.DefaultControllerRateLimiter(), workqueue.RateLimitingQueueConfig{Name: workqueueName}),

		lock:        lock,
		podNetworks: podNetworksMap,
	}

	podNetworkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				c.queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				c.queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				c.queue.Add(key)
			}
		},
	})
	return c
}

func (c *Controller) Run(numWorkers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	defer c.queue.ShutDown()

	klog.Infof("Starting podNetwork controller")
	defer klog.Infof("Shutting down podNetwork controller")

	c.podNetworkInformerFactory.Start(stopCh)

	if !cache.WaitForNamedCacheSync("podNetwork", stopCh, c.podNetworkInformer.Informer().HasSynced) {
		return
	}

	for i := 0; i < numWorkers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-stopCh
}

// worker pattern adapted from https://github.com/kubernetes/client-go/blob/master/examples/workqueue/main.go
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *Controller) processNextItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	defer c.queue.Done(key)

	err := c.reconcile(ctx, key.(string))
	c.handleErr(err, key)
	return true
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < 5 {
		klog.Warningf("Error while processing PodNetwork object, retrying %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	utilruntime.HandleError(err)
	klog.Errorf("Dropping PodNetwork %q out of the queue: %v", key, err)
	controllermetrics.WorkqueueDroppedObjects.WithLabelValues(workqueueName).Inc()
}

func addFinalizerInPlace(network *v1alpha1.PodNetwork) {
	finalizers := network.GetFinalizers()
	for _, f := range finalizers {
		if f == finalizer {
			return
		}
	}

	network.SetFinalizers(append(finalizers, finalizer))
}

func removeFinalizerInPlace(network *v1alpha1.PodNetwork) {
	finalizers := network.GetFinalizers()
	for i, f := range finalizers {
		if f == finalizer {
			finalizers = append(finalizers[:i], finalizers[i+1:]...)
			break
		}
	}

	network.SetFinalizers(finalizers)
}

func (c *Controller) reconcile(ctx context.Context, key string) error {
	network, err := c.podNetworkInformer.Lister().Get(key)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}

	klog.Infof("reconciling %s", network.Name)

	err = c.syncPodNetwork(ctx, network)

	//meta.SetStatusCondition(&network.Status.Conditions, metav1.Condition{
	//	Type:               "Ready",
	//	Status:             metav1.ConditionTrue,
	//	Reason:             "Ready",
	//	ObservedGeneration: network.Generation,
	//})

	//if !reflect.DeepEqual(originalNetwork.ObjectMeta, network.ObjectMeta) {
	//	if updateErr := c.updatePodNetwork(ctx, network); updateErr != nil {
	//		err = multierror.Append(updateErr, err)
	//	}
	//}
	//if !reflect.DeepEqual(originalNetwork.Status, network.Status) {
	//	if updateErr := c.updatePodNetworkStatus(ctx, network); updateErr != nil {
	//		err = multierror.Append(updateErr, err)
	//	}
	//}

	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) syncPodNetwork(ctx context.Context, network *v1alpha1.PodNetwork) error {
	if network.DeletionTimestamp != nil {
		return c.handleDelete(ctx, network)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	data := &DranetData{}
	if network.Spec.Parameters.Raw != nil {
		err := json.Unmarshal(network.Spec.Parameters.Raw, data)
		if err != nil {
			klog.Errorf("failed to json.Unmarshal Parameters: %v", err)
			return nil
		}
	}

	klog.Infof("PodNetwork data: %v", data)
	c.podNetworks[network.Name] = data

	addFinalizerInPlace(network)
	return nil
}

func (c *Controller) handleDelete(ctx context.Context, network *v1alpha1.PodNetwork) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.podNetworks, network.Name)

	removeFinalizerInPlace(network)
	return nil
}

//func (c *Controller) updatePodNetwork(ctx context.Context, network *v1alpha1.PodNetwork) error {
//	_, err := c.podNetworkClientset.MultinetworkV1alpha1().PodNetworks().Update(ctx, network, metav1.UpdateOptions{})
//	if err != nil {
//		return fmt.Errorf("failed to update PodNetwork: %w", err)
//	}
//	return nil
//}
//
//func (c *Controller) updatePodNetworkStatus(ctx context.Context, network *v1alpha1.PodNetwork) error {
//	_, err := c.podNetworkClientset.MultinetworkV1alpha1().PodNetworks().UpdateStatus(ctx, network, metav1.UpdateOptions{})
//	if err != nil {
//		return fmt.Errorf("failed to update PodNetwork Status: %w", err)
//	}
//	return nil
//}
