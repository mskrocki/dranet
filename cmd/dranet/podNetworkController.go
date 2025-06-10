package main

import (
	"context"
	"time"

	"github.com/google/dranet/pkg/podnet"
	"k8s.io/client-go/rest"
	podnetworkclientset "sigs.k8s.io/multi-network-api/pkg/client/clientset/versioned"
	podnetworkfactory "sigs.k8s.io/multi-network-api/pkg/client/informers/externalversions"
)

func startPodNetworkController(ctx context.Context, kubeConfig *rest.Config, pnShare *podnet.PNShare) error {
	networkClient, err := podnetworkclientset.NewForConfig(kubeConfig)
	if err != nil {
		return err
	}
	nwInfFactory := podnetworkfactory.NewSharedInformerFactory(networkClient, 0*time.Second)
	nwInformer := nwInfFactory.Multinetwork().V1alpha1().PodNetworks()

	podNetworkController := podnet.NewPodNetworkController(
		nwInformer,
		networkClient,
		nwInfFactory,
		pnShare,
	)

	go podNetworkController.Run(1, ctx.Done())
	return nil
}
