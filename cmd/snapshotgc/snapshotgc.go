package main

import (
	"context"
	"flag"
	"os"
	"sort"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(applicationapiv1alpha1.AddToScheme(scheme))
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(releasev1alpha1.AddToScheme(scheme))
}

// Stores pointers to resources to which the snapshot is associated
type snapshotData struct {
	environmentBinding applicationapiv1alpha1.SnapshotEnvironmentBinding
	release            releasev1alpha1.Release
}

func garbageCollectSnapshots(
	cl client.Client,
	logger logr.Logger,
	prSnapshotsToKeep, nonPrSnapshotsToKeep int,
) error {
	req, _ := labels.NewRequirement(
		"toolchain.dev.openshift.com/type", selection.In, []string{"tenant"},
	)
	selector := labels.NewSelector().Add(*req)
	namespaceList := &core.NamespaceList{}
	err := cl.List(
		context.Background(),
		namespaceList,
		&client.ListOptions{LabelSelector: selector},
	)
	if err != nil {
		logger.Error(err, "Failed listing namespaces")
		return err
	}

	logger.V(1).Info("Snapshot garbage collection started...")
	for _, ns := range namespaceList.Items {
		logger.V(1).Info("Processing namespace", "namespace", ns.Name)

		snapToData := make(map[string]snapshotData)
		snapToData, err = getSnapshotsForNSReleases(cl, snapToData, ns.Name, logger)
		if err != nil {
			logger.Error(
				err, "Failed getting release snapshots. Skipping namespace "+ns.Name)
			continue
		}

		snapToData, err = getSnapshotsForNSBindings(cl, snapToData, ns.Name, logger)
		if err != nil {
			logger.Error(
				err, "Failed getting bindings snapshots. Skipping namespace "+ns.Name,
			)
			continue
		}

		var candidates []applicationapiv1alpha1.Snapshot
		candidates, err = getUnassociatedNSSnapshots(cl, snapToData, ns.Name, logger)
		if err != nil {
			logger.Error(
				err,
				"Failed getting unassociated snapshots. Skipping namespace "+ns.Name,
			)
			continue
		}

		candidates = getSnapshotsForRemoval(
			cl, candidates, prSnapshotsToKeep, nonPrSnapshotsToKeep, logger,
		)

		deleteSnapshots(cl, candidates, logger)
	}
	return nil
}

// Gets a map to allow to tell with direct lookup if a snapshot is associated with
// a release resource
func getSnapshotsForNSReleases(
	cl client.Client,
	snapToData map[string]snapshotData,
	namespace string,
	logger logr.Logger,
) (map[string]snapshotData, error) {
	releases := &releasev1alpha1.ReleaseList{}
	err := cl.List(
		context.Background(),
		releases,
		&client.ListOptions{Namespace: namespace},
	)
	if err != nil {
		logger.Error(err, "Failed to list releases")
		return nil, err
	}

	for _, release := range releases.Items {
		data, ok := snapToData[release.Spec.Snapshot]
		if !ok {
			data = snapshotData{}
		}
		data.release = release
		snapToData[release.Spec.Snapshot] = data
	}
	return snapToData, nil
}

// Gets a map to allow to tell with direct lookup if a snapshot is associated with
// a SnapshotEnvironmentBinding resource
func getSnapshotsForNSBindings(
	cl client.Client,
	snapToData map[string]snapshotData,
	namespace string,
	logger logr.Logger,
) (map[string]snapshotData, error) {
	binds := &applicationapiv1alpha1.SnapshotEnvironmentBindingList{}
	err := cl.List(
		context.Background(),
		binds,
		&client.ListOptions{Namespace: namespace},
	)
	if err != nil {
		logger.Error(err, "Failed to list bindings")
		return nil, err
	}

	for _, bind := range binds.Items {
		data, ok := snapToData[bind.Spec.Snapshot]
		if !ok {
			data = snapshotData{}
		}
		data.environmentBinding = bind
		snapToData[bind.Spec.Snapshot] = data
	}
	return snapToData, nil
}

// Gets all namespace snapshots that aren't associated with a release/binding
func getUnassociatedNSSnapshots(
	cl client.Client,
	snapToData map[string]snapshotData,
	namespace string,
	logger logr.Logger,
) ([]applicationapiv1alpha1.Snapshot, error) {
	snaps := &applicationapiv1alpha1.SnapshotList{}
	err := cl.List(
		context.Background(),
		snaps,
		&client.ListOptions{Namespace: namespace},
	)

	if err != nil {
		logger.Error(err, "Failed to list snapshots")
		return nil, err
	}

	var unAssociatedSnaps []applicationapiv1alpha1.Snapshot

	for _, snap := range snaps.Items {
		if _, found := snapToData[snap.Name]; found {
			logger.V(1).Info(
				"Skipping snapshot as it's associated with release/binding",
				"snapshot-name",
				snap.Name,
			)
			continue
		}
		unAssociatedSnaps = append(unAssociatedSnaps, snap)
	}

	return unAssociatedSnaps, nil
}

func getSnapshotsForRemoval(
	cl client.Client,
	snapshots []applicationapiv1alpha1.Snapshot,
	prSnapshotsToKeep int,
	nonPrSnapshotsToKeep int,
	logger logr.Logger,
) []applicationapiv1alpha1.Snapshot {
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[j].CreationTimestamp.Before(&snapshots[i].CreationTimestamp)
	})

	shortList := []applicationapiv1alpha1.Snapshot{}
	keptPrSnaps := 0
	keptNonPrSnaps := 0

	for _, snap := range snapshots {
		label, found := snap.GetLabels()["pac.test.appstudio.openshift.io/event-type"]
		if found && label == "pull_request" && keptPrSnaps < prSnapshotsToKeep {
			// if pr, and we did not keep enough PR snapshots -> discard from candidates
			logger.V(1).Info(
				"Discarding PR candidate",
				"snapshot-name", snap.Name,
				"pr-snapshot-kept", keptPrSnaps+1,
			)
			keptPrSnaps++
			continue
		} else if (!found || label != "pull_request") && keptNonPrSnaps < nonPrSnapshotsToKeep {
			// same for non-pr
			logger.V(1).Info(
				"Discarding non-PR candidate",
				"snapshot-name", snap.Name,
				"non-pr-snapshot-kept", keptNonPrSnaps+1,
			)
			keptNonPrSnaps++
			continue
		}
		logger.V(1).Info("Adding candidate", "snapshot-name", snap.Name)
		shortList = append(shortList, snap)
	}
	return shortList
}

// Delete snapshots determined to be garbage-collected
func deleteSnapshots(
	cl client.Client,
	snapshots []applicationapiv1alpha1.Snapshot,
	logger logr.Logger,
) {
	for _, snap := range snapshots {
		err := cl.Delete(context.Background(), &snap)
		if err != nil {
			logger.V(1).Info("Failed to delete snapshot:", "error", err.Error())
		}
	}
}

func main() {
	var prSnapshotsToKeep, nonPrSnapshotsToKeep int
	flag.IntVar(
		&prSnapshotsToKeep,
		"pr-snapshots-to-keep",
		5,
		"Number of PR snapshots to keep after garbage collection",
	)
	flag.IntVar(
		&nonPrSnapshotsToKeep,
		"non-pr-snapshots-to-keep",
		5,
		"Number of non-PR snapshots to keep after garbage collection",
	)

	cl, _ := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	zc := zap.NewProductionConfig()
	z, _ := zc.Build()
	logger := zapr.NewLogger(z)
	err := garbageCollectSnapshots(cl, logger, prSnapshotsToKeep, nonPrSnapshotsToKeep)
	if err != nil {
		logger.Error(err, "Snapshots garbage collection failed")
		os.Exit(1)
	}
}
