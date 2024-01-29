package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"bou.ke/monkey"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
	"github.com/tonglil/buflogr"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Test garbage collection for snapshots", func() {
	var buf bytes.Buffer
	var logger logr.Logger

	BeforeEach(func() {
		buf.Reset()
		logger = buflogr.NewWithBuffer(&buf)
	})

	Describe("Test getSnapshotsForRemoval", func() {
		It("Handles no snapshots", func() {

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{}
			output := getSnapshotsForRemoval(cl, candidates, 2, 1, logger)

			Expect(output).To(BeEmpty())
		})

		It("No PR snapshots, some non-PR snapshots. Not enough to GCed", func() {
			currentTime := time.Now()
			newerSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "some-event",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerSnap, *olderSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{*newerSnap, *olderSnap}
			output := getSnapshotsForRemoval(cl, candidates, 2, 2, logger)

			Expect(output).To(BeEmpty())
		})

		It("Some PR snapshots, no non-PR snapshots. some to be GCed", func() {
			currentTime := time.Now()
			newerSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "older-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerSnap, *olderSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{*newerSnap, *olderSnap}
			output := getSnapshotsForRemoval(cl, candidates, 1, 2, logger)

			Expect(output).To(HaveLen(1))
			Expect(output[0].Name).To(Equal("older-snapshot"))
		})

		It("Snapshots of both types to be GCed", func() {
			currentTime := time.Now()
			newerPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "older-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "newer-non-pr-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerPRSnap, *olderPRSnap,
							*newerNonPRSnap, *olderNonPRSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{
				*newerPRSnap, *olderPRSnap, *newerNonPRSnap, *olderNonPRSnap,
			}
			output := getSnapshotsForRemoval(cl, candidates, 1, 1, logger)

			Expect(output).To(HaveLen(2))
			Expect(output[0].Name).To(Equal("older-non-pr-snapshot"))
			Expect(output[1].Name).To(Equal("older-pr-snapshot"))
		})
	})

	Describe("Test garbageCollectSnapshots", func() {
		It("Garbage collect snapshots from multiple namespaces", func() {

			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			ns2 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns2",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "not-a-tenant",
					},
				},
			}
			ns3 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns3",
				},
			}
			ns4 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns4",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			rel1 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel1",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "keep11",
				},
			}
			rel2 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel2",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "keep21",
				},
			}

			bind3 := &applicationapiv1alpha1.SnapshotEnvironmentBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bind3",
					Namespace: "ns1",
				},
				Spec: applicationapiv1alpha1.SnapshotEnvironmentBindingSpec{
					Snapshot: "keep31",
				},
			}
			bind4 := &applicationapiv1alpha1.SnapshotEnvironmentBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bind4",
					Namespace: "ns1",
				},
				Spec: applicationapiv1alpha1.SnapshotEnvironmentBindingSpec{
					Snapshot: "keep41",
				},
			}

			currentTime := time.Now()
			keep11 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep11",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep21 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep21",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep31 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep31",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep41 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep41",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns1",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns1",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "newer-non-pr-snapshot",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}

			newerPRSnapNs2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns2",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnapNs2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns2",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerNonPRSnapNs3 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "newer-non-pr-snapshot",
					Namespace:         "ns3",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnapNs3 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					Namespace:         "ns3",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}
			newerPRSnapNs4 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns4",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnapNs4 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns4",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&core.NamespaceList{
						Items: []core.Namespace{*ns1, *ns2, *ns3, *ns4},
					},
					&releasev1alpha1.ReleaseList{
						Items: []releasev1alpha1.Release{*rel1, *rel2},
					},
					&applicationapiv1alpha1.SnapshotEnvironmentBindingList{
						Items: []applicationapiv1alpha1.SnapshotEnvironmentBinding{
							*bind3, *bind4,
						},
					},
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerPRSnap, *olderPRSnap,
							*newerNonPRSnap, *olderNonPRSnap,
							*keep11, *keep21, *keep31, *keep41,
							*newerPRSnapNs2, *olderPRSnapNs2,
							*newerNonPRSnapNs3, *olderNonPRSnapNs3,
							*newerPRSnapNs4, *olderPRSnapNs4,
						},
					},
				).Build()

			var err error

			snapsBefore := &applicationapiv1alpha1.SnapshotList{}
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns1",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(8))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns2",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns3",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns4",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))

			err = garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())

			snapsAfter := &applicationapiv1alpha1.SnapshotList{}
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns1",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(6))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns2",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns3",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns4",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(1))
		})

		It("Fails if cannot list namespaces", func() {
			cl := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).Should(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(
				"no kind is registered for the type v1.NamespaceList in scheme",
			)))
		})

		It("Does not fail if getSnapshotsForNSReleases fails for namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			// using default scheme having namespace but not releases
			cl := fake.NewClientBuilder().WithLists(
				&core.NamespaceList{Items: []core.Namespace{*ns1}},
			).Build()

			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())
			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"Failed getting release snapshots.",
			))
		})

		It("Does not fail if getSnapshotsForNSBindings fails for namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			// using scheme with release, but without binding
			noBindsScheme := runtime.NewScheme()
			utilruntime.Must(clientgoscheme.AddToScheme(noBindsScheme))
			utilruntime.Must(releasev1alpha1.AddToScheme(noBindsScheme))
			cl := fake.NewClientBuilder().WithScheme(noBindsScheme).WithLists(
				&core.NamespaceList{Items: []core.Namespace{*ns1}},
			).Build()

			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())
			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"Failed getting bindings snapshots.",
			))
		})

		It("Does not fail if getUnassociatedNSSnapshots fails for namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			// Monkey-patching getUnassociatedNSSnapshots so it returns with error
			monkey.Patch(getUnassociatedNSSnapshots, func(cl client.Client,
				snapToData map[string]snapshotData,
				namespace string,
				logger logr.Logger) ([]applicationapiv1alpha1.Snapshot, error) {
				return nil, errors.New("")
			})
			cl := fake.NewClientBuilder().WithScheme(scheme).WithLists(
				&core.NamespaceList{Items: []core.Namespace{*ns1}},
			).Build()

			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())
			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"Failed getting unassociated snapshots.",
			))
		})
	})
})
