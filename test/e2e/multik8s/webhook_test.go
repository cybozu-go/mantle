package multik8s

import (
	"slices"

	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/events"
)

const annotRemoteUID = "mantle.cybozu.io/remote-uid"

var _ = Describe("webhook independence test", func() {
	It("should confirm that the webhook continues to work even when the controller is stopped", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		podName := util.GetUniqueName("pod-")

		By("setting up the environment and creating PVC on primary cluster")
		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

		By("creating MantleBackup on primary cluster")
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		By("waiting for MantleBackup to be synced to secondary cluster")
		WaitMantleBackupSynced(namespace, backupName)

		By("waiting for PVC with remote-uid annotation to be created on secondary cluster")
		Eventually(ctx, func(g Gomega) {
			pvc, err := GetPVC(SecondaryK8sCluster, namespace, pvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pvc.Annotations).NotTo(BeNil())
			g.Expect(pvc.Annotations[annotRemoteUID]).NotTo(BeEmpty())
			g.Expect(pvc.Status.Phase).To(Equal(corev1.ClaimBound))
		}).Should(Succeed())

		By("scaling down the controller deployment to 0 on secondary cluster")
		_, stderr, err := Kubectl(SecondaryK8sCluster, nil,
			"scale", "deploy", "-n", CephClusterNamespace, "mantle-controller", "--replicas=0")
		Expect(err).NotTo(HaveOccurred(), string(stderr))

		By("waiting for the controller pods to be terminated")
		Eventually(ctx, func(g Gomega) {
			pods, err := GetPodList(SecondaryK8sCluster, CephClusterNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			count := 0
			for _, pod := range pods.Items {
				if pod.Labels["app.kubernetes.io/name"] == "mantle-controller" {
					count++
				}
			}
			g.Expect(count).To(Equal(0))
		}).Should(Succeed())

		By("checking the webhook deployment is still available on secondary cluster")
		Eventually(ctx, func(g Gomega) {
			err := CheckDeploymentReady(SecondaryK8sCluster, CephClusterNamespace, "mantle-webhook")
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		By("trying to create a Pod that mounts the replicated PVC with remote-uid annotation")
		CreatePod(SecondaryK8sCluster, namespace, podName, pvcName)

		By("verifying the Pod fails to attach the volume due to webhook rejection")
		Eventually(ctx, func(g Gomega) {
			eventList, err := GetEventList(SecondaryK8sCluster, namespace)
			g.Expect(err).NotTo(HaveOccurred())
			found := false
			for _, event := range eventList.Items {
				if event.InvolvedObject.Namespace == namespace && event.InvolvedObject.Name == podName {
					if event.Reason == events.FailedAttachVolume {
						found = true

						break
					}
				}
			}
			g.Expect(found).To(BeTrue(), "expected FailedAttachVolume event not found")
		}).Should(Succeed())

		By("checking that it is allowed to create a Pod that mounts a PVC without remote-uid annotation", func() {
			podName := util.GetUniqueName("pod-")
			pvcName := util.GetUniqueName("pvc-")
			CreatePVC(ctx, SecondaryK8sCluster, namespace, pvcName)
			CreatePod(SecondaryK8sCluster, namespace, podName, pvcName)
			Eventually(ctx, func(g Gomega) {
				pods, err := GetPodList(SecondaryK8sCluster, namespace)
				g.Expect(err).NotTo(HaveOccurred())
				i := slices.IndexFunc(pods.Items, func(p corev1.Pod) bool {
					return p.Name == podName
				})
				g.Expect(i).NotTo(Equal(-1))
				g.Expect(pods.Items[i].Status.Phase).To(Equal(corev1.PodRunning))
			}).Should(Succeed())
		})

		By("scaling the controller deployment back to 1 replica on secondary cluster")
		_, stderr, err = Kubectl(SecondaryK8sCluster, nil,
			"scale", "deploy", "-n", CephClusterNamespace, "mantle-controller", "--replicas=1")
		Expect(err).NotTo(HaveOccurred(), string(stderr))

		By("waiting for the controller deployment to be ready")
		Eventually(ctx, func(g Gomega) {
			err := CheckDeploymentReady(SecondaryK8sCluster, CephClusterNamespace, "mantle-controller")
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())
	})
})
