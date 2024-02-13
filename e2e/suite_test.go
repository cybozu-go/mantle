package e2e

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)


var (
	//go:embed testdata/pvc-pod-template.yaml
	dummyPVCPodTemplate string

	//go:embed testdata/rook-pool-sc-template.yaml
	dummyRookPoolSCTemplate string

	//go:embed testdata/rbdpvcbackup-template.yaml
	dummyRBDPVCBackupTemplate string
)

const (
	pvcName = "rbd-pvc"
	podName = "test-pod"
	rbdPVCBackupName = "rbdpvcbackup-test"
)

func execAtLocal(cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	command := exec.Command(cmd, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	if len(input) != 0 {
		command.Stdin = bytes.NewReader(input)
	}

	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func kubectl(args ...string) ([]byte, []byte, error) {
	return execAtLocal("kubectl", nil, args...)
}

func kubectlWithInput(input []byte, args ...string) ([]byte, []byte, error) {
	return execAtLocal("kubectl", input, args...)
}

var operatorNamespace = "rook-ceph"

func TestMtest(t *testing.T) {
	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)

	RunSpecs(t, "rbd backup system test")
}

var _ = BeforeSuite(func() {
	By("[BeforeSuite] Waiting for rook to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", operatorNamespace, "get", "deploy", "rook-ceph-operator", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return errors.New("rook operator is not available yet")
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Waiting for ceph cluster to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", operatorNamespace, "get", "deploy", "rook-ceph-osd-0", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return errors.New("osd.0 is not available yet")
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Creating Rook Pool and SC")
	Eventually(func() error {
		manifest := fmt.Sprintf(dummyRookPoolSCTemplate, operatorNamespace, operatorNamespace, operatorNamespace, operatorNamespace)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", operatorNamespace, "-f", "-")
		if err != nil {
			return err
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Creating PVC and Pod")
	Eventually(func() error {
		manifest := fmt.Sprintf(dummyPVCPodTemplate, pvcName, podName, pvcName)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", operatorNamespace, "-f", "-")
		if err != nil {
			return err
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Waiting for PVC to get bound")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", operatorNamespace, "get", "pvc", pvcName, "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
		}

		var pvc corev1.PersistentVolumeClaim
		err = yaml.Unmarshal(stdout, &pvc)
		if err != nil {
			return err
		}

		if pvc.Status.Phase != "Bound" {
			return errors.New("PVC is not bound yet")
		}

		return nil
	}).Should(Succeed())
})

var _ = Describe("rbd backup system", func() {
	It("should create rbdpvcbackup resource", func() {
		fmt.Fprintln(os.Stderr, "TODO")

		By("Creating RBDPVCBackup")
		manifest := fmt.Sprintf(dummyRBDPVCBackupTemplate, rbdPVCBackupName, rbdPVCBackupName, pvcName)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred())

		// TODO: confirm the existence of snapshot
		By("Waiting for RBD snapshot to be created")
		// Eventually(func() error {
		// 	stdout, stderr, err := kubectl("-n", operatorNamespace, "get", "pvc", pvcName, "-o", "json")
		// 	if err != nil {
		// 		return fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
		// 	}
	
		// 	var pvc corev1.PersistentVolumeClaim
		// 	err = yaml.Unmarshal(stdout, &pvc)
		// 	if err != nil {
		// 		return err
		// 	}
	
		// 	if pvc.Status.Phase != "Bound" {
		// 		return errors.New("PVC is not bound yet")
		// 	}
	
		// 	return nil
		// }).Should(Succeed())

		// TODO: delete the
		/*
			wg := sync.WaitGroup{}
			ctx, cancel := context.WithCancel(context.Background())
			defer func() {
				cancel()
				wg.Wait()
			}()
				_, _, err := kubectlWithInput(dummyStorageClassYaml, "apply", "-f", "-")
				Expect(err).NotTo(HaveOccurred())

				_, _, err = kubectl("rollout", "restart", "-n", ns, "deploy/pie")
				Expect(err).NotTo(HaveOccurred())

				err = portForward(ctx, &wg, ns, "svc/pie", "8080:8080")
				Expect(err).NotTo(HaveOccurred())

				stdout, _, err := kubectl("get", "node", "-o=jsonpath={.items[*].metadata.name}")
				Expect(err).NotTo(HaveOccurred())
				nodeLabelKey := "node"
				nodeLabelValue := string(stdout)
				nodeLabelPair := io_prometheus_client.LabelPair{Name: &nodeLabelKey, Value: &nodeLabelValue}

				standardSCLabelKey := "storage_class"
				standardSCLabelValue := "standard"
				standardSCLabelPair := io_prometheus_client.LabelPair{Name: &standardSCLabelKey, Value: &standardSCLabelValue}

				dummySCLabelKey := "storage_class"
				dummySCLabelValue := "dummy"
				dummySCLabelPair := io_prometheus_client.LabelPair{Name: &dummySCLabelKey, Value: &dummySCLabelValue}

				onTimeLabelKey := "on_time"
				trueValue := "true"
				falseValue := "false"
				onTimeTrueLabelPair := io_prometheus_client.LabelPair{Name: &onTimeLabelKey, Value: &trueValue}
				onTimeFalseLabelPair := io_prometheus_client.LabelPair{Name: &onTimeLabelKey, Value: &falseValue}

				succeedLabelKey := "succeed"
				succeedTrueLabelPair := io_prometheus_client.LabelPair{Name: &succeedLabelKey, Value: &trueValue}

				Eventually(func(g Gomega) {
					resp, err := http.Get("http://localhost:8080/metrics")
					g.Expect(err).NotTo(HaveOccurred())
					defer resp.Body.Close()

					var parser expfmt.TextParser
					metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
					g.Expect(err).NotTo(HaveOccurred())

					By("checking latency metrics have node and storage_class labels and the storage_class label value is standard")
					for _, metricName := range []string{
						"pie_io_write_latency_seconds",
						"pie_io_read_latency_seconds",
					} {
						g.Expect(metricName).Should(BeKeyOf(metricFamilies))
						for _, metric := range metricFamilies[metricName].Metric {
							g.Expect(metric.Label).Should(ContainElement(&nodeLabelPair))
							g.Expect(metric.Label).Should(ContainElement(&standardSCLabelPair))
						}
					}

					By("checking pie_create_probe_total have on_time=true for standard SC or on_time=false for dummy SC")
					g.Expect("pie_create_probe_total").Should(BeKeyOf(metricFamilies))
					metrics := metricFamilies["pie_create_probe_total"].Metric
					g.Expect(metrics).Should(ContainElement(HaveField("Label", ContainElement(&standardSCLabelPair))))
					g.Expect(metrics).Should(ContainElement(HaveField("Label", ContainElement(&dummySCLabelPair))))
					for _, metric := range metrics {
						for _, label := range metric.Label {
							switch {
							case reflect.DeepEqual(label, &standardSCLabelPair):
								g.Expect(metric.Label).Should(ContainElement(&onTimeTrueLabelPair))
							case reflect.DeepEqual(label, &dummySCLabelPair):
								g.Expect(metric.Label).Should(ContainElement(&onTimeFalseLabelPair))
							}
						}
					}

					By("checking pie_performance_probe_total with succeed=true is more than zero for standard SC")
					g.Expect("pie_performance_probe_total").Should(BeKeyOf(metricFamilies))
					metrics = metricFamilies["pie_performance_probe_total"].Metric
					g.Expect(metrics).Should(ContainElement(HaveField("Label", ContainElement(&standardSCLabelPair))))
					for _, metric := range metrics {
						for _, label := range metric.Label {
							if reflect.DeepEqual(label, &standardSCLabelPair) {
								g.Expect(metric.Label).Should(ContainElement(&succeedTrueLabelPair))
								g.Expect(metric.Counter).ShouldNot(BeZero())
							}
						}
					}
				}).Should(Succeed())
		*/
	})

	It("should delete rbdbackup resource", func() {
		fmt.Fprintln(os.Stderr, "TODO")
	})
})
