# End-to-End Tests for Mantle

## Requirements

To set up the test environment, you'll need Minikube with the KVM2 driver enabled, so make sure to enable KVM2 beforehand. Don't worry about installing Minikube yourself; it will be installed automatically.

## Usage

First, install the necessary software by running:
```
cd test/e2e
make setup
```

Next, you can start the tests:

- To run the tests using a single Kubernetes cluster:
    ```
    make test
    ```
- To run the tests using multiple Kubernetes clusters:
    ```
    make test-multiple-k8s-clusters
    ```

These Makefile targets are designed to be idempotent, so if you need to re-run the tests after making changes to the code, simply run the appropriate target again.

Finally, to clean up your environment, run:
```
make clean
```
