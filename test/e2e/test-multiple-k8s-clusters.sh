#!/usr/bin/bash -xeu

set -o pipefail

# Exits with an "unbound variable" error if one of the following environment
# variables is undefined, thanks to "-u" option to bash.
echo "${MINIKUBE}"
echo "${MINIKUBE_HOME}"
echo "${MINIKUBE_PROFILE_PRIMARY}"
echo "${MINIKUBE_PROFILE_SECONDARY}"

cat <<EOS | ${MINIKUBE} -p ${MINIKUBE_PROFILE_PRIMARY} kubectl -- apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.27.0
        ports:
        - containerPort: 80
---
apiVersion: v1
kind: Service
metadata:
  name: nginx
spec:
  type: NodePort
  ports:
  - port: 8080
    targetPort: 80
    protocol: TCP
    name: http
  selector:
    app: nginx
EOS
${MINIKUBE} -p ${MINIKUBE_PROFILE_PRIMARY} kubectl -- wait --for=jsonpath='{.status.readyReplicas}'=1 deploy/nginx

${MINIKUBE} service list -p ${MINIKUBE_PROFILE_PRIMARY}
URL=$(${MINIKUBE} service list -p ${MINIKUBE_PROFILE_PRIMARY} -o json | jq -r '.[].URLs | select(. | length > 0)[]' | head -1)

# Exits with an errornous exit code if curl fails, thanks to "-e" option to bash.
${MINIKUBE} -p ${MINIKUBE_PROFILE_SECONDARY} kubectl -- exec -it -n rook-ceph deploy/rook-ceph-tools -- curl -vvv ${URL} > /dev/null
