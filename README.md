# Volcano project

This repository develops on top of the original Spark project to enable Spark to use [Volcano](https://volcano.sh/en/docs/schduler_introduction/) as it's resource manager when running on Kubernetes.

## Setup Local Development Environment

### 1. Setup IntelliJ and Minikube

Follow the instructions in [minikube-setup](./minikube-setup.md) to check out the 3.1.1 version of spark and set up Minikube to be able to run ``spark-submit`` locally against minikube and remote debug in IntelliJ.

Before you can do any development, you need to compile the Spark project first. Check [Useful Commands](#useful_commands) section for commands you can use to build the Spark project. You can also run
```shell
./build/mvn -Pkubernetes -DskipTests clean package
```
to download all the packages and build the project.

Read more about building Spark with Kubernetes support [here](https://spark.apache.org/docs/latest/building-spark.html#building-with-kubernetes-support).

### 3. Configure Sbt

Execute as environment variable
```shell
export SBT_OPS="-Dsbt.override.build.repos=true $SBT_OPS"
```

For continuous compilation.
```shell
./build/sbt -Pkubernetes -DskipTests -Dscalastyle.failOnViolation=false

# run ~ package in the console
```

### 4. Import Project

If you are using Intellj, instead of importing the whole Spark project, you should only import the `resource-managers/kubernetes` project. This will make sure that all the declarations can be found successfully. Don't forget to check `kubernetes` in the maven profiles menu.

## Setup Local Testing Environment

### 1. Install Volcano

### Prerequisites

- A running Kubernetes cluster of V1.13 or later, for example, Minikube or Kubernetes on Docker Desktop.
- The latest version of Volcano. Download it from [here](https://github.com/volcano-sh/volcano/releases).

### Create YAML file for deployment

You can use the YAML file in the `installer` folder. Or you can generate your own YAML file.

Under the Volcano root directory, run
```shell
make generate-yaml
```
to generate the YAML file used for install Volcano on Kubernetes cluster. You can find the outout file in `_output/release` folder.

Make sure that you have the correct configration for Volcano scheduler in your YAML file.

The ConfigMap should look like this:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |-
    actions: "reclaim, enqueue, allocate, backfill"
    tiers:
    - plugins:
      - name: conformance
      - name: drf
        enableHierarchy: true
      - name: predicates
      - name: nodeorder
      - name: binpack
```

There is an example yaml file in the `helper` folder with the desired configuration. You can also download that one and use it for installing Volcano. You might want to change the image used in the yaml file.

Then run
```shell
kubectl apply -f <your-volcano-yaml>
```
to install Volcano scheduler on your Kubernetes cluster.

### 2. Build vcctl

`vcctl` is the Volcano CLI. This step is not really necessary since you can also use `kubectl` to monitor your cluster. But for jobs created using Volcano scheduler, `vcctl` is recommended.

Under the Volcano root folder, run
```shell
make vcctl
```
to build the CLI.

The executable file is created under `_output/bin`. You can add it to your `PATH` to make it executable from everywhere in your system.

### Build Spark project

At the beginning of your development/testing, you need to build the Spark project once.

Under Spark root directory, run
```shell
./build/mvn -Pkubernetes -DskipTests clean package
```
to build the project with Kubernetes support and create all the jars. You can also use `sbt` for this step.

Then you can do some development and change some code.

After changing the code, run
```shell
./build/mvn -Pkubernetes -DskipTests package
```
for incremental MAVEN build. It will repack the jars with changes in your code. You can also achieve incremental build with `sbt`.

### Build Spark image

Under Spark root directory, run
```shell
./bin/docker-image-tool.sh -t <your-tag> build
```
to build your Spark image.

If you are using Minikube, make sure you run
```shell
minikube docker-env
```
to shift your docker environment to Minikube.

### Run spark-submit to test your changes

Under Spark root directory, run
```shell
./bin/spark-submit --master k8s://<your-k8s-cluster-url>.<your-k8s-cluster-port> \
  --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.container.image=spark:<your-tag> \
  --conf spark.kubernetes.file.upload.path=examples \
  --conf spark.kubernetes.volcano.enabled=true \ # this line enable Volcano scheduler
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar
```

After submitting your Spark job, you can run
```shell
vcctl job list
```
to get all the jobs running with Volcano scheduler.

## Useful Commands <a name="useful_commands"></a>

You might have to build all the spark modules before you can test the rest of the software

Disable linting.
```shell
export NOLINT_ON_COMPILE=true
```

Build Spark with Kubernetes support.
```shell
./build/mvn -Pkubernetes -DskipTests clean package
```

Build Spark submodule `spark-kubernetes`.
```shell
# maybe add -pKubernetes
./build/mvn -pl :spark-kubernetes_2.12 clean install
```


