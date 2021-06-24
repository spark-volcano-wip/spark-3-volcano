package org.apache.spark.volcano.dsl;

import io.fabric8.kubernetes.api.model.volcano.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.volcano.batch.Job;
import io.fabric8.kubernetes.api.model.volcano.batch.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class VolcanoJobOperationsImpl extends HasMetadataOperation<Job, JobList, DoneableJob, Resource<Job, DoneableJob>> {

    public VolcanoJobOperationsImpl(OkHttpClient client, Config config) {
        this(client, config, null);
    }

    public VolcanoJobOperationsImpl(OkHttpClient client, Config config, String namespace) {
        this(new OperationContext()
                .withOkhttpClient(client)
                .withConfig(config)
                .withNamespace(namespace)
                .withPropagationPolicy(DEFAULT_PROPAGATION_POLICY));
    }

    public VolcanoJobOperationsImpl(OperationContext context) {
        super(context.withApiGroupName("batch.volcano.sh")
                .withApiGroupVersion("v1alpha1")
                .withPlural("jobs"));

        this.type = Job.class;
        this.listType = JobList.class;
        this.doneableType = DoneableJob.class;
    }

    @Override
    public VolcanoJobOperationsImpl newInstance(OperationContext context) {
        return new VolcanoJobOperationsImpl(context);
    }

    public Job updateReplicas(Job job, int replicas) {
        // todo find the id of the task
        //        for (TaskSpec task : job.getSpec().getTasks()) {
        //            task.getName().contains("executor");
        //        }

        HashMap<String, Object> operation = new HashMap<>();
        operation.put("op", "replace");
        operation.put("path", "/spec/tasks/0/replicas");
        operation.put("value", replicas);

        return this.patch(job, Arrays.asList(operation));
    }

    protected <T> T handlePatch(T current, List<Map<String, Object>> parameters, Class<T> type) throws ExecutionException, InterruptedException, IOException {
        RequestBody body = RequestBody.create(JSON_PATCH, JSON_MAPPER.writeValueAsString(parameters));
        Request.Builder requestBuilder = new Request.Builder().patch(body).url(getResourceUrl(checkNamespace(current), checkName(current)));
        return handleResponse(requestBuilder, type, Collections.<String, String>emptyMap());
    }

    public Job patch(Job job, List<Map<String, Object>> parameters) {
        String fixedResourceVersion = getResourceVersion();
        Exception caught = null;
        int maxTries = 1;
        for (int i = 0; i < maxTries; i++) {
            try {
                return handlePatch(job, parameters, Job.class);
            } catch (KubernetesClientException e) {
                caught = e;
                // Only retry if there's a conflict and using dynamic resource version - this is normally to do with resource version & server updates.
                if (e.getCode() != 409 || fixedResourceVersion != null) {
                    break;
                }
                if (i < (maxTries - 1)) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e1) {
                        // Ignore this... would only hide the proper exception
                        // ...but make sure to preserve the interrupted status
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Exception e) {
                caught = e;
            }
        }
        throw KubernetesClientException.launderThrowable(forOperationType("patch"), caught);
    }
}