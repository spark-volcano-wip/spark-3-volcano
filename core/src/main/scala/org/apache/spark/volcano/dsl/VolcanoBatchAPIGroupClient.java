package org.apache.spark.volcano.dsl;

import io.fabric8.kubernetes.api.model.volcano.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.volcano.batch.Job;
import io.fabric8.kubernetes.api.model.volcano.batch.JobList;
import io.fabric8.kubernetes.client.BaseClient;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import okhttp3.OkHttpClient;

public class VolcanoBatchAPIGroupClient extends BaseClient {

    public VolcanoBatchAPIGroupClient() throws KubernetesClientException {
        super();
    }

    public VolcanoBatchAPIGroupClient(OkHttpClient httpClient, final Config config) throws KubernetesClientException {
        super(httpClient, config);
    }


    public MixedOperation<Job, JobList, DoneableJob, Resource<Job, DoneableJob>> jobs() {
        return new VolcanoJobOperationsImpl(httpClient, getConfiguration());
    }
}