package org.apache.spark.volcano.dsl;

import io.fabric8.kubernetes.api.model.volcano.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.volcano.batch.Job;
import io.fabric8.kubernetes.api.model.volcano.batch.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import okhttp3.OkHttpClient;

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
}