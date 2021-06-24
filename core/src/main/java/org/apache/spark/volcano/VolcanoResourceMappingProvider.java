package org.apache.spark.volcano;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.KubernetesResourceMappingProvider;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implements ServiceLoader for Volcano Resources. Used by {@link io.fabric8.kubernetes.internal.KubernetesDeserializer}
 * to map json representations for Volcano resources to appropriate Java types.
 */
public class VolcanoResourceMappingProvider implements KubernetesResourceMappingProvider {

    /**
     * Base package name for volcano CRD POJOs
     */
    public static final String BASE_PKG_NAME = "io.fabric8.kubernetes.api.model.volcano";

    private static final String KEY_SEPARATOR = "#";

    /**
     * Which sub-package links to what values of the apiVersion attribute of the Volcano CRDs
     */
    public static final Map<String, String> SUB_PKG_TO_API_VERSION_MAPPING = ImmutableMap.of(
            "batch", "batch.volcano.sh/v1alpha1",
            "bus", "bus.volcano.sh/v1alpha1",
            "scheduling", "v1beta1"
    );

    public Map<String, Class<? extends KubernetesResource>> getMappings() {
        Map<String, Class<? extends KubernetesResource>> mappings = new HashMap<>();

        for(String pkgName: SUB_PKG_TO_API_VERSION_MAPPING.keySet()) {
            // Combine class mappings for each sub-package
            Map<String, Class<? extends KubernetesResource>> mappingsForPackage = getMappingsForPackage(
                    pkgName, SUB_PKG_TO_API_VERSION_MAPPING.get(pkgName));
            mappings.putAll(mappingsForPackage);
        }

        return mappings;
    }

    private Map<String, Class<? extends KubernetesResource>> getMappingsForPackage(
            String subPkgName, String apiVersion) {

        String currentPackage = String.join(".", new String[]{BASE_PKG_NAME, subPkgName});
        Map<String, Class<? extends KubernetesResource>> mappings = new HashMap<>();

        // Find all classes in the current package that are subclasses of KubernetesResource
        Reflections classFinder = new Reflections(currentPackage);
        Set<Class<? extends KubernetesResource>> subclasses = classFinder.getSubTypesOf(KubernetesResource.class);

        for (Class<? extends KubernetesResource> resourceClass: subclasses) {
            // The expected key is apiVersion#ClassName
            String key = String.join(KEY_SEPARATOR, new String[]{apiVersion, resourceClass.getSimpleName()});
            mappings.put(key, resourceClass);
        }
        return mappings;
    }

    public static void main(String[] args) {
        VolcanoResourceMappingProvider provider = new VolcanoResourceMappingProvider();
        Map<String, Class<? extends KubernetesResource>> mappings = provider.getMappings();
        for(String key: mappings.keySet()) {
            System.out.println(key);
            System.out.println(mappings.get(key).getName());
        }
    }
}
