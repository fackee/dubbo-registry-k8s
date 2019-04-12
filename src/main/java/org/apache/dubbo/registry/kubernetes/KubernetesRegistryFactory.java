package org.apache.dubbo.registry.kubernetes;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Map;

/**
 * @author zhujianxin
 * @date 2019/4/1.
 */
public class KubernetesRegistryFactory extends AbstractRegistryFactory {

    protected static final String KUBERNETES_NAMESPACES_KEY = "group";

    protected static final String KUBERNETES_POD_NAME_KEY = "application";

    @Override
    protected Registry createRegistry(URL url) {
        Map<String, String> meta = url.getParameters();
        if (meta == null) {
            throw new IllegalArgumentException("meta data is empty");
        }
        boolean hasKubernetesConfigs = StringUtils.isBlank(meta.getOrDefault(KUBERNETES_NAMESPACES_KEY,"default")) ||
                StringUtils.isBlank(meta.get(KUBERNETES_POD_NAME_KEY));
        if (hasKubernetesConfigs) {
            throw new IllegalArgumentException("kubernetes namespaces, or pod is empty");
        }
        KubernetesClient client = new DefaultKubernetesClient();

        return new KubernetesRegistry(client, url);
    }
}
