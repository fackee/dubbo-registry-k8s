package org.apache.dubbo.registry.kubernetes;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.Map;

/**
 * @author zhujianxin
 * @date 2019/4/1.
 */
public class KubernetesRegistryFactory extends AbstractRegistryFactory {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesRegistryFactory.class);

    protected static final String KUBERNETES_NAMESPACES_KEY = "k8s-ns";

    protected static final String KUBERNETES_POD_NAME_KEY = "k8s-pod";

    @Override
    protected Registry createRegistry(URL url) {
        Map<String, String> meta = url.getParameters();
        if (meta == null) {
            throw new IllegalArgumentException("meta data is empty");
        }
        boolean hasKubernetesConfigs = StringUtils.isBlank(meta.get(KUBERNETES_NAMESPACES_KEY)) ||
                StringUtils.isBlank(meta.get(KUBERNETES_POD_NAME_KEY));
        if (hasKubernetesConfigs) {
            throw new IllegalArgumentException("kubernetes namespaces, or pod is empty");
        }
        KubernetesRegistry registry = null;
        try {
            ApiClient apiClient = Config.defaultClient();
            Configuration.setDefaultApiClient(apiClient);
            CoreV1Api coreV1Api = new CoreV1Api();
            registry = new KubernetesRegistry(url , coreV1Api);
        } catch (IOException e) {
            logger.error("create kubernetes ApiClient exception", e);
        }
        return registry;
    }
}
