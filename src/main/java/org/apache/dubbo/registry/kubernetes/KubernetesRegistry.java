package org.apache.dubbo.registry.kubernetes;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.alibaba.dubbo.common.Constants.ANY_VALUE;
import static org.apache.dubbo.registry.kubernetes.KubernetesRegistryFactory.KUBERNETES_NAMESPACES_KEY;
import static org.apache.dubbo.registry.kubernetes.KubernetesRegistryFactory.KUBERNETES_POD_NAME_KEY;


/**
 * @author zhujianxin
 * @date 2019/4/1.
 */
public class KubernetesRegistry extends FailbackRegistry {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesRegistry.class);

    private final String namespaces;

    private final KubernetesClient kubernetesClient;

    private final static String FULL_URL = "full_url";

    private final static String META_DATA = "mate";

    private final static String SVC_KEY = "service_key";

    private static final String MARK = "mark";

    private static final String APP_LABEL = "app";

    private final Map<URL, Watch> kubernetesWatcherMap = new ConcurrentHashMap<>(16);

    public KubernetesRegistry(KubernetesClient kubernetesClient, URL url) {
        super(url);
        this.kubernetesClient = kubernetesClient;
        this.namespaces = url.getParameter(KUBERNETES_NAMESPACES_KEY);
    }

    @Override
    protected void doRegister(URL url) {
        PodList pods = queryPodsByUnRegistryUrl(url);
        pods.getItems().forEach(pod -> {
            final Map<String, String> labels = pod.getMetadata().getLabels();
            labels.putAll(url2Labels(url));
            kubernetesClient.pods().createOrReplace(pod);
        });
    }

    @Override
    protected void doUnregister(URL url) {
        PodList pods = queryPodNameByRegistriedUrl(url);
        pods.getItems().forEach(pod -> {
            removeLabels(pod);
            kubernetesClient.pods().createOrReplace(pod);
        });
    }

    @Override
    protected void doSubscribe(URL url, NotifyListener notifyListener) {
        final List<URL> urls = queryUrls(url);
        this.notify(url, notifyListener, urls);

        kubernetesWatcherMap.computeIfAbsent(url, k ->
                kubernetesClient.pods().inNamespace(namespaces).withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                        .watch(new Watcher<Pod>() {
                            @Override
                            public void eventReceived(Action action, Pod pod) {
                                final List<URL> urlList =
                                        kubernetesClient.pods().withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                                                .list().getItems()
                                                .stream()
                                                .filter(p -> {
                                                    if (p.getStatus().getPhase().equals(KubernetesStatus.Running.name())) {
                                                        Map<String, String> labels = p.getMetadata().getLabels();
                                                        if (labels.get(MARK) == null) {
                                                            labels.putAll(url2Labels(url));
                                                            kubernetesClient.pods().createOrReplace(p);
                                                        }
                                                        return true;
                                                    } else {
                                                        return false;
                                                    }
                                                })
                                                .map(KubernetesRegistry.this::pod2Url)
                                                .collect(Collectors.toList());
                                doNotify(url, notifyListener, urlList);
                            }

                            @Override
                            public void onClose(KubernetesClientException e) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("pod watch closed");
                                }
                                if (e != null) {
                                    logger.error("watcher onClose exception", e);
                                }
                            }
                        }));
    }

    @Override
    protected void doUnsubscribe(URL url, NotifyListener notifyListener) {
        Watch watch = kubernetesWatcherMap.remove(url);
        watch.close();
    }

    @Override
    public boolean isAvailable() {
        return CollectionUtils.isNotEmpty(getAllRunningService());
    }

    @Override
    public void destroy() {
        super.destroy();
        Collection<URL> urls = Collections.unmodifiableSet(kubernetesWatcherMap.keySet());
        urls.forEach(url -> {
            Watch watch = kubernetesWatcherMap.remove(url);
            watch.close();
        });
    }

    private URL pod2Url(Pod pod) {
        return URL.valueOf(pod.getMetadata().getLabels().get(FULL_URL));
    }

    private Map<String, String> url2Labels(URL url) {
        final Map<String, String> labels = new HashMap<>(16);
        labels.put(MARK, Constants.DEFAULT_PROTOCOL);
        labels.put(SVC_KEY, url.getServiceKey());
        labels.put(Constants.CATEGORY_KEY, url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY));
        labels.put(FULL_URL, url.toFullString());
        labels.put(Constants.INTERFACE_KEY, url.getServiceInterface());
        labels.put(META_DATA, JSONObject.toJSONString(url.getParameters()));
        return labels;
    }

    private void removeLabels(Pod pod) {
        pod.getMetadata().getLabels().remove(MARK);
        pod.getMetadata().getLabels().remove(SVC_KEY);
        pod.getMetadata().getLabels().remove(Constants.CATEGORY_KEY);
        pod.getMetadata().getLabels().remove(FULL_URL);
        pod.getMetadata().getLabels().remove(Constants.INTERFACE_KEY);
        pod.getMetadata().getLabels().remove(META_DATA);
    }

    private PodList queryPodsByUnRegistryUrl(URL url) {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                .list();
    }

    private PodList queryPodNameByRegistriedUrl(URL url) {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .withLabel(FULL_URL, url.toFullString())
                .list();
    }

    private List<URL> getAllRunningService() {
        return kubernetesClient.pods()
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .list().getItems().stream()
                .filter(pod -> pod.getStatus().getPhase().equals(KubernetesStatus.Running.name()))
                .map(this::pod2Url)
                .collect(Collectors.toList());
    }

    private List<URL> getServicesByKey(String serviceKey) {
        return kubernetesClient.pods()
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .withLabel(SVC_KEY, serviceKey)
                .list().getItems().stream()
                .filter(pod -> pod.getStatus().getPhase().equals(KubernetesStatus.Running.name()))
                .map(this::pod2Url)
                .collect(Collectors.toList());
    }

    private List<URL> queryUrls(URL url) {
        final List<URL> urls = new ArrayList<>();
        if (ANY_VALUE.equals(url.getServiceInterface())) {
            urls.addAll(getAllRunningService());
        } else {
            urls.addAll(getServicesByKey(url.getServiceKey()));
        }
        return urls;
    }

    enum KubernetesStatus {
        Running,
        Pending,
        Terminating;
    }
}
