package org.apache.dubbo.registry.kubernetes;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Watch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private CoreV1Api api;

    private final String namespaces;

    private final String podName;

    private final ApiClient apiClient;

    private final static String FULL_URL = "full_url";

    private final static String META_DATA = "mate";

    private final static String SVC_KEY = "service_key";

    private static final String MARK = "mark";

    private static final Long INITAIL_DELAY = 0L;

    private static final Long PERIOD = 10L;

    private static final Integer CALL_TIMEOUT = 10;

    private static ExecutorService kubernetesWatcher = null;

    private final Map<URL, Watch> kubernetesWatcherMap = new ConcurrentHashMap<>(16);

    public KubernetesRegistry(ApiClient apiClient, URL url, CoreV1Api api) {
        super(url);
        this.apiClient = apiClient;
        this.api = api;
        this.namespaces = url.getParameter(KUBERNETES_NAMESPACES_KEY);
        this.podName = url.getParameter(KUBERNETES_POD_NAME_KEY);
    }

    @Override
    protected void doRegister(URL url) {
        try {
            V1Pod v1Pod = queryPodNameByUnRegistryUrl(url);
            Map<String, String> labels = v1Pod.getMetadata().getLabels();
            labels.putAll(url2Labels(url));
            api.patchNamespacedPod(v1Pod.getMetadata().getName(), namespaces, v1Pod, "false", "");
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    protected void doUnregister(URL url) {
        try {
            V1Pod v1Pod = queryPodNameByRegistriedUrl(url);
            api.deleteNamespacedPod(v1Pod.getMetadata().getName(), namespaces, null, null, null, null, null, null);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    protected void doSubscribe(URL url, NotifyListener notifyListener) {
        final List<URL> urls = queryUrls(url);
        this.notify(url, notifyListener, urls);
        try {
            final Watch<V1Pod> watch = Watch.createWatch(
                    apiClient,
                    api.listNamespacedPodCall(null, null, null,
                            null, null, null,
                            -1, null, CALL_TIMEOUT, Boolean.TRUE, null, null),
                    new TypeToken<Watch.Response<V1Pod>>() {
                    }.getType()
            );
            kubernetesWatcherMap.computeIfAbsent(url, k -> watch);
        } catch (ApiException e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        if (kubernetesWatcher == null) {
            kubernetesWatcher = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("KUBERNETES_WATCHER_EXECUTOR"));
            ((ScheduledExecutorService) kubernetesWatcher)
                    .scheduleAtFixedRate(doWatch(), INITAIL_DELAY, PERIOD, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void doUnsubscribe(URL url, NotifyListener notifyListener) {
        Watch watch = kubernetesWatcherMap.remove(url);
        try {
            watch.close();
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public boolean isAvailable() {
        try {
            return CollectionUtils.isNotEmpty(getAllRinningService());
        } catch (ApiException e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        super.destroy();
        kubernetesWatcherMap.values().forEach(watch -> {
            try {
                watch.close();
            } catch (IOException e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        kubernetesWatcherMap.clear();
        if (kubernetesWatcher != null) {
            kubernetesWatcher.shutdown();
        }
    }

    private URL pod2Url(V1Pod pod) {
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

    private V1Pod queryPodNameByUnRegistryUrl(URL url) throws Exception {
        return api.listNamespacedPod("", false, "false", null, null
                , null, null, null, null, false).getItems()
                .stream()
                .filter(pod -> {
                    final String mark = pod.getMetadata().getName();
                    final String hostName = pod.getSpec().getHostname();
                    return StringUtils.isNotEmpty(mark) && mark.startsWith(podName)
                            && StringUtils.isNotEmpty(hostName) && url.getHost().equals(hostName);
                }).collect(Collectors.toList()).get(0);
    }

    private V1Pod queryPodNameByRegistriedUrl(URL url) throws Exception {
        return api.listNamespacedPod("", false, "false", null, null
                , null, null, null, null, false).getItems()
                .stream()
                .filter(pod -> {
                    final String mark = pod.getMetadata().getLabels().get(MARK);
                    final String fullUrl = pod.getMetadata().getLabels().get(FULL_URL);
                    return StringUtils.isNotEmpty(mark) && Constants.DEFAULT_PROTOCOL.equals(mark)
                            && StringUtils.isNotEmpty(fullUrl) && url.toFullString().equals(fullUrl);
                }).collect(Collectors.toList()).get(0);
    }

    private List<URL> getAllRinningService() throws ApiException {
        return api.listNamespacedPod("", false, "false", null, null
                , null, null, null, null, false).getItems()
                .stream()
                .filter(v1Pod ->
                        v1Pod.getMetadata().getLabels().get(MARK) != null
                                && v1Pod.getMetadata().getLabels().get(MARK).equals(Constants.DEFAULT_PROTOCOL)
                                && v1Pod.getStatus().getContainerStatuses().stream()
                                .allMatch(v1ContainerStatus ->
                                        v1ContainerStatus.getState().getRunning() != null
                                )
                )
                .map(this::pod2Url)
                .collect(Collectors.toList());
    }

    private List<URL> getServicesByKey(String serviceKey) throws ApiException {
        return api.listNamespacedPod("", false, "false", null, null
                , null, null, null, null, false).getItems()
                .stream()
                .filter(v1Pod ->
                        v1Pod.getMetadata().getLabels().get(MARK) != null
                                && v1Pod.getMetadata().getLabels().get(MARK).equals(Constants.DEFAULT_PROTOCOL)
                                && v1Pod.getMetadata().getLabels().get(SVC_KEY) != null
                                && v1Pod.getMetadata().getLabels().get(SVC_KEY).equals(serviceKey)
                                && v1Pod.getStatus().getContainerStatuses().stream()
                                .allMatch(v1ContainerStatus ->
                                        v1ContainerStatus.getState().getRunning() != null
                                )
                )
                .map(this::pod2Url)
                .collect(Collectors.toList());
    }

    private List<URL> queryUrls(URL url) {
        final List<URL> urls = new ArrayList<>();
        if (ANY_VALUE.equals(url.getServiceInterface())) {
            try {
                urls.addAll(getAllRinningService());
            } catch (ApiException e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        } else {
            String serviceKey = url.getServiceKey();
            try {
                urls.addAll(getServicesByKey(serviceKey));
            } catch (ApiException e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return urls;
    }

    private Runnable doWatch() {
        return () -> {
            kubernetesWatcherMap.keySet().forEach(url -> {
                Watch watch = kubernetesWatcherMap.get(url);
                while (watch.hasNext()) {
                    Watch.Response response = watch.next();
                    processWatchReponse(url);
                }
            });
        };
    }

    private void processWatchReponse(URL url) {
        final List<URL> urls = queryUrls(url);
        for (NotifyListener listener : getSubscribed().get(url)) {
            doNotify(url, listener, urls);
        }
    }
}
