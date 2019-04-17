package org.apache.dubbo.registry.kubernetes;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final static String SVC_KEY = "service_key";

    private static final String MARK = "mark";

    private static final String APP_LABEL = "app";

    private static final String DUBBO_META_KEY = Constants.DEFAULT_PROTOCOL + "_meta_";

    private final AtomicInteger serviceCounter;

    private final Map<URL, Watch> kubernetesWatcherMap = new ConcurrentHashMap<>(16);

    public KubernetesRegistry(KubernetesClient kubernetesClient, URL url) {
        super(url);
        this.kubernetesClient = kubernetesClient;
        this.namespaces = url.getParameter(KUBERNETES_NAMESPACES_KEY);
        serviceCounter = new AtomicInteger(0);
    }


    @Override
    public void register(URL url) {
        if(isConsumerSide(url)){
            return;
        }
        super.register(url);
    }

    @Override
    protected void doRegister(URL url) {
        List<Pod> pods = queryPodsByUnRegistryUrl(url);
        if (pods != null && pods.size() > 0) {
            pods.forEach(pod -> registry(url, pod));
        }
    }

    @Override
    public void unregister(URL url) {
        if(isConsumerSide(url)){
            return;
        }
        super.unregister(url);
    }

    @Override
    protected void doUnregister(URL url) {
        List<Pod> pods = queryPodNameByRegistriedUrl(url);
        if (pods != null && pods.size() > 0) {
            pods.forEach(pod -> unregistry(pod, url));
        }
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (isProviderSide(url)) {
            return;
        }

        super.subscribe(url, listener);
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
                                                            registry(url, p);
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
    public void unsubscribe(URL url, NotifyListener listener) {
        if (isProviderSide(url)) {
            return;
        }

        super.unsubscribe(url, listener);
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

    private boolean isConsumerSide(URL url) {
        return url.getProtocol().equals(Constants.CONSUMER_PROTOCOL);
    }

    private boolean isProviderSide(URL url) {
        return url.getProtocol().equals(Constants.PROVIDER_PROTOCOL);
    }

    private URL pod2Url(Pod pod) {
        final JSONObject dubboMeta = new JSONObject();
        pod.getMetadata().getAnnotations().forEach((key, value) -> {
            if (key.startsWith(DUBBO_META_KEY)) {
                dubboMeta.putAll(JSON.parseObject(value));
            }
        });
        String fullUrl = dubboMeta.getString(FULL_URL);
        return URL.valueOf(fullUrl);
    }

    private void registry(URL url, Pod pod) {
        JSONObject meta = new JSONObject() {{
            put(Constants.INTERFACE_KEY, url.getServiceInterface());
            put(SVC_KEY, url.getServiceKey());
            put(FULL_URL, url.toFullString());
            putAll(url.getParameters());
        }};

        kubernetesClient.pods().inNamespace(pod.getMetadata().getNamespace()).withName(pod.getMetadata().getName())
                .edit()
                .editMetadata()
                .addToLabels(MARK, Constants.DEFAULT_PROTOCOL)
                .addToAnnotations(DUBBO_META_KEY + serviceCounter.getAndIncrement(), meta.toJSONString())
                .and()
                .done();
    }

    private void unregistry(Pod pod, URL url) {
        Pod registedPod = kubernetesClient.pods().inNamespace(pod.getMetadata().getNamespace()).withName(pod.getMetadata().getName()).get();
        if (registedPod.getMetadata().getAnnotations() != null) {
            registedPod.getMetadata().getAnnotations().forEach((removeKey, value) -> {
                if (removeKey.startsWith(DUBBO_META_KEY)) {
                    JSONObject jsonObject = JSONObject.parseObject(pod.getMetadata().getAnnotations().get(removeKey));
                    if (jsonObject.get(FULL_URL) != null && jsonObject.get(FULL_URL).equals(url.toFullString())) {
                        kubernetesClient.pods().inNamespace(pod.getMetadata().getNamespace()).withName(pod.getMetadata().getName())
                                .edit()
                                .editMetadata()
                                .removeFromLabels(MARK)
                                .removeFromAnnotations(removeKey)
                                .and()
                                .done();
                    }
                }
            });
        }
    }

    private List<Pod> queryPodsByUnRegistryUrl(URL url) {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                .list().getItems();
    }

    private List<Pod> queryPodNameByRegistriedUrl(URL url) {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(APP_LABEL, url.getParameter(KUBERNETES_POD_NAME_KEY))
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .list().getItems().stream()
                .filter(pod -> {
                    final JSONObject dubboMeta = new JSONObject();
                    pod.getMetadata().getAnnotations().forEach((key, value) -> {
                        if (key.startsWith(DUBBO_META_KEY)) {
                            dubboMeta.putAll(JSON.parseObject(value));
                        }
                    });
                    String fullUrl = dubboMeta.getString(FULL_URL);
                    return fullUrl != null && fullUrl.equals(url.toFullString());
                })
                .collect(Collectors.toList());
    }

    private List<URL> getAllRunningService() {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .list().getItems().stream()
                .filter(pod -> pod.getStatus().getPhase().equals(KubernetesStatus.Running.name()))
                .map(this::pod2Url)
                .collect(Collectors.toList());
    }

    private List<URL> getServicesByKey(String serviceKey) {
        return kubernetesClient.pods()
                .inNamespace(namespaces)
                .withLabel(MARK, Constants.DEFAULT_PROTOCOL)
                .list().getItems().stream()
                .filter(pod -> {
                    final boolean[] matched = {false};
                    pod.getMetadata().getAnnotations().forEach((key, value) -> {
                        if (key.startsWith(DUBBO_META_KEY)) {
                            final JSONObject dubboMeta = JSON.parseObject(value);
                            if(dubboMeta.get(SVC_KEY) != null &&
                                    dubboMeta.get(SVC_KEY).equals(serviceKey) &&
                                    pod.getStatus().getPhase().equals(KubernetesStatus.Running.name())){
                                matched[0] = true;
                                return;
                            }
                        }
                    });
                    return matched[0];
                })
                .map( pod -> getURLByPodWithServiceKey(pod,serviceKey))
                .collect(Collectors.toList());
    }

    private URL getURLByPodWithServiceKey(Pod pod,String serviceKey){
        final List<URL> urls = new ArrayList<>();
        pod.getMetadata().getAnnotations().forEach( (key , value) -> {
            if (key.startsWith(DUBBO_META_KEY)) {
                final JSONObject dubboMeta = JSON.parseObject(value);
                if(dubboMeta.get(SVC_KEY) != null &&
                        dubboMeta.get(SVC_KEY).equals(serviceKey) &&
                        pod.getStatus().getPhase().equals(KubernetesStatus.Running.name())){
                    urls.add(URL.valueOf(dubboMeta.getString(FULL_URL)));
                }
            }
        });
        return CollectionUtils.isEmpty(urls) ? null : urls.get(0);
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
