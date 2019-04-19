//import com.google.gson.reflect.TypeToken;
//import io.kubernetes.client.*;
//import io.kubernetes.client.apis.CoreV1Api;
//import io.kubernetes.client.models.V1Pod;
//import io.kubernetes.client.util.Config;
//import io.kubernetes.client.util.Watch;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.*;
import okio.Utf8;

import java.io.*;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhujianxin
 * @date 2019/4/8.
 */
public class KubernetesTest {

    private static final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) throws Exception {
        //put();

        k8s();
//        ApiClient apiClient = Config.fromConfig("/Users/rocky/tcpdump/config");
//        Configuration.setDefaultApiClient(apiClient);
//        CoreV1Api api = new CoreV1Api();
////        Attach attach = new Attach();
////        Attach.AttachResult attachResult = attach.attach("kube-ym-ad","ads-bidder-856748b9c-m8rv6",true);
//        final Watch<V1Pod> watch = Watch.createWatch(
//                apiClient,
//                api.listNamespacedPodCall("kube-ym-ad", true,
//                        null, null, null,
//                        "app=app-kubernets-example", null, "0", 300, true, null, null),
//                new TypeToken<Watch.Response<V1Pod>>() {
//                }.getType()
//        );
//
//        new Thread( () -> {
//            try {
//                while (watch.hasNext()){
//                    Watch.Response<V1Pod> response = watch.next();
//                    System.out.println(response.type + "," + response.object.getMetadata().getName() + "," + response.object.getStatus().getPhase());
//
//                }
//                System.out.println("===================");
//            }catch (Exception e){
//
//            }
//        }).start();

//        ses.scheduleWithFixedDelay(() -> {
//            for (Watch.Response<V1Pod> response : watch) {
//                System.out.println(response.type + "," + response.object.getMetadata().getName() + "," + response.object.getStatus().getPhase());
//            }
//            System.out.println("===================");
//        }, 2L, 1L, TimeUnit.SECONDS);
    }

    private static void k8s() throws Exception{
        KubernetesClient client = new DefaultKubernetesClient();
//        String base = Base64.getEncoder().encodeToString("dubbo://192.168.26.113:8888/com.dubbo.api.service.SafeCheckService?anyhost=true&application=app-kubernets-example&application.version=1.0.0&bean.name=ServiceBean:com.dubbo.api.service.SafeCheckService:1.0.0&dubbo=2.0.2&generic=false&interface=com.dubbo.api.service.SafeCheckService&methods=test,isSafe&pid=19645&revision=1.0.0&side=provider&status=server&timestamp=1554964193819&version=1.0.0".getBytes());
//        System.out.println(base);
//        System.out.println(new String(Base64.getDecoder().decode(base)));
        ;
        client.pods()
                .inNamespace("kube-ym-ad")
                .withLabel("app", "app-kubernets-example")
                .list().getItems().forEach(pod -> {
                    System.out.println(pod.getSpec().getHostname());
                    System.out.println();
//                    client.pods().inNamespace(pod.getMetadata().getNamespace()).withName(pod.getMetadata().getName())
//                            .edit()
//                            .editMetadata().addToLabels("mark","dubbo").and()
//                            .editMetadata().addToAnnotations("interface","com.dubbo.api.service.SafeCheckService:1.0.0")
//                            .addToAnnotations("full_url","dubbo://192.168.26.113:8888/com.dubbo.api.service.SafeCheckService?anyhost=true&application=app-kubernets-example&application.version=1.0.0&bean.name=ServiceBean:com.dubbo.api.service.SafeCheckService:1.0.0&dubbo=2.0.2&generic=false&interface=com.dubbo.api.service.SafeCheckService&methods=test,isSafe&pid=19645&revision=1.0.0&side=provider&status=server&timestamp=1554964193819&version=1.0.0")
//                            .and()
//                            .done();
        });
//
//        Watch watch = client.pods().inNamespace("kube-ym-ad").withLabel("app=app-kubernets-example")
//                .watch(new Watcher<Pod>() {
//                    @Override
//                    public void eventReceived(Action action, Pod pod) {
//                        System.out.println(action.name() + "," + pod.getStatus().getPhase() + "," + pod.getMetadata().getName());
//                        System.out.println("========");
//                    }
//
//                    @Override
//                    public void onClose(KubernetesClientException e) {
//                        System.out.println(e.getMessage());
//                    }
//                });
    }

    private static void gen(){
        Map<String,String> map = new HashMap<String, String>(){{
            put("feed-1200*628-interstitial-result","108326,108327,108332,108331,108324,108328,108325,108330,108326,108327,108332,108331,108324,108328,108325,108330");
            put("feed-1200*628-locker-result","108329,108306,108308,108305,108329,108306,108308,108305");
            put("feed-1200*628-screen-news","108224,108163,108164,108105,108239,108281,108199,108103,108109,108107,108224,108163,108164,108105,108239,108281,108199,108103,108109,108107");
            put("feed-1200*628-screen-news-2","116146,116144,116162,116163,116165,116146,116144,116162,116163,116165");
        }};
        final StringBuilder res = new StringBuilder();
        res.append("{" +
                "\"adLocations\":[");
        map.forEach((level,codes) -> {
            for(String code : codes.split(",")){
                res.append("{" +
                        "\"creativeSpec\":\""+code + "\"," +
                        "\"locationLevel2\":\"" + level + "\"," +
                        "\"locationLevel1\":\"feed\"" +
                        "},");
            }
        });
        res.append("]" +
                "}");
        System.out.println(res.toString());

    }

    private static void put(){
        KubernetesClient kubernetesClient = new DefaultKubernetesClient();
        kubernetesClient.pods()
                .inNamespace("kube-apps")
                .withName("dubbo-caller-app-647d8fdb47-wvgrb")
                .edit()
                .editMetadata()
                .addToAnnotations("com.dubbo.api:1.0.0","{'json':'jsontest'}")
                .and()
                .done();
    }
}
