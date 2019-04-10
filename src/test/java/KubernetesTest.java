//import com.google.gson.reflect.TypeToken;
//import io.kubernetes.client.*;
//import io.kubernetes.client.apis.CoreV1Api;
//import io.kubernetes.client.models.V1Pod;
//import io.kubernetes.client.util.Config;
//import io.kubernetes.client.util.Watch;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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
        Config config = new ConfigBuilder().build();
        KubernetesClient client = new DefaultKubernetesClient();

        Watch watch = client.pods().inNamespace("kube-ym-ad").withLabel("app=app-kubernets-example")
                .watch(new Watcher<Pod>() {
                    @Override
                    public void eventReceived(Action action, Pod pod) {
                        System.out.println(action.name() + "," + pod.getStatus().getPhase() + "," + pod.getMetadata().getName());
                        System.out.println("========");
                    }

                    @Override
                    public void onClose(KubernetesClientException e) {
                        System.out.println(e.getMessage());
                    }
                });
    }
}
