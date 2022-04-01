import clusterManagement.OnElectionCallback;
import clusterManagement.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry serviceRegistry;
    private final int port;


    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() throws InterruptedException, KeeperException {
        serviceRegistry.unregisterFormCluster();
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try {
            String currentServiceAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(),port);
            serviceRegistry.registerToCluster(currentServiceAddress);
        } catch (InterruptedException e){
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (KeeperException e){
            e.printStackTrace();
        }
    }
}
