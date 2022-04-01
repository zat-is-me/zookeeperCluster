package clusterManagement;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * @author Tatek Ahmed on 3/1/2022
 **/

public class LeaderElection implements Watcher {
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;
    String predecessorZnodeName = "";

    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String prefix = "c_";
        String znodeFullPath = null;
            try {
                if (zooKeeper.exists(ELECTION_NAMESPACE, true) != null)
                    znodeFullPath = zooKeeper.create(ELECTION_NAMESPACE + "/" + prefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException.NoNodeException e) {
                System.out.println("Parent node is not found and created one ");
            }
            System.out.println(" znode name = " + znodeFullPath);
            this.currentZnodeName = znodeFullPath.replace("/election/", "");
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                onElectionCallback.onElectedToBeLeader();
                return;
            }else {
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children,currentZnodeName)-1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/"+predecessorZnodeName,this);
            }
        }
        onElectionCallback.onWorker();
        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException e) {e.printStackTrace();
                } catch (InterruptedException e) {e.printStackTrace();}
        }
    }
}
