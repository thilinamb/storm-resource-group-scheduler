package com.thilinamb.storm.scheduler;

import org.apache.log4j.Logger;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;

import java.util.*;

/**
 * Implementation of the Storm scheduler
 *
 * @author Thilina Buddhika
 */
public class ResourceGroupScheduler implements IScheduler {

    private Logger logger = Logger.getLogger(ResourceGroupScheduler.class);

    private final double spoutHostPercentage = 0.685;
    private SupervisorGroup spoutHolderGroup;
    private SupervisorGroup boltHolderGroup;
    private boolean firstAttempt = true;

    @Override
    public void prepare(Map map) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
        Collection<TopologyDetails> topologyDetailsCollection = topologies.getTopologies();

        logger.debug("Scheduling Attempt. Initial attempt: " + firstAttempt + ", " +
                "Topology Count: " + topologyDetailsCollection.size() + ", Supervisor Size: " + supervisors.size());

        // if it is the first invocation, assign all available supervisors into groups.
        if (firstAttempt) {
            assignSupervisorsIntoGroups(supervisors.values());
            firstAttempt = false;
        }

        // assignment
        for (TopologyDetails topologyDetail : topologyDetailsCollection) {
            if (cluster.needsScheduling(topologyDetail)) {
                StormTopology topology = topologyDetail.getTopology();
                Map<String, Bolt> bolts = topology.get_bolts();
                Map<String, SpoutSpec> spouts = topology.get_spouts();
                for (String boltName : bolts.keySet()) {
                    assign(cluster, topologyDetail, boltName, boltHolderGroup);
                }
                for (String spoutName : spouts.keySet()) {
                    assign(cluster, topologyDetail, spoutName, spoutHolderGroup);
                }
            }
        }
    }

    private void assign(Cluster cluster, TopologyDetails topologyDetail, String compName,
                        SupervisorGroup supervisorGroup) {
        List<ExecutorDetails> executorsNeedToBeScheduled = cluster.
                getNeedsSchedulingComponentToExecutors(topologyDetail).get(compName);
        if (!executorsNeedToBeScheduled.isEmpty()) {
            SupervisorDetails nextComponentHolder = supervisorGroup.getNext();
            List<WorkerSlot> availableSlots = cluster.getAssignableSlots(nextComponentHolder);
            int iteration = 1;
            // if there are no available slots, try the next one
            while (availableSlots.isEmpty() && iteration <= supervisorGroup.getSize()) {
                nextComponentHolder = supervisorGroup.getNext();
                availableSlots = cluster.getAssignableSlots(nextComponentHolder);
                // makes sure it doesn't loop infinitely
                iteration++;
            }
            if (!availableSlots.isEmpty()) {
                cluster.assign(availableSlots.get(0), topologyDetail.getId(), executorsNeedToBeScheduled);
                logger.info("Assignment Successful. Component: " + compName + ", Supervisor host:" +
                        nextComponentHolder.getHost());
            } else {
                logger.error("Not enough free slots to assign " + compName);
            }
        }
    }

    private void assignSupervisorsIntoGroups(Collection<SupervisorDetails> supervisors) {
        // Optional: sorting the supervisors
        List<SupervisorDetails> sortedSupervisorList = new ArrayList<SupervisorDetails>(supervisors);
        Collections.sort(sortedSupervisorList, new Comparator<SupervisorDetails>() {
            @Override
            public int compare(SupervisorDetails o1, SupervisorDetails o2) {
                if (!o1.getHost().equals(o2.getHost())) {
                    return o1.getHost().compareTo(o2.getHost());
                } else {
                    return o1.getId().compareTo(o2.getHost());
                }
            }
        });
        // Calculating the number of supervisors dedicated for spouts and bolts
        // Assumption: there is sufficient supervisors for both groups
        int spoutHolderCount = (int) Math.ceil(supervisors.size() * spoutHostPercentage);
        List<SupervisorDetails> spoutHolderList = new ArrayList<SupervisorDetails>(spoutHolderCount);
        List<SupervisorDetails> boltHolderList = new ArrayList<SupervisorDetails>(supervisors.size() - spoutHolderCount);
        for (SupervisorDetails supervisor : supervisors) {
            if (spoutHolderList.size() < spoutHolderCount) {
                spoutHolderList.add(supervisor);
            } else {
                boltHolderList.add(supervisor);
            }
        }
        this.spoutHolderGroup = new SupervisorGroup(spoutHolderList);
        this.boltHolderGroup = new SupervisorGroup(boltHolderList);
        logger.info("Total number of available resources: " + supervisors.size() + ", SpoutHolderGroup Size: " +
                spoutHolderList.size() + ", BoltHolderGroupSize: " + boltHolderList.size());
    }
}
