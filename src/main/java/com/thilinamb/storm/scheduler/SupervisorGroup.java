package com.thilinamb.storm.scheduler;

import org.apache.storm.scheduler.SupervisorDetails;

import java.util.List;

/**
 * Groups a set of supervisor processes together.
 * Components are assigned in a round-robin fashion within the group
 *
 * @author Thilina Buddhika
 */
class SupervisorGroup {

    private List<SupervisorDetails> supervisors;
    private int lastAssignedSupervisor;

    SupervisorGroup(List<SupervisorDetails> supervisors) {
        this.supervisors = supervisors;
        this.lastAssignedSupervisor = 0;
    }

    SupervisorDetails getNext() {
        SupervisorDetails supervisor = supervisors.get(lastAssignedSupervisor);
        lastAssignedSupervisor = (lastAssignedSupervisor + 1) % supervisors.size();
        return supervisor;
    }

    int getSize(){
        return supervisors.size();
    }
}
