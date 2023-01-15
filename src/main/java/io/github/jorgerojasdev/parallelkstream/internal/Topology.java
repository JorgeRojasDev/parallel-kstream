package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.internal.model.properties.ParallelKStreamProperties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;

@RequiredArgsConstructor
@Getter
public class Topology {

    private final Map<String, SubTopology<?, ?>> subTopologiesMap = new HashMap<>();
    private final ParallelKStreamProperties properties;

    public void addSubtopology(SubTopology<?, ?> subTopology) {
        this.subTopologiesMap.putIfAbsent(subTopology.getSubTopologyName(), subTopology);
    }

    public Optional<SubTopology<?, ?>> getSubtopology(String subTopologyName) {
        return Optional.ofNullable(subTopologiesMap.get(subTopologyName));
    }

    public List<SubTopology<?, ?>> subTopologies() {
        List<SubTopology<?, ?>> subTopologies = new ArrayList<>();
        subTopologiesMap.forEach((name, subTopology) -> subTopologies.add(subTopology));
        return subTopologies;
    }

    public void init() {
        subTopologies().forEach(SubTopology::init);
    }

    public void start() {
        subTopologies().forEach(SubTopology::start);
    }

    public void pause() {
        subTopologies().forEach(SubTopology::pause);
    }

    public void resume() {
        subTopologies().forEach(SubTopology::resume);
    }
}
