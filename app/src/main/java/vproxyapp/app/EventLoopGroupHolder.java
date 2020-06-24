package vproxyapp.app;

import vproxybase.component.elgroup.EventLoopGroup;
import vproxybase.util.exception.AlreadyExistException;
import vproxybase.util.exception.NotFoundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventLoopGroupHolder {
    final Map<String, EventLoopGroup> map = new HashMap<>();

    public List<String> names() {
        return new ArrayList<>(map.keySet());
    }

    public void add(String alias) throws AlreadyExistException {
        if (map.containsKey(alias))
            throw new AlreadyExistException("event-loop-group", alias);
        EventLoopGroup group = new EventLoopGroup(alias);
        map.put(alias, group);
    }

    public EventLoopGroup get(String alias) throws NotFoundException {
        EventLoopGroup group = map.get(alias);
        if (group == null)
            throw new NotFoundException("event-loop-group", alias);
        return group;
    }

    public void removeAndClose(String alias) throws NotFoundException {
        EventLoopGroup g = map.remove(alias);
        if (g == null)
            throw new NotFoundException("event-loop-group", alias);
        g.close();
    }
}