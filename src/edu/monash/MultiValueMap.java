package edu.monash;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by psangats on 29/07/2017.
 */
public class MultiValueMap<K, V> {
    private final Map<K, Set<V>> mappings = new HashMap<>();

    public Set<V> getValues(K key) {
        return mappings.get(key);
    }

    public Boolean putValue(K key, V value) {
        Set<V> target = mappings.get(key);

        if (target == null) {
            target = new HashSet<>();
            mappings.put(key, target);
        }

        return target.add(value);
    }

}
