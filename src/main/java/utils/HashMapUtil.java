
package utils;

import java.util.HashMap;

/**
 *
 * @author Ganymedian
 */
public class HashMapUtil {
    
    /**
     * 
     * @param <T>
     * @param map
     * @param v
     * @return 
     */
    public static <T> int increaseValueByOne(HashMap<T, Integer> map, T v) {
    if (!map.containsKey(v)) {
      map.put(v, 1);
    } else {
      map.put(v, map.get(v) + 1);
    }

    return map.get(v);
  }
    
}
