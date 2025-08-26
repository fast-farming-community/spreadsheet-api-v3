package eu.fast.gw2.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OverlayReferencePlanner {

    public static Set<String> collectCompositeKeysReferencedByMains(List<String> mainTargets) {
        Set<String> refKeys = new HashSet<>();
        for (String name : mainTargets) {
            List<Map<String, Object>> base = OverlayCache.getBaseMainRows(name);
            if (base == null)
                continue;
            for (var row : base) {
                String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
                String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
                if (key != null && !key.isBlank() && OverlayHelper.isCompositeRef(cat, key)) {
                    refKeys.add(key);
                }
            }
        }
        return refKeys;
    }

    public static Set<String> expandCompositeRefs(Set<String> seed, int levels) {
        if (seed == null || seed.isEmpty() || levels <= 0)
            return new HashSet<>();
        Set<String> all = new HashSet<>(seed);
        Set<String> frontier = new HashSet<>(seed);
        for (int depth = 0; depth < levels; depth++) {
            if (frontier.isEmpty())
                break;
            OverlayCache.preloadDetailRows(frontier);
            Set<String> next = new HashSet<>();
            for (String k : frontier) {
                List<Map<String, Object>> rows = OverlayCache.getBaseDetailRows(k);
                if (rows == null)
                    continue;
                for (var r : rows) {
                    String cat = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
                    String ref = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));
                    if (ref != null && !ref.isBlank()
                            && OverlayHelper.isCompositeRef(cat, ref) && !all.contains(ref)) {
                        next.add(ref);
                    }
                }
            }
            next.removeAll(all);
            all.addAll(next);
            frontier = next;
        }
        return all;
    }

    private OverlayReferencePlanner() {
    }
}
