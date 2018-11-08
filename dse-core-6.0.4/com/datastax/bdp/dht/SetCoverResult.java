package com.datastax.bdp.dht;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class SetCoverResult<T, S> {
   private final Map<T, List<S>> cover;
   private final Set<S> uncovered;

   public SetCoverResult(Map<T, List<S>> cover) {
      this(cover, Collections.emptySet());
   }

   public SetCoverResult(Map<T, List<S>> cover, Set<S> uncovered) {
      this.cover = Collections.unmodifiableMap(cover);
      this.uncovered = Collections.unmodifiableSet(uncovered);
   }

   public Map<T, List<S>> getCover() {
      return this.cover;
   }

   public Set<S> getUncovered() {
      return this.uncovered;
   }

   public boolean isComplete() {
      return !this.cover.isEmpty() && this.uncovered.isEmpty();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         SetCoverResult that = (SetCoverResult)o;
         return Objects.equals(this.cover, that.cover) && Objects.equals(this.uncovered, that.uncovered);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.cover, this.uncovered});
   }

   public String toString() {
      return com.google.common.base.Objects.toStringHelper(this).add("cover", this.cover).add("uncovered", this.uncovered).toString();
   }
}
