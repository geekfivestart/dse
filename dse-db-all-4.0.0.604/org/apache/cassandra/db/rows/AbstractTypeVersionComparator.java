package org.apache.cassandra.db.rows;

import java.util.Comparator;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;

final class AbstractTypeVersionComparator implements Comparator<AbstractType<?>> {
   public static final Comparator<AbstractType<?>> INSTANCE = new AbstractTypeVersionComparator();

   private AbstractTypeVersionComparator() {
   }

   public int compare(AbstractType<?> type, AbstractType<?> otherType) {
      if(!type.getClass().equals(otherType.getClass())) {
         throw new IllegalArgumentException(String.format("Trying to compare 2 different types: %s and %s", new Object[]{type, otherType}));
      } else {
         return type.equals(otherType)?0:(type.isUDT()?this.compareUserType((UserType)type, (UserType)otherType):(type.isTuple()?this.compareTuple((TupleType)type, (TupleType)otherType):(type.isCollection()?this.compareCollectionTypes(type, otherType):(type instanceof CompositeType?this.compareCompositeTypes((CompositeType)type, (CompositeType)otherType):0))));
      }
   }

   private int compareCompositeTypes(CompositeType type, CompositeType otherType) {
      List<AbstractType<?>> types = type.getComponents();
      List<AbstractType<?>> otherTypes = otherType.getComponents();
      if(types.size() != otherTypes.size()) {
         return Integer.compare(types.size(), otherTypes.size());
      } else {
         int i = 0;
         int m = type.componentsCount();
         if(i < m) {
            int test = this.compare((AbstractType)types.get(i), (AbstractType)otherTypes.get(i));
            if(test != 0) {
               ;
            }

            return test;
         } else {
            return 0;
         }
      }
   }

   private int compareCollectionTypes(AbstractType<?> type, AbstractType<?> otherType) {
      return type instanceof MapType?this.compareMapType((MapType)type, (MapType)otherType):(type instanceof SetType?this.compare(((SetType)type).getElementsType(), ((SetType)otherType).getElementsType()):this.compare(((ListType)type).getElementsType(), ((ListType)otherType).getElementsType()));
   }

   private int compareMapType(MapType<?, ?> type, MapType<?, ?> otherType) {
      int test = this.compare(type.getKeysType(), otherType.getKeysType());
      return test != 0?test:this.compare(type.getValuesType(), otherType.getValuesType());
   }

   private int compareUserType(UserType type, UserType otherType) {
      return this.compareTuple(type, otherType);
   }

   private int compareTuple(TupleType type, TupleType otherType) {
      if(type.size() != otherType.size()) {
         return Integer.compare(type.size(), otherType.size());
      } else {
         int test = 0;

         for(int i = 0; test == 0 && i < type.size(); ++i) {
            test = this.compare(type.type(i), otherType.type(i));
         }

         return test;
      }
   }
}
