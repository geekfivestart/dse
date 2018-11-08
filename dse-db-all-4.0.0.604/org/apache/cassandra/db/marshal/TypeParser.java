package org.apache.cassandra.db.marshal;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TypeParser {
   private final String str;
   private int idx;
   private static final Map<String, AbstractType<?>> cache = new HashMap();
   public static final TypeParser EMPTY_PARSER = new TypeParser("", 0);

   private TypeParser(String str, int idx) {
      this.str = str;
      this.idx = idx;
   }

   public TypeParser(String str) {
      this(str, 0);
   }

   public static AbstractType<?> parse(String str) throws SyntaxException, ConfigurationException {
      if(str == null) {
         return BytesType.instance;
      } else {
         AbstractType<?> type = (AbstractType)cache.get(str);
         if(type != null) {
            return type;
         } else {
            int i = 0;
            int i = skipBlank(str, i);

            int j;
            for(j = i; !isEOS(str, i) && isIdentifierChar(str.charAt(i)); ++i) {
               ;
            }

            if(i == j) {
               return BytesType.instance;
            } else {
               String name = str.substring(j, i);
               i = skipBlank(str, i);
               if(!isEOS(str, i) && str.charAt(i) == 40) {
                  type = getAbstractType(name, new TypeParser(str, i));
               } else {
                  type = getAbstractType(name);
               }

               cache.put(str, type);
               return type;
            }
         }
      }
   }

   public static AbstractType<?> parse(CharSequence compareWith) throws SyntaxException, ConfigurationException {
      return parse(compareWith == null?null:compareWith.toString());
   }

   public AbstractType<?> parse() throws SyntaxException, ConfigurationException {
      this.skipBlank();
      String name = this.readNextIdentifier();
      this.skipBlank();
      return !this.isEOS() && this.str.charAt(this.idx) == 40?getAbstractType(name, this):getAbstractType(name);
   }

   public Map<String, String> getKeyValueParameters() throws SyntaxException {
      if(this.isEOS()) {
         return Collections.emptyMap();
      } else if(this.str.charAt(this.idx) != 40) {
         throw new IllegalStateException();
      } else {
         Map<String, String> map = new HashMap();
         ++this.idx;

         String k;
         String v;
         for(; this.skipBlankAndComma(); map.put(k, v)) {
            if(this.str.charAt(this.idx) == 41) {
               ++this.idx;
               return map;
            }

            k = this.readNextIdentifier();
            v = "";
            this.skipBlank();
            if(this.str.charAt(this.idx) == 61) {
               ++this.idx;
               this.skipBlank();
               v = this.readNextIdentifier();
            } else if(this.str.charAt(this.idx) != 44 && this.str.charAt(this.idx) != 41) {
               this.throwSyntaxError("unexpected character '" + this.str.charAt(this.idx) + "'");
            }
         }

         throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", new Object[]{this.str, Integer.valueOf(this.idx)}));
      }
   }

   public List<AbstractType<?>> getTypeParameters() throws SyntaxException, ConfigurationException {
      List<AbstractType<?>> list = new ArrayList();
      if(this.isEOS()) {
         return list;
      } else if(this.str.charAt(this.idx) != 40) {
         throw new IllegalStateException();
      } else {
         ++this.idx;

         while(this.skipBlankAndComma()) {
            if(this.str.charAt(this.idx) == 41) {
               ++this.idx;
               return list;
            }

            try {
               list.add(this.parse());
            } catch (SyntaxException var4) {
               SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", new Object[]{this.str, Integer.valueOf(this.idx)}));
               ex.initCause(var4);
               throw ex;
            }
         }

         throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", new Object[]{this.str, Integer.valueOf(this.idx)}));
      }
   }

   public Map<Byte, AbstractType<?>> getAliasParameters() throws SyntaxException, ConfigurationException {
      Map<Byte, AbstractType<?>> map = new HashMap();
      if(this.isEOS()) {
         return map;
      } else if(this.str.charAt(this.idx) != 40) {
         throw new IllegalStateException();
      } else {
         ++this.idx;

         while(this.skipBlankAndComma()) {
            if(this.str.charAt(this.idx) == 41) {
               ++this.idx;
               return map;
            }

            String alias = this.readNextIdentifier();
            if(alias.length() != 1) {
               this.throwSyntaxError("An alias should be a single character");
            }

            char aliasChar = alias.charAt(0);
            if(aliasChar < 33 || aliasChar > 127) {
               this.throwSyntaxError("An alias should be a single character in [0..9a..bA..B-+._&]");
            }

            this.skipBlank();
            if(this.str.charAt(this.idx) != 61 || this.str.charAt(this.idx + 1) != 62) {
               this.throwSyntaxError("expecting '=>' token");
            }

            this.idx += 2;
            this.skipBlank();

            try {
               map.put(Byte.valueOf((byte)aliasChar), this.parse());
            } catch (SyntaxException var6) {
               SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", new Object[]{this.str, Integer.valueOf(this.idx)}));
               ex.initCause(var6);
               throw ex;
            }
         }

         throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", new Object[]{this.str, Integer.valueOf(this.idx)}));
      }
   }

   public Map<ByteBuffer, CollectionType> getCollectionsParameters() throws SyntaxException, ConfigurationException {
      Map<ByteBuffer, CollectionType> map = new HashMap();
      if(this.isEOS()) {
         return map;
      } else if(this.str.charAt(this.idx) != 40) {
         throw new IllegalStateException();
      } else {
         ++this.idx;

         while(this.skipBlankAndComma()) {
            if(this.str.charAt(this.idx) == 41) {
               ++this.idx;
               return map;
            }

            ByteBuffer bb = this.fromHex(this.readNextIdentifier());
            this.skipBlank();
            if(this.str.charAt(this.idx) != 58) {
               this.throwSyntaxError("expecting ':' token");
            }

            ++this.idx;
            this.skipBlank();

            try {
               AbstractType<?> type = this.parse();
               if(!(type instanceof CollectionType)) {
                  throw new SyntaxException(type + " is not a collection type");
               }

               map.put(bb, (CollectionType)type);
            } catch (SyntaxException var5) {
               SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", new Object[]{this.str, Integer.valueOf(this.idx)}));
               ex.initCause(var5);
               throw ex;
            }
         }

         throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", new Object[]{this.str, Integer.valueOf(this.idx)}));
      }
   }

   private ByteBuffer fromHex(String hex) throws SyntaxException {
      try {
         return ByteBufferUtil.hexToBytes(hex);
      } catch (NumberFormatException var3) {
         this.throwSyntaxError(var3.getMessage());
         return null;
      }
   }

   public Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> getUserTypeParameters() throws SyntaxException, ConfigurationException {
      if(!this.isEOS() && this.str.charAt(this.idx) == 40) {
         ++this.idx;
         this.skipBlankAndComma();
         String keyspace = this.readNextIdentifier();
         this.skipBlankAndComma();
         ByteBuffer typeName = this.fromHex(this.readNextIdentifier());
         ArrayList defs = new ArrayList();

         while(this.skipBlankAndComma()) {
            if(this.str.charAt(this.idx) == 41) {
               ++this.idx;
               return Pair.create(Pair.create(keyspace, typeName), defs);
            }

            ByteBuffer name = this.fromHex(this.readNextIdentifier());
            this.skipBlank();
            if(this.str.charAt(this.idx) != 58) {
               this.throwSyntaxError("expecting ':' token");
            }

            ++this.idx;
            this.skipBlank();

            try {
               AbstractType type = this.parse();
               defs.add(Pair.create(name, type));
            } catch (SyntaxException var7) {
               SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", new Object[]{this.str, Integer.valueOf(this.idx)}));
               ex.initCause(var7);
               throw ex;
            }
         }

         throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", new Object[]{this.str, Integer.valueOf(this.idx)}));
      } else {
         throw new IllegalStateException();
      }
   }

   private static AbstractType<?> getAbstractType(String compareWith) throws ConfigurationException {
      String className = compareWith.contains(".")?compareWith:"org.apache.cassandra.db.marshal." + compareWith;
      Class typeClass = FBUtilities.classForName(className, "abstract-type");

      try {
         Field field = typeClass.getDeclaredField("instance");
         return (AbstractType)field.get((Object)null);
      } catch (IllegalAccessException | NoSuchFieldException var4) {
         return getRawAbstractType(typeClass, EMPTY_PARSER);
      }
   }

   private static AbstractType<?> getAbstractType(String compareWith, TypeParser parser) throws SyntaxException, ConfigurationException {
      String className = compareWith.contains(".")?compareWith:"org.apache.cassandra.db.marshal." + compareWith;
      Class typeClass = FBUtilities.classForName(className, "abstract-type");

      try {
         Method method = typeClass.getDeclaredMethod("getInstance", new Class[]{TypeParser.class});
         return (AbstractType)method.invoke((Object)null, new Object[]{parser});
      } catch (IllegalAccessException | NoSuchMethodException var6) {
         AbstractType<?> type = getRawAbstractType(typeClass);
         return AbstractType.parseDefaultParameters(type, parser);
      } catch (InvocationTargetException var7) {
         ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
         ex.initCause(var7.getTargetException());
         throw ex;
      }
   }

   private static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass) throws ConfigurationException {
      try {
         Field field = typeClass.getDeclaredField("instance");
         return (AbstractType)field.get((Object)null);
      } catch (IllegalAccessException | NoSuchFieldException var2) {
         throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
      }
   }

   private static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass, TypeParser parser) throws ConfigurationException {
      try {
         Method method = typeClass.getDeclaredMethod("getInstance", new Class[]{TypeParser.class});
         return (AbstractType)method.invoke((Object)null, new Object[]{parser});
      } catch (IllegalAccessException | NoSuchMethodException var4) {
         throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
      } catch (InvocationTargetException var5) {
         ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
         ex.initCause(var5.getTargetException());
         throw ex;
      }
   }

   private void throwSyntaxError(String msg) throws SyntaxException {
      throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: %s", new Object[]{this.str, Integer.valueOf(this.idx), msg}));
   }

   private boolean isEOS() {
      return isEOS(this.str, this.idx);
   }

   private static boolean isEOS(String str, int i) {
      return i >= str.length();
   }

   private static boolean isBlank(int c) {
      return c == 32 || c == 9 || c == 10;
   }

   private void skipBlank() {
      this.idx = skipBlank(this.str, this.idx);
   }

   private static int skipBlank(String str, int i) {
      while(!isEOS(str, i) && isBlank(str.charAt(i))) {
         ++i;
      }

      return i;
   }

   private boolean skipBlankAndComma() {
      for(boolean commaFound = false; !this.isEOS(); ++this.idx) {
         int c = this.str.charAt(this.idx);
         if(c == 44) {
            if(commaFound) {
               return true;
            }

            commaFound = true;
         } else if(!isBlank(c)) {
            return true;
         }
      }

      return false;
   }

   private static boolean isIdentifierChar(int c) {
      return c >= 48 && c <= 57 || c >= 97 && c <= 122 || c >= 65 && c <= 90 || c == 45 || c == 43 || c == 46 || c == 95 || c == 38;
   }

   public String readNextIdentifier() {
      int i;
      for(i = this.idx; !this.isEOS() && isIdentifierChar(this.str.charAt(this.idx)); ++this.idx) {
         ;
      }

      return this.str.substring(i, this.idx);
   }

   public static String stringifyAliasesParameters(Map<Byte, AbstractType<?>> aliases) {
      StringBuilder sb = new StringBuilder();
      sb.append('(');
      Iterator<Entry<Byte, AbstractType<?>>> iter = aliases.entrySet().iterator();
      Entry entry;
      if(iter.hasNext()) {
         entry = (Entry)iter.next();
         sb.append((char)((Byte)entry.getKey()).byteValue()).append("=>").append(entry.getValue());
      }

      while(iter.hasNext()) {
         entry = (Entry)iter.next();
         sb.append(',').append((char)((Byte)entry.getKey()).byteValue()).append("=>").append(entry.getValue());
      }

      sb.append(')');
      return sb.toString();
   }

   public static String stringifyTypeParameters(List<AbstractType<?>> types) {
      return stringifyTypeParameters(types, false);
   }

   public static String stringifyTypeParameters(List<AbstractType<?>> types, boolean ignoreFreezing) {
      StringBuilder sb = new StringBuilder("(");

      for(int i = 0; i < types.size(); ++i) {
         if(i > 0) {
            sb.append(",");
         }

         sb.append(((AbstractType)types.get(i)).toString(ignoreFreezing));
      }

      return sb.append(')').toString();
   }

   public static String stringifyCollectionsParameters(Map<ByteBuffer, ? extends CollectionType> collections) {
      StringBuilder sb = new StringBuilder();
      sb.append('(');
      boolean first = true;
      Iterator var3 = collections.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<ByteBuffer, ? extends CollectionType> entry = (Entry)var3.next();
         if(!first) {
            sb.append(',');
         }

         first = false;
         sb.append(ByteBufferUtil.bytesToHex((ByteBuffer)entry.getKey())).append(":");
         sb.append(entry.getValue());
      }

      sb.append(')');
      return sb.toString();
   }

   public static String stringifyUserTypeParameters(String keysace, ByteBuffer typeName, List<FieldIdentifier> fields, List<AbstractType<?>> columnTypes, boolean ignoreFreezing) {
      StringBuilder sb = new StringBuilder();
      sb.append('(').append(keysace).append(",").append(ByteBufferUtil.bytesToHex(typeName));

      for(int i = 0; i < fields.size(); ++i) {
         sb.append(',');
         sb.append(ByteBufferUtil.bytesToHex(((FieldIdentifier)fields.get(i)).bytes)).append(":");
         sb.append(((AbstractType)columnTypes.get(i)).toString(ignoreFreezing));
      }

      sb.append(')');
      return sb.toString();
   }
}
