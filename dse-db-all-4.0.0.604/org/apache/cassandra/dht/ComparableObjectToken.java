package org.apache.cassandra.dht;

abstract class ComparableObjectToken<C extends Comparable<C>> extends Token {
   private static final long serialVersionUID = 1L;
   final C token;

   protected ComparableObjectToken(C token) {
      this.token = token;
   }

   public C getTokenValue() {
      return this.token;
   }

   public String toString() {
      return this.token.toString();
   }

   public boolean equals(Object obj) {
      return this == obj?true:(obj != null && this.getClass() == obj.getClass()?this.token.equals(((ComparableObjectToken)obj).token):false);
   }

   public int hashCode() {
      return this.token.hashCode();
   }

   public int compareTo(Token o) {
      if(o.getClass() != this.getClass()) {
         throw new IllegalArgumentException("Invalid type of Token.compareTo() argument.");
      } else {
         return this.token.compareTo(((ComparableObjectToken)o).token);
      }
   }

   public double size(Token next) {
      throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.", new Object[]{this.getClass().getSimpleName()}));
   }

   public Token increaseSlightly() {
      throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.", new Object[]{this.getClass().getSimpleName()}));
   }
}
