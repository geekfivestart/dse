package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UTF8Serializer extends AbstractTextSerializer {
   public static final UTF8Serializer instance = new UTF8Serializer();

   private UTF8Serializer() {
      super(StandardCharsets.UTF_8);
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(!UTF8Serializer.UTF8Validator.validate(bytes)) {
         throw new MarshalException("String didn't validate.");
      }
   }

   static class UTF8Validator {
      UTF8Validator() {
      }

      static boolean validate(ByteBuffer buf) {
         if (buf == null) {
            return false;
         }
         buf = buf.slice();
         int b = 0;
         State state = State.START;
         while (buf.remaining() > 0) {
            b = buf.get();
            switch (state) {
               case START: {
                  if (b >= 0) {
                     if (b > 127) {
                        return false;
                     }
                     continue;
                  }
                  else if (b >> 5 == -2) {
                     if (b == -64) {
                        state = State.TWO_80;
                        continue;
                     }
                     if ((b & 0x1E) == 0x0) {
                        return false;
                     }
                     state = State.TWO;
                     continue;
                  }
                  else if (b >> 4 == -2) {
                     if (b == -32) {
                        state = State.THREE_a0bf;
                        continue;
                     }
                     state = State.THREE_80bf_2;
                     continue;
                  }
                  else {
                     if (b >> 3 != -2) {
                        return false;
                     }
                     if (b == -16) {
                        state = State.FOUR_90bf;
                        continue;
                     }
                     state = State.FOUR_80bf_3;
                     continue;
                  }
               }
               case TWO: {
                  if ((b & 0xC0) != 0x80) {
                     return false;
                  }
                  state = State.START;
                  continue;
               }
               case TWO_80: {
                  if (b != -128) {
                     return false;
                  }
                  state = State.START;
                  continue;
               }
               case THREE_a0bf: {
                  if ((b & 0xE0) == 0x80) {
                     return false;
                  }
                  state = State.THREE_80bf_1;
                  continue;
               }
               case THREE_80bf_1: {
                  if ((b & 0xC0) != 0x80) {
                     return false;
                  }
                  state = State.START;
                  continue;
               }
               case THREE_80bf_2: {
                  if ((b & 0xC0) != 0x80) {
                     return false;
                  }
                  state = State.THREE_80bf_1;
                  continue;
               }
               case FOUR_90bf: {
                  if ((b & 0x30) == 0x0) {
                     return false;
                  }
                  state = State.THREE_80bf_2;
                  continue;
               }
               case FOUR_80bf_3: {
                  if ((b & 0xC0) != 0x80) {
                     return false;
                  }
                  state = State.THREE_80bf_2;
                  continue;
               }
               default: {
                  return false;
               }
            }
         }
         return state == State.START;
      }

      static enum State {
         START,
         TWO,
         TWO_80,
         THREE_a0bf,
         THREE_80bf_1,
         THREE_80bf_2,
         FOUR_90bf,
         FOUR_80bf_3;

         private State() {
         }
      }
   }
}
