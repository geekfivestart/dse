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
         if(buf == null) {
            return false;
         } else {
            buf = buf.slice();
            int b = false;
            UTF8Serializer.UTF8Validator.State state = UTF8Serializer.UTF8Validator.State.START;

            while(buf.remaining() > 0) {
               int b = buf.get();
               switch(null.$SwitchMap$org$apache$cassandra$serializers$UTF8Serializer$UTF8Validator$State[state.ordinal()]) {
               case 1:
                  if(b >= 0) {
                     if(b > 127) {
                        return false;
                     }
                  } else if(b >> 5 == -2) {
                     if(b == -64) {
                        state = UTF8Serializer.UTF8Validator.State.TWO_80;
                     } else {
                        if((b & 30) == 0) {
                           return false;
                        }

                        state = UTF8Serializer.UTF8Validator.State.TWO;
                     }
                  } else if(b >> 4 == -2) {
                     if(b == -32) {
                        state = UTF8Serializer.UTF8Validator.State.THREE_a0bf;
                     } else {
                        state = UTF8Serializer.UTF8Validator.State.THREE_80bf_2;
                     }
                  } else {
                     if(b >> 3 != -2) {
                        return false;
                     }

                     if(b == -16) {
                        state = UTF8Serializer.UTF8Validator.State.FOUR_90bf;
                     } else {
                        state = UTF8Serializer.UTF8Validator.State.FOUR_80bf_3;
                     }
                  }
                  break;
               case 2:
                  if((b & 192) != 128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.START;
                  break;
               case 3:
                  if(b != -128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.START;
                  break;
               case 4:
                  if((b & 224) == 128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.THREE_80bf_1;
                  break;
               case 5:
                  if((b & 192) != 128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.START;
                  break;
               case 6:
                  if((b & 192) != 128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.THREE_80bf_1;
                  break;
               case 7:
                  if((b & 48) == 0) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.THREE_80bf_2;
                  break;
               case 8:
                  if((b & 192) != 128) {
                     return false;
                  }

                  state = UTF8Serializer.UTF8Validator.State.THREE_80bf_2;
                  break;
               default:
                  return false;
               }
            }

            return state == UTF8Serializer.UTF8Validator.State.START;
         }
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
