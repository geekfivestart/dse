package org.apache.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.AuthenticationScheme;
import com.datastax.bdp.cassandra.auth.AuthenticationSchemeResource;
import com.datastax.bdp.cassandra.auth.DseRowResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerSubmissionResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerWorkPoolResource;
import com.datastax.bdp.cassandra.auth.RpcResource;
import com.datastax.bdp.cassandra.cql3.RestrictRowsStatement;
import com.datastax.bdp.cassandra.cql3.RpcCallStatement;
import com.datastax.bdp.cassandra.cql3.UnRestrictRowsStatement;
import java.util.ArrayList;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.schema.ColumnMetadata.Raw;

public class Cql_DseCoreParser extends Parser {
   public static final int EOF = -1;
   public static final int T__217 = 217;
   public static final int T__218 = 218;
   public static final int T__219 = 219;
   public static final int T__220 = 220;
   public static final int T__221 = 221;
   public static final int T__222 = 222;
   public static final int T__223 = 223;
   public static final int T__224 = 224;
   public static final int T__225 = 225;
   public static final int T__226 = 226;
   public static final int T__227 = 227;
   public static final int T__228 = 228;
   public static final int T__229 = 229;
   public static final int T__230 = 230;
   public static final int T__231 = 231;
   public static final int T__232 = 232;
   public static final int T__233 = 233;
   public static final int T__234 = 234;
   public static final int T__235 = 235;
   public static final int T__236 = 236;
   public static final int T__237 = 237;
   public static final int T__238 = 238;
   public static final int T__239 = 239;
   public static final int T__240 = 240;
   public static final int T__241 = 241;
   public static final int A = 4;
   public static final int B = 5;
   public static final int BOOLEAN = 6;
   public static final int C = 7;
   public static final int COMMENT = 8;
   public static final int D = 9;
   public static final int DIGIT = 10;
   public static final int DURATION = 11;
   public static final int DURATION_UNIT = 12;
   public static final int E = 13;
   public static final int EMPTY_QUOTED_NAME = 14;
   public static final int EXPONENT = 15;
   public static final int F = 16;
   public static final int FLOAT = 17;
   public static final int G = 18;
   public static final int H = 19;
   public static final int HEX = 20;
   public static final int HEXNUMBER = 21;
   public static final int I = 22;
   public static final int IDENT = 23;
   public static final int INTEGER = 24;
   public static final int J = 25;
   public static final int K = 26;
   public static final int K_ADD = 27;
   public static final int K_AGGREGATE = 28;
   public static final int K_ALL = 29;
   public static final int K_ALLOW = 30;
   public static final int K_ALTER = 31;
   public static final int K_AND = 32;
   public static final int K_ANY = 33;
   public static final int K_APPLY = 34;
   public static final int K_AS = 35;
   public static final int K_ASC = 36;
   public static final int K_ASCII = 37;
   public static final int K_AUTHENTICATION = 38;
   public static final int K_AUTHORIZE = 39;
   public static final int K_BATCH = 40;
   public static final int K_BEGIN = 41;
   public static final int K_BIGINT = 42;
   public static final int K_BLOB = 43;
   public static final int K_BOOLEAN = 44;
   public static final int K_BY = 45;
   public static final int K_CALL = 46;
   public static final int K_CALLED = 47;
   public static final int K_CALLS = 48;
   public static final int K_CAST = 49;
   public static final int K_CLUSTERING = 50;
   public static final int K_COLUMNFAMILY = 51;
   public static final int K_COLUMNS = 52;
   public static final int K_COMMIT = 53;
   public static final int K_COMPACT = 54;
   public static final int K_CONFIG = 55;
   public static final int K_CONTAINS = 56;
   public static final int K_COUNT = 57;
   public static final int K_COUNTER = 58;
   public static final int K_CREATE = 59;
   public static final int K_CUSTOM = 60;
   public static final int K_DATE = 61;
   public static final int K_DECIMAL = 62;
   public static final int K_DEFAULT = 63;
   public static final int K_DELETE = 64;
   public static final int K_DESC = 65;
   public static final int K_DESCRIBE = 66;
   public static final int K_DETERMINISTIC = 67;
   public static final int K_DISTINCT = 68;
   public static final int K_DOUBLE = 69;
   public static final int K_DROP = 70;
   public static final int K_DURATION = 71;
   public static final int K_ENTRIES = 72;
   public static final int K_EXECUTE = 73;
   public static final int K_EXISTS = 74;
   public static final int K_FIELD = 75;
   public static final int K_FILTERING = 76;
   public static final int K_FINALFUNC = 77;
   public static final int K_FLOAT = 78;
   public static final int K_FOR = 79;
   public static final int K_FROM = 80;
   public static final int K_FROZEN = 81;
   public static final int K_FULL = 82;
   public static final int K_FUNCTION = 83;
   public static final int K_FUNCTIONS = 84;
   public static final int K_GRANT = 85;
   public static final int K_GROUP = 86;
   public static final int K_IF = 87;
   public static final int K_IN = 88;
   public static final int K_INDEX = 89;
   public static final int K_INDICES = 90;
   public static final int K_INET = 91;
   public static final int K_INITCOND = 92;
   public static final int K_INPUT = 93;
   public static final int K_INSERT = 94;
   public static final int K_INT = 95;
   public static final int K_INTERNAL = 96;
   public static final int K_INTO = 97;
   public static final int K_IS = 98;
   public static final int K_JSON = 99;
   public static final int K_KERBEROS = 100;
   public static final int K_KEY = 101;
   public static final int K_KEYS = 102;
   public static final int K_KEYSPACE = 103;
   public static final int K_KEYSPACES = 104;
   public static final int K_LANGUAGE = 105;
   public static final int K_LDAP = 106;
   public static final int K_LIKE = 107;
   public static final int K_LIMIT = 108;
   public static final int K_LIST = 109;
   public static final int K_LOGIN = 110;
   public static final int K_MAP = 111;
   public static final int K_MATERIALIZED = 112;
   public static final int K_MBEAN = 113;
   public static final int K_MBEANS = 114;
   public static final int K_METHOD = 115;
   public static final int K_MODIFY = 116;
   public static final int K_MONOTONIC = 117;
   public static final int K_NEGATIVE_INFINITY = 118;
   public static final int K_NEGATIVE_NAN = 119;
   public static final int K_NOLOGIN = 120;
   public static final int K_NORECURSIVE = 121;
   public static final int K_NOSUPERUSER = 122;
   public static final int K_NOT = 123;
   public static final int K_NULL = 124;
   public static final int K_OBJECT = 125;
   public static final int K_OF = 126;
   public static final int K_ON = 127;
   public static final int K_OPTIONS = 128;
   public static final int K_OR = 129;
   public static final int K_ORDER = 130;
   public static final int K_PARTITION = 131;
   public static final int K_PASSWORD = 132;
   public static final int K_PER = 133;
   public static final int K_PERMISSION = 134;
   public static final int K_PERMISSIONS = 135;
   public static final int K_POSITIVE_INFINITY = 136;
   public static final int K_POSITIVE_NAN = 137;
   public static final int K_PRIMARY = 138;
   public static final int K_PROFILES = 139;
   public static final int K_REBUILD = 140;
   public static final int K_RELOAD = 141;
   public static final int K_REMOTE = 142;
   public static final int K_RENAME = 143;
   public static final int K_REPLACE = 144;
   public static final int K_RESOURCE = 145;
   public static final int K_RESTRICT = 146;
   public static final int K_RETURNS = 147;
   public static final int K_REVOKE = 148;
   public static final int K_ROLE = 149;
   public static final int K_ROLES = 150;
   public static final int K_ROWS = 151;
   public static final int K_SCHEMA = 152;
   public static final int K_SCHEME = 153;
   public static final int K_SCHEMES = 154;
   public static final int K_SEARCH = 155;
   public static final int K_SELECT = 156;
   public static final int K_SET = 157;
   public static final int K_SFUNC = 158;
   public static final int K_SMALLINT = 159;
   public static final int K_STATIC = 160;
   public static final int K_STORAGE = 161;
   public static final int K_STYPE = 162;
   public static final int K_SUBMISSION = 163;
   public static final int K_SUPERUSER = 164;
   public static final int K_TEXT = 165;
   public static final int K_TIME = 166;
   public static final int K_TIMESTAMP = 167;
   public static final int K_TIMEUUID = 168;
   public static final int K_TINYINT = 169;
   public static final int K_TO = 170;
   public static final int K_TOKEN = 171;
   public static final int K_TRIGGER = 172;
   public static final int K_TRUNCATE = 173;
   public static final int K_TTL = 174;
   public static final int K_TUPLE = 175;
   public static final int K_TYPE = 176;
   public static final int K_UNLOGGED = 177;
   public static final int K_UNRESTRICT = 178;
   public static final int K_UNSET = 179;
   public static final int K_UPDATE = 180;
   public static final int K_USE = 181;
   public static final int K_USER = 182;
   public static final int K_USERS = 183;
   public static final int K_USING = 184;
   public static final int K_UUID = 185;
   public static final int K_VALUES = 186;
   public static final int K_VARCHAR = 187;
   public static final int K_VARINT = 188;
   public static final int K_VIEW = 189;
   public static final int K_WHERE = 190;
   public static final int K_WITH = 191;
   public static final int K_WORKPOOL = 192;
   public static final int K_WRITETIME = 193;
   public static final int L = 194;
   public static final int LETTER = 195;
   public static final int M = 196;
   public static final int MULTILINE_COMMENT = 197;
   public static final int N = 198;
   public static final int O = 199;
   public static final int P = 200;
   public static final int Q = 201;
   public static final int QMARK = 202;
   public static final int QUOTED_NAME = 203;
   public static final int R = 204;
   public static final int RANGE = 205;
   public static final int S = 206;
   public static final int STRING_LITERAL = 207;
   public static final int T = 208;
   public static final int U = 209;
   public static final int UUID = 210;
   public static final int V = 211;
   public static final int W = 212;
   public static final int WS = 213;
   public static final int X = 214;
   public static final int Y = 215;
   public static final int Z = 216;
   public CqlParser gCql;
   public CqlParser gParent;
   public static final BitSet FOLLOW_rpcCallStatement_in_dseCoreStatement31 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_restrictRowsStatement_in_dseCoreStatement57 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_unrestrictRowsStatement_in_dseCoreStatement78 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_dseRowResource_in_dseCoreResource110 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_authenticationSchemeResource_in_dseCoreResource122 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_rpcCallResource_in_dseCoreResource134 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_workPoolResource_in_dseCoreResource146 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_submissionResource_in_dseCoreResource158 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_CALL_in_rpcCallStatement192 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_rpcObjectName_in_rpcCallStatement196 = new BitSet(new long[]{0L, 0L, 0L, 17179869184L});
   public static final BitSet FOLLOW_226_in_rpcCallStatement198 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_rpcObjectName_in_rpcCallStatement202 = new BitSet(new long[]{0L, 0L, 0L, 134217728L});
   public static final BitSet FOLLOW_219_in_rpcCallStatement210 = new BitSet(new long[]{19007552L, 1206964700135292928L, 768L, 299136285180928L});
   public static final BitSet FOLLOW_value_in_rpcCallStatement216 = new BitSet(new long[]{19007552L, 1206964700135292928L, 768L, 299138432664576L});
   public static final BitSet FOLLOW_223_in_rpcCallStatement222 = new BitSet(new long[]{19007552L, 1206964700135292928L, 768L, 299136016745472L});
   public static final BitSet FOLLOW_value_in_rpcCallStatement226 = new BitSet(new long[]{19007552L, 1206964700135292928L, 768L, 299138432664576L});
   public static final BitSet FOLLOW_220_in_rpcCallStatement236 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_ALL_in_authenticationSchemeResource267 = new BitSet(new long[]{274877906944L});
   public static final BitSet FOLLOW_K_AUTHENTICATION_in_authenticationSchemeResource269 = new BitSet(new long[]{0L, 0L, 67108864L});
   public static final BitSet FOLLOW_K_SCHEMES_in_authenticationSchemeResource271 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_scheme_in_authenticationSchemeResource283 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_set_in_scheme308 = new BitSet(new long[]{0L, 0L, 33554432L});
   public static final BitSet FOLLOW_K_SCHEME_in_scheme326 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_ALL_in_rpcCallResource355 = new BitSet(new long[]{0L, 0L, 16384L});
   public static final BitSet FOLLOW_K_REMOTE_in_rpcCallResource357 = new BitSet(new long[]{281474976710656L});
   public static final BitSet FOLLOW_K_CALLS_in_rpcCallResource359 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_REMOTE_in_rpcCallResource369 = new BitSet(new long[]{0L, 2305843009213693952L});
   public static final BitSet FOLLOW_K_OBJECT_in_rpcCallResource371 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_rpcObjectName_in_rpcCallResource375 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_REMOTE_in_rpcCallResource385 = new BitSet(new long[]{0L, 2251799813685248L});
   public static final BitSet FOLLOW_K_METHOD_in_rpcCallResource387 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_rpcObjectName_in_rpcCallResource391 = new BitSet(new long[]{0L, 0L, 0L, 17179869184L});
   public static final BitSet FOLLOW_226_in_rpcCallResource393 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_rpcObjectName_in_rpcCallResource397 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_IDENT_in_rpcObjectName422 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_QUOTED_NAME_in_rpcObjectName447 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_unreserved_keyword_in_rpcObjectName466 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_QMARK_in_rpcObjectName476 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_ANY_in_workPoolResource501 = new BitSet(new long[]{0L, 0L, 0L, 1L});
   public static final BitSet FOLLOW_K_WORKPOOL_in_workPoolResource503 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_WORKPOOL_in_workPoolResource513 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
   public static final BitSet FOLLOW_STRING_LITERAL_in_workPoolResource517 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_ANY_in_submissionResource542 = new BitSet(new long[]{0L, 0L, 34359738368L});
   public static final BitSet FOLLOW_K_SUBMISSION_in_submissionResource544 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_ANY_in_submissionResource554 = new BitSet(new long[]{0L, 0L, 34359738368L});
   public static final BitSet FOLLOW_K_SUBMISSION_in_submissionResource556 = new BitSet(new long[]{0L, 16777216L});
   public static final BitSet FOLLOW_K_IN_in_submissionResource558 = new BitSet(new long[]{0L, 0L, 0L, 1L});
   public static final BitSet FOLLOW_K_WORKPOOL_in_submissionResource560 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
   public static final BitSet FOLLOW_STRING_LITERAL_in_submissionResource564 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_SUBMISSION_in_submissionResource574 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
   public static final BitSet FOLLOW_STRING_LITERAL_in_submissionResource578 = new BitSet(new long[]{0L, 16777216L});
   public static final BitSet FOLLOW_K_IN_in_submissionResource580 = new BitSet(new long[]{0L, 0L, 0L, 1L});
   public static final BitSet FOLLOW_K_WORKPOOL_in_submissionResource582 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
   public static final BitSet FOLLOW_STRING_LITERAL_in_submissionResource586 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_RESTRICT_in_restrictRowsStatement611 = new BitSet(new long[]{0L, 0L, 8388608L});
   public static final BitSet FOLLOW_K_ROWS_in_restrictRowsStatement613 = new BitSet(new long[]{0L, -9223372036854775808L});
   public static final BitSet FOLLOW_K_ON_in_restrictRowsStatement615 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_columnFamilyName_in_restrictRowsStatement627 = new BitSet(new long[]{0L, 0L, 72057594037927936L});
   public static final BitSet FOLLOW_K_USING_in_restrictRowsStatement635 = new BitSet(new long[]{-578751678455463936L, 2677653284601887928L, 2219663287022156025L, 2051L});
   public static final BitSet FOLLOW_cident_in_restrictRowsStatement647 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_K_UNRESTRICT_in_unrestrictRowsStatement678 = new BitSet(new long[]{0L, 0L, 8388608L});
   public static final BitSet FOLLOW_K_ROWS_in_unrestrictRowsStatement680 = new BitSet(new long[]{0L, -9223372036854775808L});
   public static final BitSet FOLLOW_K_ON_in_unrestrictRowsStatement682 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_columnFamilyName_in_unrestrictRowsStatement694 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_STRING_LITERAL_in_dseRowResource727 = new BitSet(new long[]{0L, 0L, 8388608L});
   public static final BitSet FOLLOW_K_ROWS_in_dseRowResource729 = new BitSet(new long[]{0L, 16777216L});
   public static final BitSet FOLLOW_K_IN_in_dseRowResource731 = new BitSet(new long[]{-576499878641795072L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dseRowResource735 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
   public static final BitSet FOLLOW_columnFamilyName_in_dseRowResource741 = new BitSet(new long[]{2L});

   public Cql_DseCoreParser(TokenStream input, CqlParser gCql) {
      this(input, new RecognizerSharedState(), gCql);
   }

   public Cql_DseCoreParser(TokenStream input, RecognizerSharedState state, CqlParser gCql) {
      super(input, state);
      this.gCql = gCql;
      this.gParent = gCql;
   }

   protected void addRecognitionError(String msg) {
      this.gParent.addRecognitionError(msg);
   }

   public final AuthenticationSchemeResource authenticationSchemeResource() throws RecognitionException {
      AuthenticationSchemeResource res = null;
      AuthenticationScheme sc = null;

      try {
         try {
            int alt5 = true;
            int LA5_0 = this.input.LA(1);
            byte alt5;
            if(LA5_0 == 29) {
               alt5 = 1;
            } else {
               if(LA5_0 != 96 && LA5_0 != 100 && LA5_0 != 106) {
                  NoViableAltException nvae = new NoViableAltException("", 5, 0, this.input);
                  throw nvae;
               }

               alt5 = 2;
            }

            switch(alt5) {
            case 1:
               this.match(this.input, 29, FOLLOW_K_ALL_in_authenticationSchemeResource267);
               this.match(this.input, 38, FOLLOW_K_AUTHENTICATION_in_authenticationSchemeResource269);
               this.match(this.input, 154, FOLLOW_K_SCHEMES_in_authenticationSchemeResource271);
               res = AuthenticationSchemeResource.root();
               break;
            case 2:
               this.pushFollow(FOLLOW_scheme_in_authenticationSchemeResource283);
               sc = this.scheme();
               --this.state._fsp;
               res = AuthenticationSchemeResource.scheme(sc);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return res;
      } finally {
         ;
      }
   }

   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
      this.gParent.displayRecognitionError(tokenNames, e);
   }

   public final IResource dseCoreResource() throws RecognitionException {
      IResource res = null;
      DseRowResource w = null;
      AuthenticationSchemeResource a = null;
      RpcResource r = null;
      ResourceManagerWorkPoolResource wpr = null;
      ResourceManagerSubmissionResource sr = null;

      try {
         try {
            int alt2 = true;
            int LA2_5;
            int nvaeMark;
            NoViableAltException nvae;
            byte alt2;
            switch(this.input.LA(1)) {
            case 29:
               LA2_5 = this.input.LA(2);
               if(LA2_5 == 38) {
                  alt2 = 2;
               } else {
                  if(LA2_5 != 142) {
                     nvaeMark = this.input.mark();

                     try {
                        this.input.consume();
                        nvae = new NoViableAltException("", 2, 2, this.input);
                        throw nvae;
                     } finally {
                        this.input.rewind(nvaeMark);
                     }
                  }

                  alt2 = 3;
               }
               break;
            case 33:
               LA2_5 = this.input.LA(2);
               if(LA2_5 == 192) {
                  alt2 = 4;
               } else {
                  if(LA2_5 != 163) {
                     nvaeMark = this.input.mark();

                     try {
                        this.input.consume();
                        nvae = new NoViableAltException("", 2, 5, this.input);
                        throw nvae;
                     } finally {
                        this.input.rewind(nvaeMark);
                     }
                  }

                  alt2 = 5;
               }
               break;
            case 96:
            case 100:
            case 106:
               alt2 = 2;
               break;
            case 142:
               alt2 = 3;
               break;
            case 163:
               alt2 = 5;
               break;
            case 192:
               alt2 = 4;
               break;
            case 207:
               alt2 = 1;
               break;
            default:
               NoViableAltException nvae = new NoViableAltException("", 2, 0, this.input);
               throw nvae;
            }

            switch(alt2) {
            case 1:
               this.pushFollow(FOLLOW_dseRowResource_in_dseCoreResource110);
               w = this.dseRowResource();
               --this.state._fsp;
               res = w;
               break;
            case 2:
               this.pushFollow(FOLLOW_authenticationSchemeResource_in_dseCoreResource122);
               a = this.authenticationSchemeResource();
               --this.state._fsp;
               res = a;
               break;
            case 3:
               this.pushFollow(FOLLOW_rpcCallResource_in_dseCoreResource134);
               r = this.rpcCallResource();
               --this.state._fsp;
               res = r;
               break;
            case 4:
               this.pushFollow(FOLLOW_workPoolResource_in_dseCoreResource146);
               wpr = this.workPoolResource();
               --this.state._fsp;
               res = wpr;
               break;
            case 5:
               this.pushFollow(FOLLOW_submissionResource_in_dseCoreResource158);
               sr = this.submissionResource();
               --this.state._fsp;
               res = sr;
            }
         } catch (RecognitionException var28) {
            this.reportError(var28);
            this.recover(this.input, var28);
         }

         return (IResource)res;
      } finally {
         ;
      }
   }

   public final ParsedStatement dseCoreStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      ParsedStatement st1 = null;
      RestrictRowsStatement st2 = null;
      UnRestrictRowsStatement st3 = null;

      try {
         try {
            int alt1 = true;
            byte alt1;
            switch(this.input.LA(1)) {
            case 46:
               alt1 = 1;
               break;
            case 146:
               alt1 = 2;
               break;
            case 178:
               alt1 = 3;
               break;
            default:
               NoViableAltException nvae = new NoViableAltException("", 1, 0, this.input);
               throw nvae;
            }

            switch(alt1) {
            case 1:
               this.pushFollow(FOLLOW_rpcCallStatement_in_dseCoreStatement31);
               st1 = this.rpcCallStatement();
               --this.state._fsp;
               stmt = st1;
               break;
            case 2:
               this.pushFollow(FOLLOW_restrictRowsStatement_in_dseCoreStatement57);
               st2 = this.restrictRowsStatement();
               --this.state._fsp;
               stmt = st2;
               break;
            case 3:
               this.pushFollow(FOLLOW_unrestrictRowsStatement_in_dseCoreStatement78);
               st3 = this.unrestrictRowsStatement();
               --this.state._fsp;
               stmt = st3;
            }
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return (ParsedStatement)stmt;
      } finally {
         ;
      }
   }

   public final DseRowResource dseRowResource() throws RecognitionException {
      DseRowResource res = null;
      Token f = null;
      CFName cf = null;

      try {
         try {
            f = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_dseRowResource727);
            this.match(this.input, 151, FOLLOW_K_ROWS_in_dseRowResource729);
            this.match(this.input, 88, FOLLOW_K_IN_in_dseRowResource731);
            int alt10 = 2;
            int LA10_0 = this.input.LA(1);
            if(LA10_0 == 51) {
               alt10 = 1;
            }

            switch(alt10) {
            case 1:
               this.match(this.input, 51, FOLLOW_K_COLUMNFAMILY_in_dseRowResource735);
            default:
               this.pushFollow(FOLLOW_columnFamilyName_in_dseRowResource741);
               cf = this.gCql.columnFamilyName();
               --this.state._fsp;
               res = DseRowResource.table(cf.getKeyspace(), cf.getColumnFamily(), f != null?f.getText():null);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return res;
      } finally {
         ;
      }
   }

   public Parser[] getDelegates() {
      return new Parser[0];
   }

   public String getGrammarFileName() {
      return "DseCoreParser.g";
   }

   public String[] getTokenNames() {
      return CqlParser.tokenNames;
   }

   public final RestrictRowsStatement restrictRowsStatement() throws RecognitionException {
      RestrictRowsStatement stmt = null;
      CFName cf = null;
      Raw c = null;

      try {
         try {
            this.match(this.input, 146, FOLLOW_K_RESTRICT_in_restrictRowsStatement611);
            this.match(this.input, 151, FOLLOW_K_ROWS_in_restrictRowsStatement613);
            this.match(this.input, 127, FOLLOW_K_ON_in_restrictRowsStatement615);
            this.pushFollow(FOLLOW_columnFamilyName_in_restrictRowsStatement627);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            this.match(this.input, 184, FOLLOW_K_USING_in_restrictRowsStatement635);
            this.pushFollow(FOLLOW_cident_in_restrictRowsStatement647);
            c = this.gCql.cident();
            --this.state._fsp;
            stmt = new RestrictRowsStatement(cf, c);
         } catch (RecognitionException var8) {
            this.reportError(var8);
            this.recover(this.input, var8);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final RpcResource rpcCallResource() throws RecognitionException {
      RpcResource res = null;
      String ro = null;
      String rm = null;

      try {
         try {
            int alt6 = true;
            int LA6_0 = this.input.LA(1);
            byte alt6;
            if(LA6_0 == 29) {
               alt6 = 1;
            } else {
               if(LA6_0 != 142) {
                  NoViableAltException nvae = new NoViableAltException("", 6, 0, this.input);
                  throw nvae;
               }

               int LA6_2 = this.input.LA(2);
               if(LA6_2 == 125) {
                  alt6 = 2;
               } else {
                  if(LA6_2 != 115) {
                     int nvaeMark = this.input.mark();

                     try {
                        this.input.consume();
                        NoViableAltException nvae = new NoViableAltException("", 6, 2, this.input);
                        throw nvae;
                     } finally {
                        this.input.rewind(nvaeMark);
                     }
                  }

                  alt6 = 3;
               }
            }

            switch(alt6) {
            case 1:
               this.match(this.input, 29, FOLLOW_K_ALL_in_rpcCallResource355);
               this.match(this.input, 142, FOLLOW_K_REMOTE_in_rpcCallResource357);
               this.match(this.input, 48, FOLLOW_K_CALLS_in_rpcCallResource359);
               res = RpcResource.root();
               break;
            case 2:
               this.match(this.input, 142, FOLLOW_K_REMOTE_in_rpcCallResource369);
               this.match(this.input, 125, FOLLOW_K_OBJECT_in_rpcCallResource371);
               this.pushFollow(FOLLOW_rpcObjectName_in_rpcCallResource375);
               ro = this.rpcObjectName();
               --this.state._fsp;
               res = RpcResource.object(ro);
               break;
            case 3:
               this.match(this.input, 142, FOLLOW_K_REMOTE_in_rpcCallResource385);
               this.match(this.input, 115, FOLLOW_K_METHOD_in_rpcCallResource387);
               this.pushFollow(FOLLOW_rpcObjectName_in_rpcCallResource391);
               ro = this.rpcObjectName();
               --this.state._fsp;
               this.match(this.input, 226, FOLLOW_226_in_rpcCallResource393);
               this.pushFollow(FOLLOW_rpcObjectName_in_rpcCallResource397);
               rm = this.rpcObjectName();
               --this.state._fsp;
               res = RpcResource.method(ro, rm);
            }
         } catch (RecognitionException var18) {
            this.reportError(var18);
            this.recover(this.input, var18);
         }

         return res;
      } finally {
         ;
      }
   }

   public final ParsedStatement rpcCallStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      String ro = null;
      String rm = null;
      org.apache.cassandra.cql3.Term.Raw v1 = null;
      org.apache.cassandra.cql3.Term.Raw vn = null;
      ArrayList values = new ArrayList();

      try {
         this.match(this.input, 46, FOLLOW_K_CALL_in_rpcCallStatement192);
         this.pushFollow(FOLLOW_rpcObjectName_in_rpcCallStatement196);
         ro = this.rpcObjectName();
         --this.state._fsp;
         this.match(this.input, 226, FOLLOW_226_in_rpcCallStatement198);
         this.pushFollow(FOLLOW_rpcObjectName_in_rpcCallStatement202);
         rm = this.rpcObjectName();
         --this.state._fsp;
         this.match(this.input, 219, FOLLOW_219_in_rpcCallStatement210);

         label159:
         while(true) {
            int alt4 = 2;
            int LA4_0 = this.input.LA(1);
            if(LA4_0 == 6 || LA4_0 == 11 || LA4_0 == 17 || LA4_0 == 21 || LA4_0 == 24 || LA4_0 >= 118 && LA4_0 <= 119 || LA4_0 == 124 || LA4_0 >= 136 && LA4_0 <= 137 || LA4_0 == 202 || LA4_0 == 207 || LA4_0 == 210 || LA4_0 == 219 || LA4_0 == 228 || LA4_0 == 236 || LA4_0 == 240) {
               alt4 = 1;
            }

            switch(alt4) {
            case 1:
               this.pushFollow(FOLLOW_value_in_rpcCallStatement216);
               v1 = this.gCql.value();
               --this.state._fsp;
               values.add(v1);

               while(true) {
                  int alt3 = 2;
                  int LA3_0 = this.input.LA(1);
                  if(LA3_0 == 223) {
                     alt3 = 1;
                  }

                  switch(alt3) {
                  case 1:
                     this.match(this.input, 223, FOLLOW_223_in_rpcCallStatement222);
                     this.pushFollow(FOLLOW_value_in_rpcCallStatement226);
                     vn = this.gCql.value();
                     --this.state._fsp;
                     values.add(vn);
                     break;
                  default:
                     continue label159;
                  }
               }
            default:
               this.match(this.input, 220, FOLLOW_220_in_rpcCallStatement236);
               stmt = new RpcCallStatement(ro, rm, values);
               return stmt;
            }
         }
      } catch (RecognitionException var14) {
         this.reportError(var14);
         this.recover(this.input, var14);
         return stmt;
      } finally {
         ;
      }
   }

   public final String rpcObjectName() throws RecognitionException {
      String id = null;
      Token t = null;
      String k = null;

      try {
         try {
            int alt7 = true;
            byte alt7;
            switch(this.input.LA(1)) {
            case 23:
               alt7 = 1;
               break;
            case 24:
            case 25:
            case 26:
            case 27:
            case 30:
            case 31:
            case 32:
            case 34:
            case 36:
            case 39:
            case 40:
            case 41:
            case 45:
            case 51:
            case 59:
            case 64:
            case 65:
            case 66:
            case 70:
            case 72:
            case 73:
            case 79:
            case 80:
            case 82:
            case 85:
            case 87:
            case 88:
            case 89:
            case 94:
            case 97:
            case 98:
            case 103:
            case 108:
            case 112:
            case 113:
            case 114:
            case 116:
            case 118:
            case 119:
            case 121:
            case 123:
            case 124:
            case 126:
            case 127:
            case 129:
            case 130:
            case 136:
            case 137:
            case 138:
            case 143:
            case 144:
            case 148:
            case 156:
            case 157:
            case 170:
            case 171:
            case 173:
            case 177:
            case 180:
            case 181:
            case 184:
            case 189:
            case 190:
            case 191:
            case 194:
            case 195:
            case 196:
            case 197:
            case 198:
            case 199:
            case 200:
            case 201:
            default:
               NoViableAltException nvae = new NoViableAltException("", 7, 0, this.input);
               throw nvae;
            case 28:
            case 29:
            case 33:
            case 35:
            case 37:
            case 38:
            case 42:
            case 43:
            case 44:
            case 46:
            case 47:
            case 48:
            case 49:
            case 50:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 60:
            case 61:
            case 62:
            case 63:
            case 67:
            case 68:
            case 69:
            case 71:
            case 74:
            case 75:
            case 76:
            case 77:
            case 78:
            case 81:
            case 83:
            case 84:
            case 86:
            case 90:
            case 91:
            case 92:
            case 93:
            case 95:
            case 96:
            case 99:
            case 100:
            case 101:
            case 102:
            case 104:
            case 105:
            case 106:
            case 107:
            case 109:
            case 110:
            case 111:
            case 115:
            case 117:
            case 120:
            case 122:
            case 125:
            case 128:
            case 131:
            case 132:
            case 133:
            case 134:
            case 135:
            case 139:
            case 140:
            case 141:
            case 142:
            case 145:
            case 146:
            case 147:
            case 149:
            case 150:
            case 151:
            case 152:
            case 153:
            case 154:
            case 155:
            case 158:
            case 159:
            case 160:
            case 161:
            case 162:
            case 163:
            case 164:
            case 165:
            case 166:
            case 167:
            case 168:
            case 169:
            case 172:
            case 174:
            case 175:
            case 176:
            case 178:
            case 179:
            case 182:
            case 183:
            case 185:
            case 186:
            case 187:
            case 188:
            case 192:
            case 193:
               alt7 = 3;
               break;
            case 202:
               alt7 = 4;
               break;
            case 203:
               alt7 = 2;
            }

            switch(alt7) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_rpcObjectName422);
               id = t != null?t.getText():null;
               break;
            case 2:
               t = (Token)this.match(this.input, 203, FOLLOW_QUOTED_NAME_in_rpcObjectName447);
               id = t != null?t.getText():null;
               break;
            case 3:
               this.pushFollow(FOLLOW_unreserved_keyword_in_rpcObjectName466);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               id = k;
               break;
            case 4:
               this.match(this.input, 202, FOLLOW_QMARK_in_rpcObjectName476);
               this.gParent.addRecognitionError("Bind variables cannot be used for rpc object names");
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return id;
      } finally {
         ;
      }
   }

   public final AuthenticationScheme scheme() throws RecognitionException {
      AuthenticationScheme scheme = null;
      Token s = null;

      try {
         try {
            s = this.input.LT(1);
            if(this.input.LA(1) != 96 && this.input.LA(1) != 100 && this.input.LA(1) != 106) {
               MismatchedSetException mse = new MismatchedSetException((BitSet)null, this.input);
               throw mse;
            }

            this.input.consume();
            this.state.errorRecovery = false;
            this.match(this.input, 153, FOLLOW_K_SCHEME_in_scheme326);
            scheme = AuthenticationScheme.valueOf((s != null?s.getText():null).toUpperCase());
         } catch (RecognitionException var7) {
            this.reportError(var7);
            this.recover(this.input, var7);
         }

         return scheme;
      } finally {
         ;
      }
   }

   public final ResourceManagerSubmissionResource submissionResource() throws RecognitionException {
      ResourceManagerSubmissionResource res = null;
      Token workPoolName = null;
      Token submissionId = null;

      try {
         try {
            int alt9 = true;
            int LA9_0 = this.input.LA(1);
            byte alt9;
            if(LA9_0 == 33) {
               int LA9_1 = this.input.LA(2);
               int LA9_3;
               if(LA9_1 != 163) {
                  LA9_3 = this.input.mark();

                  try {
                     this.input.consume();
                     NoViableAltException nvae = new NoViableAltException("", 9, 1, this.input);
                     throw nvae;
                  } finally {
                     this.input.rewind(LA9_3);
                  }
               }

               LA9_3 = this.input.LA(3);
               if(LA9_3 == 88) {
                  alt9 = 2;
               } else {
                  if(LA9_3 != -1 && LA9_3 != 80 && LA9_3 != 121 && LA9_3 != 126 && LA9_3 != 170 && LA9_3 != 229) {
                     int nvaeMark = this.input.mark();

                     try {
                        for(int nvaeConsume = 0; nvaeConsume < 2; ++nvaeConsume) {
                           this.input.consume();
                        }

                        NoViableAltException nvae = new NoViableAltException("", 9, 3, this.input);
                        throw nvae;
                     } finally {
                        this.input.rewind(nvaeMark);
                     }
                  }

                  alt9 = 1;
               }
            } else {
               if(LA9_0 != 163) {
                  NoViableAltException nvae = new NoViableAltException("", 9, 0, this.input);
                  throw nvae;
               }

               alt9 = 3;
            }

            switch(alt9) {
            case 1:
               this.match(this.input, 33, FOLLOW_K_ANY_in_submissionResource542);
               this.match(this.input, 163, FOLLOW_K_SUBMISSION_in_submissionResource544);
               res = ResourceManagerSubmissionResource.root();
               break;
            case 2:
               this.match(this.input, 33, FOLLOW_K_ANY_in_submissionResource554);
               this.match(this.input, 163, FOLLOW_K_SUBMISSION_in_submissionResource556);
               this.match(this.input, 88, FOLLOW_K_IN_in_submissionResource558);
               this.match(this.input, 192, FOLLOW_K_WORKPOOL_in_submissionResource560);
               workPoolName = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_submissionResource564);
               res = ResourceManagerSubmissionResource.workPoolFromSpec(workPoolName != null?workPoolName.getText():null);
               break;
            case 3:
               this.match(this.input, 163, FOLLOW_K_SUBMISSION_in_submissionResource574);
               submissionId = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_submissionResource578);
               this.match(this.input, 88, FOLLOW_K_IN_in_submissionResource580);
               this.match(this.input, 192, FOLLOW_K_WORKPOOL_in_submissionResource582);
               workPoolName = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_submissionResource586);
               res = ResourceManagerSubmissionResource.submissionFromSpec(workPoolName != null?workPoolName.getText():null, submissionId != null?submissionId.getText():null);
            }
         } catch (RecognitionException var27) {
            this.reportError(var27);
            this.recover(this.input, var27);
         }

         return res;
      } finally {
         ;
      }
   }

   public final UnRestrictRowsStatement unrestrictRowsStatement() throws RecognitionException {
      UnRestrictRowsStatement stmt = null;
      CFName cf = null;

      try {
         try {
            this.match(this.input, 178, FOLLOW_K_UNRESTRICT_in_unrestrictRowsStatement678);
            this.match(this.input, 151, FOLLOW_K_ROWS_in_unrestrictRowsStatement680);
            this.match(this.input, 127, FOLLOW_K_ON_in_unrestrictRowsStatement682);
            this.pushFollow(FOLLOW_columnFamilyName_in_unrestrictRowsStatement694);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            stmt = new UnRestrictRowsStatement(cf);
         } catch (RecognitionException var7) {
            this.reportError(var7);
            this.recover(this.input, var7);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final ResourceManagerWorkPoolResource workPoolResource() throws RecognitionException {
      ResourceManagerWorkPoolResource res = null;
      Token workPoolName = null;

      try {
         try {
            int alt8 = true;
            int LA8_0 = this.input.LA(1);
            byte alt8;
            if(LA8_0 == 33) {
               alt8 = 1;
            } else {
               if(LA8_0 != 192) {
                  NoViableAltException nvae = new NoViableAltException("", 8, 0, this.input);
                  throw nvae;
               }

               alt8 = 2;
            }

            switch(alt8) {
            case 1:
               this.match(this.input, 33, FOLLOW_K_ANY_in_workPoolResource501);
               this.match(this.input, 192, FOLLOW_K_WORKPOOL_in_workPoolResource503);
               res = ResourceManagerWorkPoolResource.root();
               break;
            case 2:
               this.match(this.input, 192, FOLLOW_K_WORKPOOL_in_workPoolResource513);
               workPoolName = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_workPoolResource517);
               res = ResourceManagerWorkPoolResource.workPoolFromSpec(workPoolName != null?workPoolName.getText():null);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return res;
      } finally {
         ;
      }
   }
}
