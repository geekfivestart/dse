package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.DFA;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;

public class CqlLexer extends Lexer {
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
   public static final int Tokens = 242;
   List<Token> tokens;
   private final List<ErrorListener> listeners;
   public Cql_Lexer gLexer;
   protected CqlLexer.DFA1 dfa1;
   static final String DFA1_eotS = "\u0005\uffff\u0001&\u0001\uffff\u0001(\u0001)\u0001*\u0002\uffff\u0001,\u0001\uffff\u0001.\u0004\uffff\u0001#\u0002\uffff\r#\u0001\uffff\u0001#\n\uffff\u0015#\u0001\\\u0017#\u0001\uffff\u0005#\u0001y\u0004#\u0001~\u0002#\u0001\u0081\u0007#\u0001\uffff\u0006#\u0001\uffff\u0004#\u0001\uffff\u0002#\u0001\uffff\u0001\u0097\u0004#\u0001\u009c\u0001\u009d\u0002#\u0001 \u0001¢\u0001£\u0004#\u0001¨\u0001©\u0001#\u0001«\u0001¬\u0001\uffff\u0001\u00ad\u0001#\u0001¯\u0001#\u0002\uffff\u0002#\u0001\uffff\u0001³\u0002\uffff\u0002#\u0001¶\u0001#\u0002\uffff\u0001¸\u0003\uffff\u0001¹\u0001\uffff\u0003#\u0001\uffff\u0001#\u0001¾\u0001\uffff\u0001¿\u0002\uffff\u0001À\u0001Á\u0002#\u0004\uffff\u0001#\u0001Å\u0001#\u0001\uffff\u0002#\u0001É\u0001\uffff";
   static final String DFA1_eofS = "Ê\uffff";
   static final String DFA1_minS = "\u0001\t\u0004\uffff\u0001=\u0001\uffff\u0001-\u0001.\u0001*\u0002\uffff\u0001=\u0001\uffff\u0001=\u0004\uffff\u0001x\u0002\uffff\u0001N\u0001C\u0001N\u0001D\u0002E\u0001B\u0001E\u0001A\u0001R\u0001N\u0001I\u0001O\u0001\uffff\u0001R\n\uffff\u0001p\u0001T\u0001Y\u0001H\u0001A\u0001B\u0001D\u0001A\u0001R\u0001B\u0001W\u0001J\u0001T\u0002L\u0001O\u0001S\u0001E\u0001R\u0001r\u0001H\u00010\u0001E\u0001R\u0001M\u0001E\u0001I\u0001P\u0001B\u0002O\u0001U\u0001S\u0001E\u0001H\u0001L\u0001F\u0001U\u0001M\u0001F\u0001E\u0001L\u0001K\u0001(\u0001E\u0001\uffff\u0001M\u0001C\u0001I\u0001R\u0001C\u00010\u0001E\u0001T\u0001A\u0001I\u00010\u0001C\u0001O\u00010\u0001I\u0001M\u0002I\u0001T\u0001D\u0001P\u0001\uffff\u0001N\u0001A\u0001H\u0001S\u0001N\u0001E\u0001\uffff\u0001R\u0001E\u0001D\u0001L\u0001\uffff\u0001T\u0001D\u0001\uffff\u00010\u0001G\u0001N\u0001T\u0001L\u00020\u0001O\u0001T\u00030\u0001S\u0001A\u0001S\u0001O\u00020\u0001D\u00020\u0001\uffff\u00010\u0001S\u00010\u0001E\u0002\uffff\u0001O\u0001I\u0001\uffff\u00010\u0002\uffff\u0001I\u0001L\u00010\u0001S\u0002\uffff\u00010\u0003\uffff\u00010\u0001\uffff\u0001S\u0001L\u0001C\u0001\uffff\u0001O\u00010\u0001\uffff\u00010\u0002\uffff\u00020\u0001A\u0001N\u0004\uffff\u0001T\u00010\u0001I\u0001\uffff\u0001O\u0001N\u00010\u0001\uffff";
   static final String DFA1_maxS = "\u0001}\u0004\uffff\u0001=\u0001\uffff\u0001n\u0001.\u0001/\u0002\uffff\u0001=\u0001\uffff\u0001=\u0004\uffff\u0001x\u0002\uffff\u0002u\u0001n\u0001d\u0001e\u0001o\u0001b\u0001e\u0001o\u0001r\u0001n\u0001i\u0001o\u0001\uffff\u0001r\n\uffff\u0001p\u0001t\u0001y\u0001h\u0001a\u0001b\u0001t\u0001a\u0001r\u0001m\u0001w\u0001j\u0001t\u0001l\u0001n\u0001o\u0001s\u0001e\u0002r\u0001h\u0001z\u0001e\u0001r\u0001m\u0001e\u0001i\u0001p\u0001b\u0002o\u0001u\u0001s\u0001e\u0001h\u0001l\u0001f\u0001u\u0001m\u0001f\u0001e\u0001l\u0001k\u0001(\u0001e\u0001\uffff\u0001m\u0001c\u0001i\u0001r\u0001c\u0001z\u0001e\u0001t\u0001a\u0001i\u0001z\u0001c\u0001o\u0001z\u0001i\u0001m\u0002i\u0001t\u0001d\u0001p\u0001\uffff\u0001n\u0001e\u0001h\u0001s\u0001n\u0001e\u0001\uffff\u0001r\u0001e\u0001d\u0001l\u0001\uffff\u0001t\u0001d\u0001\uffff\u0001z\u0001g\u0001n\u0001t\u0001l\u0002z\u0001o\u0001t\u0003z\u0001s\u0001a\u0001s\u0001o\u0002z\u0001d\u0002z\u0001\uffff\u0001z\u0001s\u0001z\u0001e\u0002\uffff\u0001o\u0001i\u0001\uffff\u0001z\u0002\uffff\u0001i\u0001l\u0001z\u0001s\u0002\uffff\u0001z\u0003\uffff\u0001z\u0001\uffff\u0001s\u0001l\u0001c\u0001\uffff\u0001o\u0001z\u0001\uffff\u0001z\u0002\uffff\u0002z\u0001a\u0001n\u0004\uffff\u0001t\u0001z\u0001i\u0001\uffff\u0001o\u0001n\u0001z\u0001\uffff";
   static final String DFA1_acceptS = "\u0001\uffff\u0001\u0001\u0001\u0002\u0001\u0003\u0001\u0004\u0001\uffff\u0001\u0007\u0003\uffff\u0001\f\u0001\r\u0001\uffff\u0001\u0010\u0001\uffff\u0001\u0013\u0001\u0014\u0001\u0015\u0001\u0016\u0001\uffff\u0001\u0018\u0001\u0019\r\uffff\u00014\u0001\uffff\u0001\u0006\u0001\u0005\u0001\t\u0001\b\u0001\n\u0001\u000b\u0001\u000f\u0001\u000e\u0001\u0012\u0001\u0011-\uffff\u00011\u0015\uffff\u0001\u0017\u0006\uffff\u0001\u001e\u0004\uffff\u0001(\u0002\uffff\u0001#\u0015\uffff\u0001$\u0004\uffff\u0001-\u0001/\u0002\uffff\u0001\u001c\u0001\uffff\u0001&\u0001%\u0004\uffff\u0001 \u0001,\u0001\uffff\u0001!\u0001\"\u0001'\u0001\uffff\u0001+\u0003\uffff\u0001\u001b\u0002\uffff\u00013\u0001\uffff\u0001.\u0001)\u0004\uffff\u0001\u001d\u0001\u001f\u0001*\u00010\u0003\uffff\u00012\u0003\uffff\u0001\u001a";
   static final String DFA1_specialS = "Ê\uffff}>";
   static final String[] DFA1_transitionS = new String[]{"\u0002#\u0002\uffff\u0001#\u0012\uffff\u0001#\u0001\u0001\u0001#\u0001\uffff\u0001#\u0001\u0002\u0001\uffff\u0001#\u0001\u0003\u0001\u0004\u0001\u0011\u0001\u0005\u0001\u0006\u0001\u0007\u0001\b\u0001\t\n#\u0001\n\u0001\u000b\u0001\f\u0001\r\u0001\u000e\u0001#\u0001\u000f\u0001\u0016\u0001#\u0001\u001e\u0002#\u0001!\u0002#\u0001\u0018\u0001#\u0001\u001a\u0001\u0019\u0001\u001d\u0001#\u0001\u001c\u0001\u001f\u0001#\u0001\u001b\u0001\u0017\u0001#\u0001 \u0001#\u0001\"\u0003#\u0001\u0010\u0001\uffff\u0001\u0012\u0003\uffff\u0001\u0016\u0001#\u0001\u001e\u0001#\u0001\u0013\u0001!\u0002#\u0001\u0018\u0001#\u0001\u001a\u0001\u0019\u0001\u001d\u0001#\u0001\u001c\u0001$\u0001#\u0001\u001b\u0001\u0017\u0001#\u0001 \u0001#\u0001\"\u0003#\u0001\u0014\u0001\uffff\u0001\u0015", "", "", "", "", "\u0001%", "", "\u0001#\u0002\uffff\n#\u0003\uffff\u0001'\u000b\uffff\u0001#\u0004\uffff\u0001#\u0001\uffff\u0001#\u0018\uffff\u0001#\u0004\uffff\u0001#", "\u0001#", "\u0001#\u0004\uffff\u0001#", "", "", "\u0001+", "", "\u0001-", "", "", "", "", "\u0001/", "", "", "\u00011\u0006\uffff\u00010\u0018\uffff\u00011\u0006\uffff\u00010", "\u00012\u0001\uffff\u00013\u000f\uffff\u00014\r\uffff\u00012\u0001\uffff\u00013\u000f\uffff\u00014", "\u00015\u001f\uffff\u00015", "\u00016\u001f\uffff\u00016", "\u00017\u001f\uffff\u00017", "\u00018\t\uffff\u00019\u0015\uffff\u00018\t\uffff\u00019", "\u0001:\u001f\uffff\u0001:", "\u0001;\u001f\uffff\u0001;", "\u0001<\r\uffff\u0001=\u0011\uffff\u0001<\r\uffff\u0001=", "\u0001>\u001f\uffff\u0001>", "\u0001?\u001f\uffff\u0001?", "\u0001@\u001f\uffff\u0001@", "\u0001A\u001f\uffff\u0001A", "", "\u0001>\u001f\uffff\u0001>", "", "", "", "", "", "", "", "", "", "", "\u0001B", "\u0001C\u001f\uffff\u0001C", "\u0001D\u001f\uffff\u0001D", "\u0001E\u001f\uffff\u0001E", "\u0001F\u001f\uffff\u0001F", "\u0001G\u001f\uffff\u0001G", "\u0001I\u000f\uffff\u0001H\u000f\uffff\u0001I\u000f\uffff\u0001H", "\u0001J\u001f\uffff\u0001J", "\u0001K\u001f\uffff\u0001K", "\u0001N\t\uffff\u0001M\u0001L\u0014\uffff\u0001N\t\uffff\u0001M\u0001L", "\u0001O\u001f\uffff\u0001O", "\u0001P\u001f\uffff\u0001P", "\u0001Q\u001f\uffff\u0001Q", "\u0001R\u001f\uffff\u0001R", "\u0001T\u0001U\u0001S\u001d\uffff\u0001T\u0001U\u0001S", "\u0001V\u001f\uffff\u0001V", "\u0001W\u001f\uffff\u0001W", "\u0001X\u001f\uffff\u0001X", "\u0001Y\u001f\uffff\u0001Y", "\u0001Z", "\u0001[\u001f\uffff\u0001[", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001]\u001f\uffff\u0001]", "\u0001^\u001f\uffff\u0001^", "\u0001_\u001f\uffff\u0001_", "\u0001`\u001f\uffff\u0001`", "\u0001a\u001f\uffff\u0001a", "\u0001b\u001f\uffff\u0001b", "\u0001c\u001f\uffff\u0001c", "\u0001d\u001f\uffff\u0001d", "\u0001e\u001f\uffff\u0001e", "\u0001f\u001f\uffff\u0001f", "\u0001g\u001f\uffff\u0001g", "\u0001h\u001f\uffff\u0001h", "\u0001i\u001f\uffff\u0001i", "\u0001j\u001f\uffff\u0001j", "\u0001k\u001f\uffff\u0001k", "\u0001l\u001f\uffff\u0001l", "\u0001m\u001f\uffff\u0001m", "\u0001n\u001f\uffff\u0001n", "\u0001o\u001f\uffff\u0001o", "\u0001p\u001f\uffff\u0001p", "\u0001q\u001f\uffff\u0001q", "\u0001r", "\u0001s\u001f\uffff\u0001s", "", "\u0001t\u001f\uffff\u0001t", "\u0001u\u001f\uffff\u0001u", "\u0001v\u001f\uffff\u0001v", "\u0001w\u001f\uffff\u0001w", "\u0001x\u001f\uffff\u0001x", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001z\u001f\uffff\u0001z", "\u0001{\u001f\uffff\u0001{", "\u0001|\u001f\uffff\u0001|", "\u0001}\u001f\uffff\u0001}", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001\u007f\u001f\uffff\u0001\u007f", "\u0001\u0080\u001f\uffff\u0001\u0080", "\n#\u0007\uffff\u0012#\u0001\u0082\u0007#\u0004\uffff\u0001#\u0001\uffff\u0012#\u0001\u0082\u0007#", "\u0001\u0083\u001f\uffff\u0001\u0083", "\u0001\u0084\u001f\uffff\u0001\u0084", "\u0001\u0085\u001f\uffff\u0001\u0085", "\u0001\u0086\u001f\uffff\u0001\u0086", "\u0001\u0087\u001f\uffff\u0001\u0087", "\u0001\u0088\u001f\uffff\u0001\u0088", "\u0001\u0089\u001f\uffff\u0001\u0089", "", "\u0001\u008a\u001f\uffff\u0001\u008a", "\u0001\u008c\u0003\uffff\u0001\u008b\u001b\uffff\u0001\u008c\u0003\uffff\u0001\u008b", "\u0001\u008d\u001f\uffff\u0001\u008d", "\u0001\u008e\u001f\uffff\u0001\u008e", "\u0001\u008f\u001f\uffff\u0001\u008f", "\u0001\u0090\u001f\uffff\u0001\u0090", "", "\u0001\u0091\u001f\uffff\u0001\u0091", "\u0001\u0092\u001f\uffff\u0001\u0092", "\u0001\u0093\u001f\uffff\u0001\u0093", "\u0001\u0094\u001f\uffff\u0001\u0094", "", "\u0001\u0095\u001f\uffff\u0001\u0095", "\u0001\u0096\u001f\uffff\u0001\u0096", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001\u0098\u001f\uffff\u0001\u0098", "\u0001\u0099\u001f\uffff\u0001\u0099", "\u0001\u009a\u001f\uffff\u0001\u009a", "\u0001\u009b\u001f\uffff\u0001\u009b", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001\u009e\u001f\uffff\u0001\u009e", "\u0001\u009f\u001f\uffff\u0001\u009f", "\n#\u0007\uffff\u0012#\u0001¡\u0007#\u0004\uffff\u0001#\u0001\uffff\u0012#\u0001¡\u0007#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001¤\u001f\uffff\u0001¤", "\u0001¥\u001f\uffff\u0001¥", "\u0001¦\u001f\uffff\u0001¦", "\u0001§\u001f\uffff\u0001§", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001ª\u001f\uffff\u0001ª", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001®\u001f\uffff\u0001®", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001°\u001f\uffff\u0001°", "", "", "\u0001±\u001f\uffff\u0001±", "\u0001²\u001f\uffff\u0001²", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "", "\u0001´\u001f\uffff\u0001´", "\u0001µ\u001f\uffff\u0001µ", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001·\u001f\uffff\u0001·", "", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "\u0001º\u001f\uffff\u0001º", "\u0001»\u001f\uffff\u0001»", "\u0001¼\u001f\uffff\u0001¼", "", "\u0001½\u001f\uffff\u0001½", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "", "", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001Â\u001f\uffff\u0001Â", "\u0001Ã\u001f\uffff\u0001Ã", "", "", "", "", "\u0001Ä\u001f\uffff\u0001Ä", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", "\u0001Æ\u001f\uffff\u0001Æ", "", "\u0001Ç\u001f\uffff\u0001Ç", "\u0001È\u001f\uffff\u0001È", "\n#\u0007\uffff\u001a#\u0004\uffff\u0001#\u0001\uffff\u001a#", ""};
   static final short[] DFA1_eot = DFA.unpackEncodedString("\u0005\uffff\u0001&\u0001\uffff\u0001(\u0001)\u0001*\u0002\uffff\u0001,\u0001\uffff\u0001.\u0004\uffff\u0001#\u0002\uffff\r#\u0001\uffff\u0001#\n\uffff\u0015#\u0001\\\u0017#\u0001\uffff\u0005#\u0001y\u0004#\u0001~\u0002#\u0001\u0081\u0007#\u0001\uffff\u0006#\u0001\uffff\u0004#\u0001\uffff\u0002#\u0001\uffff\u0001\u0097\u0004#\u0001\u009c\u0001\u009d\u0002#\u0001 \u0001¢\u0001£\u0004#\u0001¨\u0001©\u0001#\u0001«\u0001¬\u0001\uffff\u0001\u00ad\u0001#\u0001¯\u0001#\u0002\uffff\u0002#\u0001\uffff\u0001³\u0002\uffff\u0002#\u0001¶\u0001#\u0002\uffff\u0001¸\u0003\uffff\u0001¹\u0001\uffff\u0003#\u0001\uffff\u0001#\u0001¾\u0001\uffff\u0001¿\u0002\uffff\u0001À\u0001Á\u0002#\u0004\uffff\u0001#\u0001Å\u0001#\u0001\uffff\u0002#\u0001É\u0001\uffff");
   static final short[] DFA1_eof = DFA.unpackEncodedString("Ê\uffff");
   static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars("\u0001\t\u0004\uffff\u0001=\u0001\uffff\u0001-\u0001.\u0001*\u0002\uffff\u0001=\u0001\uffff\u0001=\u0004\uffff\u0001x\u0002\uffff\u0001N\u0001C\u0001N\u0001D\u0002E\u0001B\u0001E\u0001A\u0001R\u0001N\u0001I\u0001O\u0001\uffff\u0001R\n\uffff\u0001p\u0001T\u0001Y\u0001H\u0001A\u0001B\u0001D\u0001A\u0001R\u0001B\u0001W\u0001J\u0001T\u0002L\u0001O\u0001S\u0001E\u0001R\u0001r\u0001H\u00010\u0001E\u0001R\u0001M\u0001E\u0001I\u0001P\u0001B\u0002O\u0001U\u0001S\u0001E\u0001H\u0001L\u0001F\u0001U\u0001M\u0001F\u0001E\u0001L\u0001K\u0001(\u0001E\u0001\uffff\u0001M\u0001C\u0001I\u0001R\u0001C\u00010\u0001E\u0001T\u0001A\u0001I\u00010\u0001C\u0001O\u00010\u0001I\u0001M\u0002I\u0001T\u0001D\u0001P\u0001\uffff\u0001N\u0001A\u0001H\u0001S\u0001N\u0001E\u0001\uffff\u0001R\u0001E\u0001D\u0001L\u0001\uffff\u0001T\u0001D\u0001\uffff\u00010\u0001G\u0001N\u0001T\u0001L\u00020\u0001O\u0001T\u00030\u0001S\u0001A\u0001S\u0001O\u00020\u0001D\u00020\u0001\uffff\u00010\u0001S\u00010\u0001E\u0002\uffff\u0001O\u0001I\u0001\uffff\u00010\u0002\uffff\u0001I\u0001L\u00010\u0001S\u0002\uffff\u00010\u0003\uffff\u00010\u0001\uffff\u0001S\u0001L\u0001C\u0001\uffff\u0001O\u00010\u0001\uffff\u00010\u0002\uffff\u00020\u0001A\u0001N\u0004\uffff\u0001T\u00010\u0001I\u0001\uffff\u0001O\u0001N\u00010\u0001\uffff");
   static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars("\u0001}\u0004\uffff\u0001=\u0001\uffff\u0001n\u0001.\u0001/\u0002\uffff\u0001=\u0001\uffff\u0001=\u0004\uffff\u0001x\u0002\uffff\u0002u\u0001n\u0001d\u0001e\u0001o\u0001b\u0001e\u0001o\u0001r\u0001n\u0001i\u0001o\u0001\uffff\u0001r\n\uffff\u0001p\u0001t\u0001y\u0001h\u0001a\u0001b\u0001t\u0001a\u0001r\u0001m\u0001w\u0001j\u0001t\u0001l\u0001n\u0001o\u0001s\u0001e\u0002r\u0001h\u0001z\u0001e\u0001r\u0001m\u0001e\u0001i\u0001p\u0001b\u0002o\u0001u\u0001s\u0001e\u0001h\u0001l\u0001f\u0001u\u0001m\u0001f\u0001e\u0001l\u0001k\u0001(\u0001e\u0001\uffff\u0001m\u0001c\u0001i\u0001r\u0001c\u0001z\u0001e\u0001t\u0001a\u0001i\u0001z\u0001c\u0001o\u0001z\u0001i\u0001m\u0002i\u0001t\u0001d\u0001p\u0001\uffff\u0001n\u0001e\u0001h\u0001s\u0001n\u0001e\u0001\uffff\u0001r\u0001e\u0001d\u0001l\u0001\uffff\u0001t\u0001d\u0001\uffff\u0001z\u0001g\u0001n\u0001t\u0001l\u0002z\u0001o\u0001t\u0003z\u0001s\u0001a\u0001s\u0001o\u0002z\u0001d\u0002z\u0001\uffff\u0001z\u0001s\u0001z\u0001e\u0002\uffff\u0001o\u0001i\u0001\uffff\u0001z\u0002\uffff\u0001i\u0001l\u0001z\u0001s\u0002\uffff\u0001z\u0003\uffff\u0001z\u0001\uffff\u0001s\u0001l\u0001c\u0001\uffff\u0001o\u0001z\u0001\uffff\u0001z\u0002\uffff\u0002z\u0001a\u0001n\u0004\uffff\u0001t\u0001z\u0001i\u0001\uffff\u0001o\u0001n\u0001z\u0001\uffff");
   static final short[] DFA1_accept = DFA.unpackEncodedString("\u0001\uffff\u0001\u0001\u0001\u0002\u0001\u0003\u0001\u0004\u0001\uffff\u0001\u0007\u0003\uffff\u0001\f\u0001\r\u0001\uffff\u0001\u0010\u0001\uffff\u0001\u0013\u0001\u0014\u0001\u0015\u0001\u0016\u0001\uffff\u0001\u0018\u0001\u0019\r\uffff\u00014\u0001\uffff\u0001\u0006\u0001\u0005\u0001\t\u0001\b\u0001\n\u0001\u000b\u0001\u000f\u0001\u000e\u0001\u0012\u0001\u0011-\uffff\u00011\u0015\uffff\u0001\u0017\u0006\uffff\u0001\u001e\u0004\uffff\u0001(\u0002\uffff\u0001#\u0015\uffff\u0001$\u0004\uffff\u0001-\u0001/\u0002\uffff\u0001\u001c\u0001\uffff\u0001&\u0001%\u0004\uffff\u0001 \u0001,\u0001\uffff\u0001!\u0001\"\u0001'\u0001\uffff\u0001+\u0003\uffff\u0001\u001b\u0002\uffff\u00013\u0001\uffff\u0001.\u0001)\u0004\uffff\u0001\u001d\u0001\u001f\u0001*\u00010\u0003\uffff\u00012\u0003\uffff\u0001\u001a");
   static final short[] DFA1_special = DFA.unpackEncodedString("Ê\uffff}>");
   static final short[][] DFA1_transition;

   public CqlLexer() {
      this.tokens = new ArrayList();
      this.listeners = new ArrayList();
      this.dfa1 = new CqlLexer.DFA1(this);
   }

   public CqlLexer(CharStream input) {
      this(input, new RecognizerSharedState());
   }

   public CqlLexer(CharStream input, RecognizerSharedState state) {
      super(input, state);
      this.tokens = new ArrayList();
      this.listeners = new ArrayList();
      this.dfa1 = new CqlLexer.DFA1(this);
      this.gLexer = new Cql_Lexer(input, state, this);
   }

   public void addErrorListener(ErrorListener listener) {
      this.listeners.add(listener);
   }

   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
      int i = 0;

      for(int m = this.listeners.size(); i < m; ++i) {
         ((ErrorListener)this.listeners.get(i)).syntaxError(this, tokenNames, e);
      }

   }

   public void emit(Token token) {
      this.state.token = token;
      this.tokens.add(token);
   }

   public Lexer[] getDelegates() {
      return new Lexer[]{this.gLexer};
   }

   public String getGrammarFileName() {
      return "org/apache/cassandra/cql3/Cql.g";
   }

   public final void mK_ANY() throws RecognitionException {
      int _type = 33;
      int _channel = 0;
      this.gLexer.mA();
      this.gLexer.mN();
      this.gLexer.mY();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_AUTHENTICATION() throws RecognitionException {
      int _type = 38;
      int _channel = 0;
      this.gLexer.mA();
      this.gLexer.mU();
      this.gLexer.mT();
      this.gLexer.mH();
      this.gLexer.mE();
      this.gLexer.mN();
      this.gLexer.mT();
      this.gLexer.mI();
      this.gLexer.mC();
      this.gLexer.mA();
      this.gLexer.mT();
      this.gLexer.mI();
      this.gLexer.mO();
      this.gLexer.mN();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_CALL() throws RecognitionException {
      int _type = 46;
      int _channel = 0;
      this.gLexer.mC();
      this.gLexer.mA();
      this.gLexer.mL();
      this.gLexer.mL();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_CALLS() throws RecognitionException {
      int _type = 48;
      int _channel = 0;
      this.gLexer.mC();
      this.gLexer.mA();
      this.gLexer.mL();
      this.gLexer.mL();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_COLUMNS() throws RecognitionException {
      int _type = 52;
      int _channel = 0;
      this.gLexer.mC();
      this.gLexer.mO();
      this.gLexer.mL();
      this.gLexer.mU();
      this.gLexer.mM();
      this.gLexer.mN();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_COMMIT() throws RecognitionException {
      int _type = 53;
      int _channel = 0;
      this.gLexer.mC();
      this.gLexer.mO();
      this.gLexer.mM();
      this.gLexer.mM();
      this.gLexer.mI();
      this.gLexer.mT();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_CONFIG() throws RecognitionException {
      int _type = 55;
      int _channel = 0;
      this.gLexer.mC();
      this.gLexer.mO();
      this.gLexer.mN();
      this.gLexer.mF();
      this.gLexer.mI();
      this.gLexer.mG();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_FIELD() throws RecognitionException {
      int _type = 75;
      int _channel = 0;
      this.gLexer.mF();
      this.gLexer.mI();
      this.gLexer.mE();
      this.gLexer.mL();
      this.gLexer.mD();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_INDICES() throws RecognitionException {
      int _type = 90;
      int _channel = 0;
      this.gLexer.mI();
      this.gLexer.mN();
      this.gLexer.mD();
      this.gLexer.mI();
      this.gLexer.mC();
      this.gLexer.mE();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_INTERNAL() throws RecognitionException {
      int _type = 96;
      int _channel = 0;
      this.gLexer.mI();
      this.gLexer.mN();
      this.gLexer.mT();
      this.gLexer.mE();
      this.gLexer.mR();
      this.gLexer.mN();
      this.gLexer.mA();
      this.gLexer.mL();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_KERBEROS() throws RecognitionException {
      int _type = 100;
      int _channel = 0;
      this.gLexer.mK();
      this.gLexer.mE();
      this.gLexer.mR();
      this.gLexer.mB();
      this.gLexer.mE();
      this.gLexer.mR();
      this.gLexer.mO();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_LDAP() throws RecognitionException {
      int _type = 106;
      int _channel = 0;
      this.gLexer.mL();
      this.gLexer.mD();
      this.gLexer.mA();
      this.gLexer.mP();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_METHOD() throws RecognitionException {
      int _type = 115;
      int _channel = 0;
      this.gLexer.mM();
      this.gLexer.mE();
      this.gLexer.mT();
      this.gLexer.mH();
      this.gLexer.mO();
      this.gLexer.mD();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_OBJECT() throws RecognitionException {
      int _type = 125;
      int _channel = 0;
      this.gLexer.mO();
      this.gLexer.mB();
      this.gLexer.mJ();
      this.gLexer.mE();
      this.gLexer.mC();
      this.gLexer.mT();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_PROFILES() throws RecognitionException {
      int _type = 139;
      int _channel = 0;
      this.gLexer.mP();
      this.gLexer.mR();
      this.gLexer.mO();
      this.gLexer.mF();
      this.gLexer.mI();
      this.gLexer.mL();
      this.gLexer.mE();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_REBUILD() throws RecognitionException {
      int _type = 140;
      int _channel = 0;
      this.gLexer.mR();
      this.gLexer.mE();
      this.gLexer.mB();
      this.gLexer.mU();
      this.gLexer.mI();
      this.gLexer.mL();
      this.gLexer.mD();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_RELOAD() throws RecognitionException {
      int _type = 141;
      int _channel = 0;
      this.gLexer.mR();
      this.gLexer.mE();
      this.gLexer.mL();
      this.gLexer.mO();
      this.gLexer.mA();
      this.gLexer.mD();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_REMOTE() throws RecognitionException {
      int _type = 142;
      int _channel = 0;
      this.gLexer.mR();
      this.gLexer.mE();
      this.gLexer.mM();
      this.gLexer.mO();
      this.gLexer.mT();
      this.gLexer.mE();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_ROWS() throws RecognitionException {
      int _type = 151;
      int _channel = 0;
      this.gLexer.mR();
      this.gLexer.mO();
      this.gLexer.mW();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_SCHEMA() throws RecognitionException {
      int _type = 152;
      int _channel = 0;
      this.gLexer.mS();
      this.gLexer.mC();
      this.gLexer.mH();
      this.gLexer.mE();
      this.gLexer.mM();
      this.gLexer.mA();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_SCHEME() throws RecognitionException {
      int _type = 153;
      int _channel = 0;
      this.gLexer.mS();
      this.gLexer.mC();
      this.gLexer.mH();
      this.gLexer.mE();
      this.gLexer.mM();
      this.gLexer.mE();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_SCHEMES() throws RecognitionException {
      int _type = 154;
      int _channel = 0;
      this.gLexer.mS();
      this.gLexer.mC();
      this.gLexer.mH();
      this.gLexer.mE();
      this.gLexer.mM();
      this.gLexer.mE();
      this.gLexer.mS();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_SEARCH() throws RecognitionException {
      int _type = 155;
      int _channel = 0;
      this.gLexer.mS();
      this.gLexer.mE();
      this.gLexer.mA();
      this.gLexer.mR();
      this.gLexer.mC();
      this.gLexer.mH();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_SUBMISSION() throws RecognitionException {
      int _type = 163;
      int _channel = 0;
      this.gLexer.mS();
      this.gLexer.mU();
      this.gLexer.mB();
      this.gLexer.mM();
      this.gLexer.mI();
      this.gLexer.mS();
      this.gLexer.mS();
      this.gLexer.mI();
      this.gLexer.mO();
      this.gLexer.mN();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_UNSET() throws RecognitionException {
      int _type = 179;
      int _channel = 0;
      this.gLexer.mU();
      this.gLexer.mN();
      this.gLexer.mS();
      this.gLexer.mE();
      this.gLexer.mT();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mK_WORKPOOL() throws RecognitionException {
      int _type = 192;
      int _channel = 0;
      this.gLexer.mW();
      this.gLexer.mO();
      this.gLexer.mR();
      this.gLexer.mK();
      this.gLexer.mP();
      this.gLexer.mO();
      this.gLexer.mO();
      this.gLexer.mL();
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__217() throws RecognitionException {
      int _type = 217;
      int _channel = 0;
      this.match("!=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__218() throws RecognitionException {
      int _type = 218;
      int _channel = 0;
      this.match(37);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__219() throws RecognitionException {
      int _type = 219;
      int _channel = 0;
      this.match(40);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__220() throws RecognitionException {
      int _type = 220;
      int _channel = 0;
      this.match(41);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__221() throws RecognitionException {
      int _type = 221;
      int _channel = 0;
      this.match(43);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__222() throws RecognitionException {
      int _type = 222;
      int _channel = 0;
      this.match("+=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__223() throws RecognitionException {
      int _type = 223;
      int _channel = 0;
      this.match(44);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__224() throws RecognitionException {
      int _type = 224;
      int _channel = 0;
      this.match(45);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__225() throws RecognitionException {
      int _type = 225;
      int _channel = 0;
      this.match("-=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__226() throws RecognitionException {
      int _type = 226;
      int _channel = 0;
      this.match(46);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__227() throws RecognitionException {
      int _type = 227;
      int _channel = 0;
      this.match(47);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__228() throws RecognitionException {
      int _type = 228;
      int _channel = 0;
      this.match(58);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__229() throws RecognitionException {
      int _type = 229;
      int _channel = 0;
      this.match(59);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__230() throws RecognitionException {
      int _type = 230;
      int _channel = 0;
      this.match(60);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__231() throws RecognitionException {
      int _type = 231;
      int _channel = 0;
      this.match("<=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__232() throws RecognitionException {
      int _type = 232;
      int _channel = 0;
      this.match(61);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__233() throws RecognitionException {
      int _type = 233;
      int _channel = 0;
      this.match(62);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__234() throws RecognitionException {
      int _type = 234;
      int _channel = 0;
      this.match(">=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__235() throws RecognitionException {
      int _type = 235;
      int _channel = 0;
      this.match(64);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__236() throws RecognitionException {
      int _type = 236;
      int _channel = 0;
      this.match(91);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__237() throws RecognitionException {
      int _type = 237;
      int _channel = 0;
      this.match(42);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__238() throws RecognitionException {
      int _type = 238;
      int _channel = 0;
      this.match(93);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__239() throws RecognitionException {
      int _type = 239;
      int _channel = 0;
      this.match("expr(");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__240() throws RecognitionException {
      int _type = 240;
      int _channel = 0;
      this.match(123);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__241() throws RecognitionException {
      int _type = 241;
      int _channel = 0;
      this.match(125);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public void mTokens() throws RecognitionException {
      int alt1 = true;
      int alt1 = this.dfa1.predict(this.input);
      switch(alt1) {
      case 1:
         this.mT__217();
         break;
      case 2:
         this.mT__218();
         break;
      case 3:
         this.mT__219();
         break;
      case 4:
         this.mT__220();
         break;
      case 5:
         this.mT__221();
         break;
      case 6:
         this.mT__222();
         break;
      case 7:
         this.mT__223();
         break;
      case 8:
         this.mT__224();
         break;
      case 9:
         this.mT__225();
         break;
      case 10:
         this.mT__226();
         break;
      case 11:
         this.mT__227();
         break;
      case 12:
         this.mT__228();
         break;
      case 13:
         this.mT__229();
         break;
      case 14:
         this.mT__230();
         break;
      case 15:
         this.mT__231();
         break;
      case 16:
         this.mT__232();
         break;
      case 17:
         this.mT__233();
         break;
      case 18:
         this.mT__234();
         break;
      case 19:
         this.mT__235();
         break;
      case 20:
         this.mT__236();
         break;
      case 21:
         this.mT__237();
         break;
      case 22:
         this.mT__238();
         break;
      case 23:
         this.mT__239();
         break;
      case 24:
         this.mT__240();
         break;
      case 25:
         this.mT__241();
         break;
      case 26:
         this.mK_AUTHENTICATION();
         break;
      case 27:
         this.mK_SCHEMES();
         break;
      case 28:
         this.mK_SCHEME();
         break;
      case 29:
         this.mK_INTERNAL();
         break;
      case 30:
         this.mK_LDAP();
         break;
      case 31:
         this.mK_KERBEROS();
         break;
      case 32:
         this.mK_REMOTE();
         break;
      case 33:
         this.mK_OBJECT();
         break;
      case 34:
         this.mK_METHOD();
         break;
      case 35:
         this.mK_CALL();
         break;
      case 36:
         this.mK_CALLS();
         break;
      case 37:
         this.mK_SEARCH();
         break;
      case 38:
         this.mK_SCHEMA();
         break;
      case 39:
         this.mK_CONFIG();
         break;
      case 40:
         this.mK_ROWS();
         break;
      case 41:
         this.mK_COLUMNS();
         break;
      case 42:
         this.mK_PROFILES();
         break;
      case 43:
         this.mK_COMMIT();
         break;
      case 44:
         this.mK_RELOAD();
         break;
      case 45:
         this.mK_UNSET();
         break;
      case 46:
         this.mK_REBUILD();
         break;
      case 47:
         this.mK_FIELD();
         break;
      case 48:
         this.mK_WORKPOOL();
         break;
      case 49:
         this.mK_ANY();
         break;
      case 50:
         this.mK_SUBMISSION();
         break;
      case 51:
         this.mK_INDICES();
         break;
      case 52:
         this.gLexer.mTokens();
      }

   }

   public Token nextToken() {
      super.nextToken();
      return (Token)(this.tokens.size() == 0?new CommonToken(-1):(Token)this.tokens.remove(0));
   }

   public void removeErrorListener(ErrorListener listener) {
      this.listeners.remove(listener);
   }

   static {
      int numStates = DFA1_transitionS.length;
      DFA1_transition = new short[numStates][];

      for(int i = 0; i < numStates; ++i) {
         DFA1_transition[i] = DFA.unpackEncodedString(DFA1_transitionS[i]);
      }

   }

   protected class DFA1 extends DFA {
      public DFA1(BaseRecognizer recognizer) {
         this.recognizer = recognizer;
         this.decisionNumber = 1;
         this.eot = CqlLexer.DFA1_eot;
         this.eof = CqlLexer.DFA1_eof;
         this.min = CqlLexer.DFA1_min;
         this.max = CqlLexer.DFA1_max;
         this.accept = CqlLexer.DFA1_accept;
         this.special = CqlLexer.DFA1_special;
         this.transition = CqlLexer.DFA1_transition;
      }

      public String getDescription() {
         return "1:1: Tokens : ( T__217 | T__218 | T__219 | T__220 | T__221 | T__222 | T__223 | T__224 | T__225 | T__226 | T__227 | T__228 | T__229 | T__230 | T__231 | T__232 | T__233 | T__234 | T__235 | T__236 | T__237 | T__238 | T__239 | T__240 | T__241 | K_AUTHENTICATION | K_SCHEMES | K_SCHEME | K_INTERNAL | K_LDAP | K_KERBEROS | K_REMOTE | K_OBJECT | K_METHOD | K_CALL | K_CALLS | K_SEARCH | K_SCHEMA | K_CONFIG | K_ROWS | K_COLUMNS | K_PROFILES | K_COMMIT | K_RELOAD | K_UNSET | K_REBUILD | K_FIELD | K_WORKPOOL | K_ANY | K_SUBMISSION | K_INDICES | Lexer. Tokens );";
      }
   }
}
