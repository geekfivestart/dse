package org.apache.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.DseResourceFactory;
import com.datastax.bdp.xml.XmlPath;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.DFA;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class Cql_DseSearchParser extends Parser {
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
   @Inject(
      optional = true
   )
   private static Cql_DseSearchParser.SearchStatementFactory searchStatementFactory = new Cql_DseSearchParser.SearchStatementFactory() {
   };
   @Inject(
      optional = true
   )
   @Named("SearchResourceRootName")
   private static String searchResourceRootName = "";
   private DseResourceFactory dseResourceFactory;
   protected Cql_DseSearchParser.DFA8 dfa8;
   static final String DFA8_eotS = "\n\uffff";
   static final String DFA8_eofS = "\u0004\uffff\u0001\u0005\u0001\uffff\u0001\u0007\u0003\uffff";
   static final String DFA8_minS = "\u0001\u001b\u0001\u0017\u0001\uffff\u0002\u0017\u0001\uffff\u0001\u0017\u0003\uffff";
   static final String DFA8_maxS = "\u0001\u009d\u0001Ë\u0001\uffff\u0001Ë\u0001ì\u0001\uffff\u0001ì\u0003\uffff";
   static final String DFA8_acceptS = "\u0002\uffff\u0001\u0003\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0004\u0001\u0002\u0001\u0005";
   static final String DFA8_specialS = "\n\uffff}>";
   static final String[] DFA8_transitionS = new String[]{"\u0001\u0001*\uffff\u0001\u0003V\uffff\u0001\u0002", "\u0001\u0005\u0004\uffff\u0002\u0005\u0003\uffff\u0001\u0005\u0001\uffff\u0001\u0005\u0001\uffff\u0002\u0005\u0003\uffff\u0003\u0005\u0001\uffff\u0005\u0005\u0001\uffff\u0007\u0005\u0001\uffff\u0004\u0005\u0003\uffff\u0003\u0005\u0001\uffff\u0001\u0005\u0002\uffff\u0001\u0005\u0001\u0004\u0003\u0005\u0002\uffff\u0001\u0005\u0001\uffff\u0002\u0005\u0001\uffff\u0001\u0005\u0003\uffff\u0004\u0005\u0001\uffff\u0002\u0005\u0002\uffff\u0004\u0005\u0001\uffff\u0004\u0005\u0001\uffff\u0003\u0005\u0003\uffff\u0001\u0005\u0001\uffff\u0001\u0005\u0002\uffff\u0001\u0005\u0001\uffff\u0001\u0005\u0002\uffff\u0001\u0005\u0002\uffff\u0001\u0005\u0002\uffff\u0005\u0005\u0003\uffff\u0004\u0005\u0002\uffff\u0003\u0005\u0001\uffff\u0007\u0005\u0002\uffff\f\u0005\u0002\uffff\u0001\u0005\u0001\uffff\u0003\u0005\u0001\uffff\u0002\u0005\u0002\uffff\u0002\u0005\u0001\uffff\u0004\u0005\u0003\uffff\u0002\u0005\t\uffff\u0001\u0005", "", "\u0001\u0007\u0004\uffff\u0002\u0007\u0003\uffff\u0001\u0007\u0001\uffff\u0001\u0007\u0001\uffff\u0002\u0007\u0003\uffff\u0003\u0007\u0001\uffff\u0005\u0007\u0001\uffff\u0007\u0007\u0001\uffff\u0004\u0007\u0003\uffff\u0003\u0007\u0001\uffff\u0001\u0007\u0002\uffff\u0001\u0007\u0001\u0006\u0003\u0007\u0002\uffff\u0001\u0007\u0001\uffff\u0002\u0007\u0001\uffff\u0001\u0007\u0003\uffff\u0004\u0007\u0001\uffff\u0002\u0007\u0002\uffff\u0004\u0007\u0001\uffff\u0004\u0007\u0001\uffff\u0003\u0007\u0003\uffff\u0001\u0007\u0001\uffff\u0001\u0007\u0002\uffff\u0001\u0007\u0001\uffff\u0001\u0007\u0002\uffff\u0001\u0007\u0002\uffff\u0001\u0007\u0002\uffff\u0005\u0007\u0003\uffff\u0004\u0007\u0002\uffff\u0003\u0007\u0001\uffff\u0007\u0007\u0002\uffff\f\u0007\u0002\uffff\u0001\u0007\u0001\uffff\u0003\u0007\u0001\uffff\u0002\u0007\u0002\uffff\u0002\u0007\u0001\uffff\u0004\u0007\u0003\uffff\u0002\u0007\t\uffff\u0001\u0007", "\u0001\b\u0004\uffff\u0002\b\u0003\uffff\u0001\b\u0001\uffff\u0001\b\u0001\uffff\u0002\b\u0003\uffff\u0003\b\u0001\uffff\u0005\b\u0001\uffff\u0007\b\u0001\uffff\u0004\b\u0003\uffff\u0003\b\u0001\uffff\u0001\b\u0002\uffff\u0005\b\u0002\uffff\u0001\b\u0001\uffff\u0002\b\u0001\uffff\u0001\b\u0003\uffff\u0004\b\u0001\uffff\u0002\b\u0002\uffff\u0004\b\u0001\uffff\u0004\b\u0001\uffff\u0003\b\u0003\uffff\u0001\b\u0001\uffff\u0001\b\u0002\uffff\u0001\b\u0001\uffff\u0001\b\u0002\uffff\u0001\b\u0002\uffff\u0001\b\u0002\uffff\u0005\b\u0003\uffff\u0004\b\u0002\uffff\u0003\b\u0001\uffff\u0007\b\u0002\uffff\f\b\u0002\uffff\u0001\b\u0001\uffff\u0003\b\u0001\uffff\u0002\b\u0002\uffff\u0002\b\u0001\uffff\u0004\b\u0002\uffff\u0001\u0005\u0002\b\t\uffff\u0001\b\u0016\uffff\u0001\u0005\u0002\uffff\u0001\u0005\u0006\uffff\u0001\u0005", "", "\u0001\t\u0004\uffff\u0002\t\u0003\uffff\u0001\t\u0001\uffff\u0001\t\u0001\uffff\u0002\t\u0003\uffff\u0003\t\u0001\uffff\u0005\t\u0001\uffff\u0007\t\u0001\uffff\u0004\t\u0003\uffff\u0003\t\u0001\uffff\u0001\t\u0002\uffff\u0005\t\u0002\uffff\u0001\t\u0001\uffff\u0002\t\u0001\uffff\u0001\t\u0003\uffff\u0004\t\u0001\uffff\u0002\t\u0002\uffff\u0004\t\u0001\uffff\u0004\t\u0001\uffff\u0003\t\u0003\uffff\u0001\t\u0001\uffff\u0001\t\u0002\uffff\u0001\t\u0001\uffff\u0001\t\u0002\uffff\u0001\t\u0002\uffff\u0001\t\u0002\uffff\u0005\t\u0003\uffff\u0004\t\u0002\uffff\u0003\t\u0001\uffff\u0007\t\u0002\uffff\f\t\u0002\uffff\u0001\t\u0001\uffff\u0003\t\u0001\uffff\u0002\t\u0002\uffff\u0002\t\u0001\uffff\u0004\t\u0003\uffff\u0002\t\t\uffff\u0001\t\u0016\uffff\u0001\u0007\u0002\uffff\u0001\u0007\u0005\uffff\u0002\u0007", "", "", ""};
   static final short[] DFA8_eot = DFA.unpackEncodedString("\n\uffff");
   static final short[] DFA8_eof = DFA.unpackEncodedString("\u0004\uffff\u0001\u0005\u0001\uffff\u0001\u0007\u0003\uffff");
   static final char[] DFA8_min = DFA.unpackEncodedStringToUnsignedChars("\u0001\u001b\u0001\u0017\u0001\uffff\u0002\u0017\u0001\uffff\u0001\u0017\u0003\uffff");
   static final char[] DFA8_max = DFA.unpackEncodedStringToUnsignedChars("\u0001\u009d\u0001Ë\u0001\uffff\u0001Ë\u0001ì\u0001\uffff\u0001ì\u0003\uffff");
   static final short[] DFA8_accept = DFA.unpackEncodedString("\u0002\uffff\u0001\u0003\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0004\u0001\u0002\u0001\u0005");
   static final short[] DFA8_special = DFA.unpackEncodedString("\n\uffff}>");
   static final short[][] DFA8_transition;
   public static final BitSet FOLLOW_createSearchIndexStatement_in_dseSearchStatement31;
   public static final BitSet FOLLOW_dropSearchIndexStatement_in_dseSearchStatement46;
   public static final BitSet FOLLOW_commitSearchIndexStatement_in_dseSearchStatement63;
   public static final BitSet FOLLOW_reloadSearchIndexStatement_in_dseSearchStatement78;
   public static final BitSet FOLLOW_alterSearchIndexStatement_in_dseSearchStatement93;
   public static final BitSet FOLLOW_rebuildSearchIndexStatement_in_dseSearchStatement109;
   public static final BitSet FOLLOW_searchIndexResource_in_dseSearchResource136;
   public static final BitSet FOLLOW_K_CREATE_in_createSearchIndexStatement170;
   public static final BitSet FOLLOW_K_SEARCH_in_createSearchIndexStatement172;
   public static final BitSet FOLLOW_K_INDEX_in_createSearchIndexStatement174;
   public static final BitSet FOLLOW_K_IF_in_createSearchIndexStatement177;
   public static final BitSet FOLLOW_K_NOT_in_createSearchIndexStatement179;
   public static final BitSet FOLLOW_K_EXISTS_in_createSearchIndexStatement181;
   public static final BitSet FOLLOW_K_ON_in_createSearchIndexStatement194;
   public static final BitSet FOLLOW_columnFamilyName_in_createSearchIndexStatement200;
   public static final BitSet FOLLOW_K_WITH_in_createSearchIndexStatement209;
   public static final BitSet FOLLOW_indexOptions_DseSearchClause_in_createSearchIndexStatement213;
   public static final BitSet FOLLOW_K_ALTER_in_alterSearchIndexStatement255;
   public static final BitSet FOLLOW_K_SEARCH_in_alterSearchIndexStatement257;
   public static final BitSet FOLLOW_K_INDEX_in_alterSearchIndexStatement259;
   public static final BitSet FOLLOW_K_SCHEMA_in_alterSearchIndexStatement262;
   public static final BitSet FOLLOW_K_CONFIG_in_alterSearchIndexStatement266;
   public static final BitSet FOLLOW_K_ON_in_alterSearchIndexStatement277;
   public static final BitSet FOLLOW_columnFamilyName_in_alterSearchIndexStatement283;
   public static final BitSet FOLLOW_K_ADD_in_alterSearchIndexStatement315;
   public static final BitSet FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement321;
   public static final BitSet FOLLOW_K_WITH_in_alterSearchIndexStatement324;
   public static final BitSet FOLLOW_STRING_LITERAL_in_alterSearchIndexStatement330;
   public static final BitSet FOLLOW_K_ADD_in_alterSearchIndexStatement390;
   public static final BitSet FOLLOW_K_FIELD_in_alterSearchIndexStatement392;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement398;
   public static final BitSet FOLLOW_K_SET_in_alterSearchIndexStatement456;
   public static final BitSet FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement462;
   public static final BitSet FOLLOW_235_in_alterSearchIndexStatement465;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement471;
   public static final BitSet FOLLOW_232_in_alterSearchIndexStatement475;
   public static final BitSet FOLLOW_elementValue_DseSearchClause_in_alterSearchIndexStatement481;
   public static final BitSet FOLLOW_K_DROP_in_alterSearchIndexStatement540;
   public static final BitSet FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement546;
   public static final BitSet FOLLOW_235_in_alterSearchIndexStatement549;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement555;
   public static final BitSet FOLLOW_K_DROP_in_alterSearchIndexStatement615;
   public static final BitSet FOLLOW_K_FIELD_in_alterSearchIndexStatement617;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement623;
   public static final BitSet FOLLOW_K_DROP_in_dropSearchIndexStatement678;
   public static final BitSet FOLLOW_K_SEARCH_in_dropSearchIndexStatement680;
   public static final BitSet FOLLOW_K_INDEX_in_dropSearchIndexStatement682;
   public static final BitSet FOLLOW_K_ON_in_dropSearchIndexStatement690;
   public static final BitSet FOLLOW_columnFamilyName_in_dropSearchIndexStatement696;
   public static final BitSet FOLLOW_K_WITH_in_dropSearchIndexStatement705;
   public static final BitSet FOLLOW_K_OPTIONS_in_dropSearchIndexStatement707;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_dropSearchIndexStatement711;
   public static final BitSet FOLLOW_K_COMMIT_in_commitSearchIndexStatement744;
   public static final BitSet FOLLOW_K_SEARCH_in_commitSearchIndexStatement746;
   public static final BitSet FOLLOW_K_INDEX_in_commitSearchIndexStatement748;
   public static final BitSet FOLLOW_K_ON_in_commitSearchIndexStatement756;
   public static final BitSet FOLLOW_columnFamilyName_in_commitSearchIndexStatement762;
   public static final BitSet FOLLOW_K_RELOAD_in_reloadSearchIndexStatement793;
   public static final BitSet FOLLOW_K_SEARCH_in_reloadSearchIndexStatement795;
   public static final BitSet FOLLOW_K_INDEX_in_reloadSearchIndexStatement797;
   public static final BitSet FOLLOW_K_ON_in_reloadSearchIndexStatement799;
   public static final BitSet FOLLOW_columnFamilyName_in_reloadSearchIndexStatement805;
   public static final BitSet FOLLOW_K_WITH_in_reloadSearchIndexStatement814;
   public static final BitSet FOLLOW_K_OPTIONS_in_reloadSearchIndexStatement816;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_reloadSearchIndexStatement820;
   public static final BitSet FOLLOW_K_REBUILD_in_rebuildSearchIndexStatement855;
   public static final BitSet FOLLOW_K_SEARCH_in_rebuildSearchIndexStatement857;
   public static final BitSet FOLLOW_K_INDEX_in_rebuildSearchIndexStatement859;
   public static final BitSet FOLLOW_K_ON_in_rebuildSearchIndexStatement861;
   public static final BitSet FOLLOW_columnFamilyName_in_rebuildSearchIndexStatement867;
   public static final BitSet FOLLOW_K_WITH_in_rebuildSearchIndexStatement876;
   public static final BitSet FOLLOW_K_OPTIONS_in_rebuildSearchIndexStatement878;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_rebuildSearchIndexStatement882;
   public static final BitSet FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause915;
   public static final BitSet FOLLOW_K_AND_in_indexOptions_DseSearchClause926;
   public static final BitSet FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause930;
   public static final BitSet FOLLOW_columnList_DseSearchClause_in_indexOption_DseSearchClause960;
   public static final BitSet FOLLOW_profileList_DseSearchClause_in_indexOption_DseSearchClause972;
   public static final BitSet FOLLOW_K_CONFIG_in_indexOption_DseSearchClause982;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause986;
   public static final BitSet FOLLOW_K_OPTIONS_in_indexOption_DseSearchClause996;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause1000;
   public static final BitSet FOLLOW_K_COLUMNS_in_columnList_DseSearchClause1023;
   public static final BitSet FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1033;
   public static final BitSet FOLLOW_223_in_columnList_DseSearchClause1045;
   public static final BitSet FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1049;
   public static final BitSet FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1085;
   public static final BitSet FOLLOW_226_in_column_DseSearchClause1091;
   public static final BitSet FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1095;
   public static final BitSet FOLLOW_optionMap_DseSearchClause_in_column_DseSearchClause1110;
   public static final BitSet FOLLOW_K_PROFILES_in_profileList_DseSearchClause1141;
   public static final BitSet FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1151;
   public static final BitSet FOLLOW_223_in_profileList_DseSearchClause1163;
   public static final BitSet FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1167;
   public static final BitSet FOLLOW_240_in_optionMap_DseSearchClause1192;
   public static final BitSet FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1208;
   public static final BitSet FOLLOW_228_in_optionMap_DseSearchClause1210;
   public static final BitSet FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1214;
   public static final BitSet FOLLOW_223_in_optionMap_DseSearchClause1230;
   public static final BitSet FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1234;
   public static final BitSet FOLLOW_228_in_optionMap_DseSearchClause1236;
   public static final BitSet FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1240;
   public static final BitSet FOLLOW_241_in_optionMap_DseSearchClause1256;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1288;
   public static final BitSet FOLLOW_236_in_elementPath_DseSearchClause1300;
   public static final BitSet FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1312;
   public static final BitSet FOLLOW_223_in_elementPath_DseSearchClause1326;
   public static final BitSet FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1330;
   public static final BitSet FOLLOW_238_in_elementPath_DseSearchClause1345;
   public static final BitSet FOLLOW_226_in_elementPath_DseSearchClause1364;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1376;
   public static final BitSet FOLLOW_236_in_elementPath_DseSearchClause1390;
   public static final BitSet FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1404;
   public static final BitSet FOLLOW_223_in_elementPath_DseSearchClause1420;
   public static final BitSet FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1424;
   public static final BitSet FOLLOW_238_in_elementPath_DseSearchClause1441;
   public static final BitSet FOLLOW_235_in_attribute_DseSearchClause1490;
   public static final BitSet FOLLOW_elementName_DseSearchClause_in_attribute_DseSearchClause1494;
   public static final BitSet FOLLOW_232_in_attribute_DseSearchClause1496;
   public static final BitSet FOLLOW_STRING_LITERAL_in_attribute_DseSearchClause1500;
   public static final BitSet FOLLOW_IDENT_in_elementName_DseSearchClause1525;
   public static final BitSet FOLLOW_QUOTED_NAME_in_elementName_DseSearchClause1550;
   public static final BitSet FOLLOW_unreserved_keyword_in_elementName_DseSearchClause1569;
   public static final BitSet FOLLOW_IDENT_in_columnName_DseSearchClause1594;
   public static final BitSet FOLLOW_QUOTED_NAME_in_columnName_DseSearchClause1619;
   public static final BitSet FOLLOW_237_in_columnName_DseSearchClause1638;
   public static final BitSet FOLLOW_unreserved_keyword_in_columnName_DseSearchClause1664;
   public static final BitSet FOLLOW_IDENT_in_elementValue_DseSearchClause1689;
   public static final BitSet FOLLOW_QUOTED_NAME_in_elementValue_DseSearchClause1723;
   public static final BitSet FOLLOW_optionValue_DseSearchClause_in_elementValue_DseSearchClause1751;
   public static final BitSet FOLLOW_unreserved_keyword_in_elementValue_DseSearchClause1763;
   public static final BitSet FOLLOW_IDENT_in_profileName_DseSearchClause1797;
   public static final BitSet FOLLOW_QUOTED_NAME_in_profileName_DseSearchClause1822;
   public static final BitSet FOLLOW_unreserved_keyword_in_profileName_DseSearchClause1841;
   public static final BitSet FOLLOW_IDENT_in_optionName_DseSearchClause1866;
   public static final BitSet FOLLOW_unreserved_keyword_in_optionName_DseSearchClause1891;
   public static final BitSet FOLLOW_K_ALL_in_searchIndexResource1955;
   public static final BitSet FOLLOW_K_SEARCH_in_searchIndexResource1957;
   public static final BitSet FOLLOW_K_INDICES_in_searchIndexResource1959;
   public static final BitSet FOLLOW_K_SEARCH_in_searchIndexResource1969;
   public static final BitSet FOLLOW_K_KEYSPACE_in_searchIndexResource1971;
   public static final BitSet FOLLOW_keyspaceName_in_searchIndexResource1975;
   public static final BitSet FOLLOW_K_SEARCH_in_searchIndexResource1985;
   public static final BitSet FOLLOW_K_INDEX_in_searchIndexResource1987;
   public static final BitSet FOLLOW_columnFamilyName_in_searchIndexResource1991;

   public Cql_DseSearchParser(TokenStream input, CqlParser gCql) {
      this(input, new RecognizerSharedState(), gCql);
   }

   public Cql_DseSearchParser(TokenStream input, RecognizerSharedState state, CqlParser gCql) {
      super(input, state);
      this.dseResourceFactory = new DseResourceFactory();
      this.dfa8 = new Cql_DseSearchParser.DFA8(this);
      this.gCql = gCql;
      this.gParent = gCql;
   }

   protected void addRecognitionError(String msg) {
      this.gParent.addRecognitionError(msg);
   }

   public final ParsedStatement alterSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      Token json = null;
      CFName cf = null;
      XmlPath path = null;
      String field = null;
      String attr = null;
      String elementValue = null;
      boolean config = false;

      try {
         try {
            this.match(this.input, 31, FOLLOW_K_ALTER_in_alterSearchIndexStatement255);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_alterSearchIndexStatement257);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_alterSearchIndexStatement259);
            int alt4 = true;
            int LA4_0 = this.input.LA(1);
            byte alt4;
            if(LA4_0 == 152) {
               alt4 = 1;
            } else {
               if(LA4_0 != 55) {
                  NoViableAltException nvae = new NoViableAltException("", 4, 0, this.input);
                  throw nvae;
               }

               alt4 = 2;
            }

            switch(alt4) {
            case 1:
               this.match(this.input, 152, FOLLOW_K_SCHEMA_in_alterSearchIndexStatement262);
               break;
            case 2:
               this.match(this.input, 55, FOLLOW_K_CONFIG_in_alterSearchIndexStatement266);
               config = true;
            }

            this.match(this.input, 127, FOLLOW_K_ON_in_alterSearchIndexStatement277);
            this.pushFollow(FOLLOW_columnFamilyName_in_alterSearchIndexStatement283);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            int alt8 = true;
            int alt8 = this.dfa8.predict(this.input);
            byte alt7;
            int LA7_0;
            switch(alt8) {
            case 1:
               this.match(this.input, 27, FOLLOW_K_ADD_in_alterSearchIndexStatement315);
               this.pushFollow(FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement321);
               path = this.elementPath_DseSearchClause();
               --this.state._fsp;
               alt7 = 2;
               LA7_0 = this.input.LA(1);
               if(LA7_0 == 191) {
                  alt7 = 1;
               }

               switch(alt7) {
               case 1:
                  this.match(this.input, 191, FOLLOW_K_WITH_in_alterSearchIndexStatement324);
                  json = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_alterSearchIndexStatement330);
               default:
                  stmt = searchStatementFactory.alterSearchIndexStatement(cf, config, "ADD", path, (String)null, (String)null, json != null?json.getText():null);
                  return stmt;
               }
            case 2:
               this.match(this.input, 27, FOLLOW_K_ADD_in_alterSearchIndexStatement390);
               this.match(this.input, 75, FOLLOW_K_FIELD_in_alterSearchIndexStatement392);
               this.pushFollow(FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement398);
               field = this.elementName_DseSearchClause();
               --this.state._fsp;
               stmt = searchStatementFactory.alterSearchIndexStatement(cf, config, "ADD", XmlPath.fromString("field"), (String)null, field, (String)null);
               break;
            case 3:
               this.match(this.input, 157, FOLLOW_K_SET_in_alterSearchIndexStatement456);
               this.pushFollow(FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement462);
               path = this.elementPath_DseSearchClause();
               --this.state._fsp;
               alt7 = 2;
               LA7_0 = this.input.LA(1);
               if(LA7_0 == 235) {
                  alt7 = 1;
               }

               switch(alt7) {
               case 1:
                  this.match(this.input, 235, FOLLOW_235_in_alterSearchIndexStatement465);
                  this.pushFollow(FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement471);
                  attr = this.elementName_DseSearchClause();
                  --this.state._fsp;
               default:
                  this.match(this.input, 232, FOLLOW_232_in_alterSearchIndexStatement475);
                  this.pushFollow(FOLLOW_elementValue_DseSearchClause_in_alterSearchIndexStatement481);
                  elementValue = this.elementValue_DseSearchClause();
                  --this.state._fsp;
                  stmt = searchStatementFactory.alterSearchIndexStatement(cf, config, "SET", path, attr, elementValue, (String)null);
                  return stmt;
               }
            case 4:
               this.match(this.input, 70, FOLLOW_K_DROP_in_alterSearchIndexStatement540);
               this.pushFollow(FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement546);
               path = this.elementPath_DseSearchClause();
               --this.state._fsp;
               alt7 = 2;
               LA7_0 = this.input.LA(1);
               if(LA7_0 == 235) {
                  alt7 = 1;
               }

               switch(alt7) {
               case 1:
                  this.match(this.input, 235, FOLLOW_235_in_alterSearchIndexStatement549);
                  this.pushFollow(FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement555);
                  attr = this.elementName_DseSearchClause();
                  --this.state._fsp;
               default:
                  stmt = searchStatementFactory.alterSearchIndexStatement(cf, config, "DROP", path, attr, (String)null, (String)null);
                  return stmt;
               }
            case 5:
               this.match(this.input, 70, FOLLOW_K_DROP_in_alterSearchIndexStatement615);
               this.match(this.input, 75, FOLLOW_K_FIELD_in_alterSearchIndexStatement617);
               this.pushFollow(FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement623);
               field = this.elementName_DseSearchClause();
               --this.state._fsp;
               stmt = searchStatementFactory.alterSearchIndexStatement(cf, config, "DROP", XmlPath.fromString("field"), (String)null, field, (String)null);
            }
         } catch (RecognitionException var17) {
            this.reportError(var17);
            this.recover(this.input, var17);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.attribute_DseSearchClause_return attribute_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.attribute_DseSearchClause_return retval = new Cql_DseSearchParser.attribute_DseSearchClause_return();
      retval.start = this.input.LT(1);
      Token val = null;
      String id = null;

      try {
         try {
            this.match(this.input, 235, FOLLOW_235_in_attribute_DseSearchClause1490);
            this.pushFollow(FOLLOW_elementName_DseSearchClause_in_attribute_DseSearchClause1494);
            id = this.elementName_DseSearchClause();
            --this.state._fsp;
            this.match(this.input, 232, FOLLOW_232_in_attribute_DseSearchClause1496);
            val = (Token)this.match(this.input, 207, FOLLOW_STRING_LITERAL_in_attribute_DseSearchClause1500);
            retval.key = id;
            retval.value = val != null?val.getText():null;
            retval.stop = this.input.LT(-1);
         } catch (RecognitionException var8) {
            this.reportError(var8);
            this.recover(this.input, var8);
         }

         return retval;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.columnList_DseSearchClause_return columnList_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.columnList_DseSearchClause_return retval = new Cql_DseSearchParser.columnList_DseSearchClause_return();
      retval.start = this.input.LT(1);
      Cql_DseSearchParser.column_DseSearchClause_return column = null;

      try {
         this.match(this.input, 52, FOLLOW_K_COLUMNS_in_columnList_DseSearchClause1023);
         this.pushFollow(FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1033);
         column = this.column_DseSearchClause();
         --this.state._fsp;
         retval.columns = new ArrayList();
         retval.columnOptions = new HashMap();
         retval.columns.add(column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).name:null);
         retval.columnOptions.put(column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).name:null, column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).options:null);

         while(true) {
            int alt14 = 2;
            int LA14_0 = this.input.LA(1);
            if(LA14_0 == 223) {
               alt14 = 1;
            }

            switch(alt14) {
            case 1:
               this.match(this.input, 223, FOLLOW_223_in_columnList_DseSearchClause1045);
               this.pushFollow(FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1049);
               column = this.column_DseSearchClause();
               --this.state._fsp;
               retval.columns.add(column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).name:null);
               retval.columnOptions.put(column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).name:null, column != null?((Cql_DseSearchParser.column_DseSearchClause_return)column).options:null);
               break;
            default:
               retval.stop = this.input.LT(-1);
               return retval;
            }
         }
      } catch (RecognitionException var8) {
         this.reportError(var8);
         this.recover(this.input, var8);
         return retval;
      } finally {
         ;
      }
   }

   public final String columnName_DseSearchClause() throws RecognitionException {
      String id = null;
      Token t = null;
      Token w = null;
      String k = null;

      try {
         try {
            int alt26 = true;
            byte alt26;
            switch(this.input.LA(1)) {
            case 23:
               alt26 = 1;
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
            case 202:
            case 204:
            case 205:
            case 206:
            case 207:
            case 208:
            case 209:
            case 210:
            case 211:
            case 212:
            case 213:
            case 214:
            case 215:
            case 216:
            case 217:
            case 218:
            case 219:
            case 220:
            case 221:
            case 222:
            case 223:
            case 224:
            case 225:
            case 226:
            case 227:
            case 228:
            case 229:
            case 230:
            case 231:
            case 232:
            case 233:
            case 234:
            case 235:
            case 236:
            default:
               NoViableAltException nvae = new NoViableAltException("", 26, 0, this.input);
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
               alt26 = 4;
               break;
            case 203:
               alt26 = 2;
               break;
            case 237:
               alt26 = 3;
            }

            switch(alt26) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_columnName_DseSearchClause1594);
               id = (t != null?t.getText():null).toLowerCase();
               break;
            case 2:
               t = (Token)this.match(this.input, 203, FOLLOW_QUOTED_NAME_in_columnName_DseSearchClause1619);
               id = t != null?t.getText():null;
               break;
            case 3:
               w = (Token)this.match(this.input, 237, FOLLOW_237_in_columnName_DseSearchClause1638);
               id = w != null?w.getText():null;
               break;
            case 4:
               this.pushFollow(FOLLOW_unreserved_keyword_in_columnName_DseSearchClause1664);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               id = k.toLowerCase();
            }
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return id;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.column_DseSearchClause_return column_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.column_DseSearchClause_return retval = new Cql_DseSearchParser.column_DseSearchClause_return();
      retval.start = this.input.LT(1);
      String c = null;
      String cn = null;
      Map<String, String> map = null;
      StringBuilder builder = new StringBuilder();

      try {
         this.pushFollow(FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1085);
         c = this.columnName_DseSearchClause();
         --this.state._fsp;
         builder.append(c);

         while(true) {
            int alt16 = 2;
            int LA16_0 = this.input.LA(1);
            if(LA16_0 == 226) {
               alt16 = 1;
            }

            switch(alt16) {
            case 1:
               this.match(this.input, 226, FOLLOW_226_in_column_DseSearchClause1091);
               this.pushFollow(FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1095);
               cn = this.columnName_DseSearchClause();
               --this.state._fsp;
               builder.append('.');
               builder.append(cn);
               break;
            default:
               alt16 = 2;
               LA16_0 = this.input.LA(1);
               if(LA16_0 == 240) {
                  alt16 = 1;
               }

               switch(alt16) {
               case 1:
                  this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_column_DseSearchClause1110);
                  map = this.optionMap_DseSearchClause();
                  --this.state._fsp;
               default:
                  retval.name = builder.toString();
                  retval.options = map;
                  retval.stop = this.input.LT(-1);
                  return retval;
               }
            }
         }
      } catch (RecognitionException var11) {
         this.reportError(var11);
         this.recover(this.input, var11);
         return retval;
      } finally {
         ;
      }
   }

   public final ParsedStatement commitSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      CFName cf = null;

      try {
         try {
            this.match(this.input, 53, FOLLOW_K_COMMIT_in_commitSearchIndexStatement744);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_commitSearchIndexStatement746);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_commitSearchIndexStatement748);
            this.match(this.input, 127, FOLLOW_K_ON_in_commitSearchIndexStatement756);
            this.pushFollow(FOLLOW_columnFamilyName_in_commitSearchIndexStatement762);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            stmt = searchStatementFactory.commitSearchIndexStatement(cf);
         } catch (RecognitionException var7) {
            this.reportError(var7);
            this.recover(this.input, var7);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final ParsedStatement createSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      CFName cf = null;
      ParserRuleReturnScope options = null;
      boolean ifNotExists = false;

      try {
         try {
            this.match(this.input, 59, FOLLOW_K_CREATE_in_createSearchIndexStatement170);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_createSearchIndexStatement172);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_createSearchIndexStatement174);
            int alt2 = 2;
            int LA2_0 = this.input.LA(1);
            if(LA2_0 == 87) {
               alt2 = 1;
            }

            switch(alt2) {
            case 1:
               this.match(this.input, 87, FOLLOW_K_IF_in_createSearchIndexStatement177);
               this.match(this.input, 123, FOLLOW_K_NOT_in_createSearchIndexStatement179);
               this.match(this.input, 74, FOLLOW_K_EXISTS_in_createSearchIndexStatement181);
               ifNotExists = true;
            default:
               this.match(this.input, 127, FOLLOW_K_ON_in_createSearchIndexStatement194);
               this.pushFollow(FOLLOW_columnFamilyName_in_createSearchIndexStatement200);
               cf = this.gCql.columnFamilyName();
               --this.state._fsp;
               int alt3 = 2;
               int LA3_0 = this.input.LA(1);
               if(LA3_0 == 191) {
                  alt3 = 1;
               }

               switch(alt3) {
               case 1:
                  this.match(this.input, 191, FOLLOW_K_WITH_in_createSearchIndexStatement209);
                  this.pushFollow(FOLLOW_indexOptions_DseSearchClause_in_createSearchIndexStatement213);
                  options = this.indexOptions_DseSearchClause();
                  --this.state._fsp;
               default:
                  stmt = searchStatementFactory.createSearchIndexStatement(cf, ifNotExists, options != null?((Cql_DseSearchParser.indexOptions_DseSearchClause_return)options).columnList:null, options != null?((Cql_DseSearchParser.indexOptions_DseSearchClause_return)options).columnOptions:null, options != null?((Cql_DseSearchParser.indexOptions_DseSearchClause_return)options).profileList:null, options != null?((Cql_DseSearchParser.indexOptions_DseSearchClause_return)options).configOptions:null, options != null?((Cql_DseSearchParser.indexOptions_DseSearchClause_return)options).requestOptions:null);
               }
            }
         } catch (RecognitionException var12) {
            this.reportError(var12);
            this.recover(this.input, var12);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
      this.gParent.displayRecognitionError(tokenNames, e);
   }

   public final ParsedStatement dropSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      CFName cf = null;
      Map options = null;

      try {
         try {
            this.match(this.input, 70, FOLLOW_K_DROP_in_dropSearchIndexStatement678);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_dropSearchIndexStatement680);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_dropSearchIndexStatement682);
            this.match(this.input, 127, FOLLOW_K_ON_in_dropSearchIndexStatement690);
            this.pushFollow(FOLLOW_columnFamilyName_in_dropSearchIndexStatement696);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            int alt9 = 2;
            int LA9_0 = this.input.LA(1);
            if(LA9_0 == 191) {
               alt9 = 1;
            }

            switch(alt9) {
            case 1:
               this.match(this.input, 191, FOLLOW_K_WITH_in_dropSearchIndexStatement705);
               this.match(this.input, 128, FOLLOW_K_OPTIONS_in_dropSearchIndexStatement707);
               this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_dropSearchIndexStatement711);
               options = this.optionMap_DseSearchClause();
               --this.state._fsp;
            default:
               stmt = searchStatementFactory.dropSearchIndexStatement(cf, options);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final IResource dseSearchResource() throws RecognitionException {
      IResource res = null;
      IResource c = null;

      try {
         try {
            this.pushFollow(FOLLOW_searchIndexResource_in_dseSearchResource136);
            c = this.searchIndexResource();
            --this.state._fsp;
            res = c;
         } catch (RecognitionException var7) {
            this.reportError(var7);
            this.recover(this.input, var7);
         }

         return res;
      } finally {
         ;
      }
   }

   public final ParsedStatement dseSearchStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      ParsedStatement st1 = null;
      ParsedStatement st2 = null;
      ParsedStatement st3 = null;
      ParsedStatement st4 = null;
      ParsedStatement st5 = null;
      ParsedStatement st6 = null;

      try {
         try {
            int alt1 = true;
            byte alt1;
            switch(this.input.LA(1)) {
            case 31:
               alt1 = 5;
               break;
            case 53:
               alt1 = 3;
               break;
            case 59:
               alt1 = 1;
               break;
            case 70:
               alt1 = 2;
               break;
            case 140:
               alt1 = 6;
               break;
            case 141:
               alt1 = 4;
               break;
            default:
               NoViableAltException nvae = new NoViableAltException("", 1, 0, this.input);
               throw nvae;
            }

            switch(alt1) {
            case 1:
               this.pushFollow(FOLLOW_createSearchIndexStatement_in_dseSearchStatement31);
               st1 = this.createSearchIndexStatement();
               --this.state._fsp;
               stmt = st1;
               break;
            case 2:
               this.pushFollow(FOLLOW_dropSearchIndexStatement_in_dseSearchStatement46);
               st2 = this.dropSearchIndexStatement();
               --this.state._fsp;
               stmt = st2;
               break;
            case 3:
               this.pushFollow(FOLLOW_commitSearchIndexStatement_in_dseSearchStatement63);
               st3 = this.commitSearchIndexStatement();
               --this.state._fsp;
               stmt = st3;
               break;
            case 4:
               this.pushFollow(FOLLOW_reloadSearchIndexStatement_in_dseSearchStatement78);
               st4 = this.reloadSearchIndexStatement();
               --this.state._fsp;
               stmt = st4;
               break;
            case 5:
               this.pushFollow(FOLLOW_alterSearchIndexStatement_in_dseSearchStatement93);
               st5 = this.alterSearchIndexStatement();
               --this.state._fsp;
               stmt = st5;
               break;
            case 6:
               this.pushFollow(FOLLOW_rebuildSearchIndexStatement_in_dseSearchStatement109);
               st6 = this.rebuildSearchIndexStatement();
               --this.state._fsp;
               stmt = st6;
            }
         } catch (RecognitionException var13) {
            this.reportError(var13);
            this.recover(this.input, var13);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final String elementName_DseSearchClause() throws RecognitionException {
      String name = null;
      Token t = null;
      String k = null;

      try {
         try {
            int alt25 = true;
            byte alt25;
            switch(this.input.LA(1)) {
            case 23:
               alt25 = 1;
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
            case 202:
            default:
               NoViableAltException nvae = new NoViableAltException("", 25, 0, this.input);
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
               alt25 = 3;
               break;
            case 203:
               alt25 = 2;
            }

            switch(alt25) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_elementName_DseSearchClause1525);
               name = t != null?t.getText():null;
               break;
            case 2:
               t = (Token)this.match(this.input, 203, FOLLOW_QUOTED_NAME_in_elementName_DseSearchClause1550);
               name = t != null?t.getText():null;
               break;
            case 3:
               this.pushFollow(FOLLOW_unreserved_keyword_in_elementName_DseSearchClause1569);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               name = k;
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return name;
      } finally {
         ;
      }
   }

   public final XmlPath elementPath_DseSearchClause() throws RecognitionException {
      XmlPath path = null;
      String el = null;
      ParserRuleReturnScope at = null;
      String eln = null;
      ParserRuleReturnScope atn = null;
      XmlPath.Builder builder = new XmlPath.Builder();

      try {
         this.pushFollow(FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1288);
         el = this.elementName_DseSearchClause();
         --this.state._fsp;
         builder.addElement(el);
         int alt21 = 2;
         int LA21_0 = this.input.LA(1);
         if(LA21_0 == 236) {
            alt21 = 1;
         }

         byte alt20;
         int LA20_0;
         switch(alt21) {
         case 1:
            this.match(this.input, 236, FOLLOW_236_in_elementPath_DseSearchClause1300);
            this.pushFollow(FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1312);
            at = this.attribute_DseSearchClause();
            --this.state._fsp;
            builder.addAttribute(at != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)at).key:null, at != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)at).value:null);

            label214:
            while(true) {
               alt20 = 2;
               LA20_0 = this.input.LA(1);
               if(LA20_0 == 223) {
                  alt20 = 1;
               }

               switch(alt20) {
               case 1:
                  this.match(this.input, 223, FOLLOW_223_in_elementPath_DseSearchClause1326);
                  this.pushFollow(FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1330);
                  at = this.attribute_DseSearchClause();
                  --this.state._fsp;
                  builder.addAttribute(at != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)at).key:null, at != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)at).value:null);
                  break;
               default:
                  this.match(this.input, 238, FOLLOW_238_in_elementPath_DseSearchClause1345);
                  break label214;
               }
            }
         }

         while(true) {
            label236:
            while(true) {
               alt20 = 2;
               LA20_0 = this.input.LA(1);
               if(LA20_0 == 226) {
                  alt20 = 1;
               }

               switch(alt20) {
               case 1:
                  this.match(this.input, 226, FOLLOW_226_in_elementPath_DseSearchClause1364);
                  this.pushFollow(FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1376);
                  eln = this.elementName_DseSearchClause();
                  --this.state._fsp;
                  builder.addElement(eln);
                  int alt23 = 2;
                  int LA23_0 = this.input.LA(1);
                  if(LA23_0 == 236) {
                     alt23 = 1;
                  }

                  switch(alt23) {
                  case 1:
                     this.match(this.input, 236, FOLLOW_236_in_elementPath_DseSearchClause1390);
                     this.pushFollow(FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1404);
                     atn = this.attribute_DseSearchClause();
                     --this.state._fsp;
                     builder.addAttribute(atn != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)atn).key:null, atn != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)atn).value:null);

                     while(true) {
                        int alt22 = 2;
                        int LA22_0 = this.input.LA(1);
                        if(LA22_0 == 223) {
                           alt22 = 1;
                        }

                        switch(alt22) {
                        case 1:
                           this.match(this.input, 223, FOLLOW_223_in_elementPath_DseSearchClause1420);
                           this.pushFollow(FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1424);
                           atn = this.attribute_DseSearchClause();
                           --this.state._fsp;
                           builder.addAttribute(atn != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)atn).key:null, atn != null?((Cql_DseSearchParser.attribute_DseSearchClause_return)atn).value:null);
                           break;
                        default:
                           this.match(this.input, 238, FOLLOW_238_in_elementPath_DseSearchClause1441);
                           continue label236;
                        }
                     }
                  default:
                     continue;
                  }
               default:
                  path = builder.build();
                  return path;
               }
            }
         }
      } catch (RecognitionException var18) {
         this.reportError(var18);
         this.recover(this.input, var18);
         return path;
      } finally {
         ;
      }
   }

   public final String elementValue_DseSearchClause() throws RecognitionException {
      String id = null;
      Token t = null;
      ParserRuleReturnScope v = null;
      String k = null;

      try {
         try {
            int alt27 = true;
            byte alt27;
            switch(this.input.LA(1)) {
            case 6:
            case 17:
            case 24:
            case 207:
               alt27 = 3;
               break;
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
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
            case 202:
            case 204:
            case 205:
            case 206:
            default:
               NoViableAltException nvae = new NoViableAltException("", 27, 0, this.input);
               throw nvae;
            case 23:
               alt27 = 1;
               break;
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
               alt27 = 4;
               break;
            case 203:
               alt27 = 2;
            }

            switch(alt27) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_elementValue_DseSearchClause1689);
               id = t != null?t.getText():null;
               break;
            case 2:
               t = (Token)this.match(this.input, 203, FOLLOW_QUOTED_NAME_in_elementValue_DseSearchClause1723);
               id = t != null?t.getText():null;
               break;
            case 3:
               this.pushFollow(FOLLOW_optionValue_DseSearchClause_in_elementValue_DseSearchClause1751);
               v = this.optionValue_DseSearchClause();
               --this.state._fsp;
               id = v != null?this.input.toString(v.start, v.stop):null;
               break;
            case 4:
               this.pushFollow(FOLLOW_unreserved_keyword_in_elementValue_DseSearchClause1763);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               id = k;
            }
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return id;
      } finally {
         ;
      }
   }

   public Parser[] getDelegates() {
      return new Parser[0];
   }

   public String getGrammarFileName() {
      return "DseSearchParser.g";
   }

   protected String getSearchResourceRootName() {
      if(searchResourceRootName == null) {
         throw new InvalidRequestException("Permission management of search resources is not supported on this node");
      } else {
         return searchResourceRootName;
      }
   }

   public String[] getTokenNames() {
      return CqlParser.tokenNames;
   }

   public final Cql_DseSearchParser.indexOption_DseSearchClause_return indexOption_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.indexOption_DseSearchClause_return retval = new Cql_DseSearchParser.indexOption_DseSearchClause_return();
      retval.start = this.input.LT(1);
      ParserRuleReturnScope list = null;
      List<String> profiles = null;
      Map options = null;

      try {
         try {
            int alt13 = true;
            byte alt13;
            switch(this.input.LA(1)) {
            case 52:
               alt13 = 1;
               break;
            case 55:
               alt13 = 3;
               break;
            case 128:
               alt13 = 4;
               break;
            case 139:
               alt13 = 2;
               break;
            default:
               NoViableAltException nvae = new NoViableAltException("", 13, 0, this.input);
               throw nvae;
            }

            switch(alt13) {
            case 1:
               this.pushFollow(FOLLOW_columnList_DseSearchClause_in_indexOption_DseSearchClause960);
               list = this.columnList_DseSearchClause();
               --this.state._fsp;
               retval.columnList = list != null?((Cql_DseSearchParser.columnList_DseSearchClause_return)list).columns:null;
               retval.columnOptions = list != null?((Cql_DseSearchParser.columnList_DseSearchClause_return)list).columnOptions:null;
               break;
            case 2:
               this.pushFollow(FOLLOW_profileList_DseSearchClause_in_indexOption_DseSearchClause972);
               profiles = this.profileList_DseSearchClause();
               --this.state._fsp;
               retval.profileList = profiles;
               break;
            case 3:
               this.match(this.input, 55, FOLLOW_K_CONFIG_in_indexOption_DseSearchClause982);
               this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause986);
               options = this.optionMap_DseSearchClause();
               --this.state._fsp;
               retval.configOptions = options;
               break;
            case 4:
               this.match(this.input, 128, FOLLOW_K_OPTIONS_in_indexOption_DseSearchClause996);
               this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause1000);
               options = this.optionMap_DseSearchClause();
               --this.state._fsp;
               retval.requestOptions = options;
            }

            retval.stop = this.input.LT(-1);
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return retval;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.indexOptions_DseSearchClause_return indexOptions_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.indexOptions_DseSearchClause_return retval = new Cql_DseSearchParser.indexOptions_DseSearchClause_return();
      retval.start = this.input.LT(1);
      Cql_DseSearchParser.indexOption_DseSearchClause_return option = null;

      try {
         this.pushFollow(FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause915);
         option = this.indexOption_DseSearchClause();
         --this.state._fsp;
         retval.columnList = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).columnList:null;
         retval.columnOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).columnOptions:null;
         retval.profileList = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).profileList:null;
         retval.configOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).configOptions:null;
         retval.requestOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).requestOptions:null;

         while(true) {
            int alt12 = 2;
            int LA12_0 = this.input.LA(1);
            if(LA12_0 == 32) {
               alt12 = 1;
            }

            switch(alt12) {
            case 1:
               this.match(this.input, 32, FOLLOW_K_AND_in_indexOptions_DseSearchClause926);
               this.pushFollow(FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause930);
               option = this.indexOption_DseSearchClause();
               --this.state._fsp;
               if((option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).columnList:null) != null) {
                  if(retval.columnList == null) {
                     retval.columnList = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).columnList:null;
                     retval.columnOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).columnOptions:null;
                  } else {
                     this.addRecognitionError("Only one COLUMNS clause is supported");
                  }
               }

               if((option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).profileList:null) != null) {
                  if(retval.profileList == null) {
                     retval.profileList = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).profileList:null;
                  } else {
                     this.addRecognitionError("Only one PROFILES clause is supported");
                  }
               }

               if((option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).configOptions:null) != null) {
                  if(retval.configOptions == null) {
                     retval.configOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).configOptions:null;
                  } else {
                     this.addRecognitionError("Only one CONFIG clause is supported");
                  }
               }

               if((option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).requestOptions:null) != null) {
                  if(retval.requestOptions == null) {
                     retval.requestOptions = option != null?((Cql_DseSearchParser.indexOption_DseSearchClause_return)option).requestOptions:null;
                  } else {
                     this.addRecognitionError("Only one OPTIONS clause is supported");
                  }
               }
               break;
            default:
               retval.stop = this.input.LT(-1);
               return retval;
            }
         }
      } catch (RecognitionException var8) {
         this.reportError(var8);
         this.recover(this.input, var8);
         return retval;
      } finally {
         ;
      }
   }

   public final Map<String, String> optionMap_DseSearchClause() throws RecognitionException {
      Map<String, String> options = null;
      ParserRuleReturnScope k1 = null;
      ParserRuleReturnScope v1 = null;
      ParserRuleReturnScope kn = null;
      Cql_DseSearchParser.optionValue_DseSearchClause_return vn = null;

      try {
         try {
            this.match(this.input, 240, FOLLOW_240_in_optionMap_DseSearchClause1192);
            int alt19 = 2;
            int LA19_0 = this.input.LA(1);
            if(LA19_0 == 23 || LA19_0 >= 28 && LA19_0 <= 29 || LA19_0 == 33 || LA19_0 == 35 || LA19_0 >= 37 && LA19_0 <= 38 || LA19_0 >= 42 && LA19_0 <= 44 || LA19_0 >= 46 && LA19_0 <= 50 || LA19_0 >= 52 && LA19_0 <= 58 || LA19_0 >= 60 && LA19_0 <= 63 || LA19_0 >= 67 && LA19_0 <= 69 || LA19_0 == 71 || LA19_0 >= 74 && LA19_0 <= 78 || LA19_0 == 81 || LA19_0 >= 83 && LA19_0 <= 84 || LA19_0 == 86 || LA19_0 >= 90 && LA19_0 <= 93 || LA19_0 >= 95 && LA19_0 <= 96 || LA19_0 >= 99 && LA19_0 <= 102 || LA19_0 >= 104 && LA19_0 <= 107 || LA19_0 >= 109 && LA19_0 <= 111 || LA19_0 == 115 || LA19_0 == 117 || LA19_0 == 120 || LA19_0 == 122 || LA19_0 == 125 || LA19_0 == 128 || LA19_0 >= 131 && LA19_0 <= 135 || LA19_0 >= 139 && LA19_0 <= 142 || LA19_0 >= 145 && LA19_0 <= 147 || LA19_0 >= 149 && LA19_0 <= 155 || LA19_0 >= 158 && LA19_0 <= 169 || LA19_0 == 172 || LA19_0 >= 174 && LA19_0 <= 176 || LA19_0 >= 178 && LA19_0 <= 179 || LA19_0 >= 182 && LA19_0 <= 183 || LA19_0 >= 185 && LA19_0 <= 188 || LA19_0 >= 192 && LA19_0 <= 193) {
               alt19 = 1;
            }

            switch(alt19) {
            case 1:
               this.pushFollow(FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1208);
               k1 = this.optionName_DseSearchClause();
               --this.state._fsp;
               this.match(this.input, 228, FOLLOW_228_in_optionMap_DseSearchClause1210);
               this.pushFollow(FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1214);
               v1 = this.optionValue_DseSearchClause();
               --this.state._fsp;
               options = new HashMap();
               options.put(k1 != null?this.input.toString(k1.start, k1.stop):null, v1 != null?this.input.toString(v1.start, v1.stop):null);

               label342:
               while(true) {
                  int alt18 = 2;
                  int LA18_0 = this.input.LA(1);
                  if(LA18_0 == 223) {
                     alt18 = 1;
                  }

                  switch(alt18) {
                  case 1:
                     this.match(this.input, 223, FOLLOW_223_in_optionMap_DseSearchClause1230);
                     this.pushFollow(FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1234);
                     kn = this.optionName_DseSearchClause();
                     --this.state._fsp;
                     this.match(this.input, 228, FOLLOW_228_in_optionMap_DseSearchClause1236);
                     this.pushFollow(FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1240);
                     vn = this.optionValue_DseSearchClause();
                     --this.state._fsp;
                     options.put(kn != null?this.input.toString(kn.start, kn.stop):null, vn != null?this.input.toString(vn.start, vn.stop):null);
                     break;
                  default:
                     break label342;
                  }
               }
            }

            this.match(this.input, 241, FOLLOW_241_in_optionMap_DseSearchClause1256);
         } catch (RecognitionException var13) {
            this.reportError(var13);
            this.recover(this.input, var13);
         }

         return options;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.optionName_DseSearchClause_return optionName_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.optionName_DseSearchClause_return retval = new Cql_DseSearchParser.optionName_DseSearchClause_return();
      retval.start = this.input.LT(1);
      Token t = null;
      String k = null;

      try {
         try {
            int alt29 = true;
            int LA29_0 = this.input.LA(1);
            byte alt29;
            if(LA29_0 == 23) {
               alt29 = 1;
            } else {
               if((LA29_0 < 28 || LA29_0 > 29) && LA29_0 != 33 && LA29_0 != 35 && (LA29_0 < 37 || LA29_0 > 38) && (LA29_0 < 42 || LA29_0 > 44) && (LA29_0 < 46 || LA29_0 > 50) && (LA29_0 < 52 || LA29_0 > 58) && (LA29_0 < 60 || LA29_0 > 63) && (LA29_0 < 67 || LA29_0 > 69) && LA29_0 != 71 && (LA29_0 < 74 || LA29_0 > 78) && LA29_0 != 81 && (LA29_0 < 83 || LA29_0 > 84) && LA29_0 != 86 && (LA29_0 < 90 || LA29_0 > 93) && (LA29_0 < 95 || LA29_0 > 96) && (LA29_0 < 99 || LA29_0 > 102) && (LA29_0 < 104 || LA29_0 > 107) && (LA29_0 < 109 || LA29_0 > 111) && LA29_0 != 115 && LA29_0 != 117 && LA29_0 != 120 && LA29_0 != 122 && LA29_0 != 125 && LA29_0 != 128 && (LA29_0 < 131 || LA29_0 > 135) && (LA29_0 < 139 || LA29_0 > 142) && (LA29_0 < 145 || LA29_0 > 147) && (LA29_0 < 149 || LA29_0 > 155) && (LA29_0 < 158 || LA29_0 > 169) && LA29_0 != 172 && (LA29_0 < 174 || LA29_0 > 176) && (LA29_0 < 178 || LA29_0 > 179) && (LA29_0 < 182 || LA29_0 > 183) && (LA29_0 < 185 || LA29_0 > 188) && (LA29_0 < 192 || LA29_0 > 193)) {
                  NoViableAltException nvae = new NoViableAltException("", 29, 0, this.input);
                  throw nvae;
               }

               alt29 = 2;
            }

            switch(alt29) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_optionName_DseSearchClause1866);
               retval.id = t != null?t.getText():null;
               break;
            case 2:
               this.pushFollow(FOLLOW_unreserved_keyword_in_optionName_DseSearchClause1891);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               retval.id = k;
            }

            retval.stop = this.input.LT(-1);
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return retval;
      } finally {
         ;
      }
   }

   public final Cql_DseSearchParser.optionValue_DseSearchClause_return optionValue_DseSearchClause() throws RecognitionException {
      Cql_DseSearchParser.optionValue_DseSearchClause_return retval = new Cql_DseSearchParser.optionValue_DseSearchClause_return();
      retval.start = this.input.LT(1);

      try {
         try {
            if(this.input.LA(1) != 6 && this.input.LA(1) != 17 && this.input.LA(1) != 24 && this.input.LA(1) != 207) {
               MismatchedSetException mse = new MismatchedSetException((BitSet)null, this.input);
               throw mse;
            }

            this.input.consume();
            this.state.errorRecovery = false;
            retval.stop = this.input.LT(-1);
         } catch (RecognitionException var6) {
            this.reportError(var6);
            this.recover(this.input, var6);
         }

         return retval;
      } finally {
         ;
      }
   }

   public final List<String> profileList_DseSearchClause() throws RecognitionException {
      List<String> profiles = null;
      String p = null;
      String pn = null;

      try {
         this.match(this.input, 139, FOLLOW_K_PROFILES_in_profileList_DseSearchClause1141);
         this.pushFollow(FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1151);
         p = this.profileName_DseSearchClause();
         --this.state._fsp;
         profiles = new ArrayList();
         profiles.add(p);

         while(true) {
            int alt17 = 2;
            int LA17_0 = this.input.LA(1);
            if(LA17_0 == 223) {
               alt17 = 1;
            }

            switch(alt17) {
            case 1:
               this.match(this.input, 223, FOLLOW_223_in_profileList_DseSearchClause1163);
               this.pushFollow(FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1167);
               pn = this.profileName_DseSearchClause();
               --this.state._fsp;
               profiles.add(pn);
               break;
            default:
               return profiles;
            }
         }
      } catch (RecognitionException var9) {
         this.reportError(var9);
         this.recover(this.input, var9);
         return profiles;
      } finally {
         ;
      }
   }

   public final String profileName_DseSearchClause() throws RecognitionException {
      String id = null;
      Token t = null;
      String k = null;

      try {
         try {
            int alt28 = true;
            byte alt28;
            switch(this.input.LA(1)) {
            case 23:
               alt28 = 1;
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
            case 202:
            default:
               NoViableAltException nvae = new NoViableAltException("", 28, 0, this.input);
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
               alt28 = 3;
               break;
            case 203:
               alt28 = 2;
            }

            switch(alt28) {
            case 1:
               t = (Token)this.match(this.input, 23, FOLLOW_IDENT_in_profileName_DseSearchClause1797);
               id = (t != null?t.getText():null).toLowerCase();
               break;
            case 2:
               t = (Token)this.match(this.input, 203, FOLLOW_QUOTED_NAME_in_profileName_DseSearchClause1822);
               id = t != null?t.getText():null;
               break;
            case 3:
               this.pushFollow(FOLLOW_unreserved_keyword_in_profileName_DseSearchClause1841);
               k = this.gCql.unreserved_keyword();
               --this.state._fsp;
               id = k.toLowerCase();
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

   public final ParsedStatement rebuildSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      CFName cf = null;
      Map options = null;

      try {
         try {
            this.match(this.input, 140, FOLLOW_K_REBUILD_in_rebuildSearchIndexStatement855);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_rebuildSearchIndexStatement857);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_rebuildSearchIndexStatement859);
            this.match(this.input, 127, FOLLOW_K_ON_in_rebuildSearchIndexStatement861);
            this.pushFollow(FOLLOW_columnFamilyName_in_rebuildSearchIndexStatement867);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            int alt11 = 2;
            int LA11_0 = this.input.LA(1);
            if(LA11_0 == 191) {
               alt11 = 1;
            }

            switch(alt11) {
            case 1:
               this.match(this.input, 191, FOLLOW_K_WITH_in_rebuildSearchIndexStatement876);
               this.match(this.input, 128, FOLLOW_K_OPTIONS_in_rebuildSearchIndexStatement878);
               this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_rebuildSearchIndexStatement882);
               options = this.optionMap_DseSearchClause();
               --this.state._fsp;
            default:
               stmt = searchStatementFactory.rebuildSearchIndexStatement(cf, options);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public void recover(IntStream input, RecognitionException re) {
   }

   protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow) throws RecognitionException {
      throw new MismatchedTokenException(ttype, input);
   }

   public final ParsedStatement reloadSearchIndexStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      CFName cf = null;
      Map options = null;

      try {
         try {
            this.match(this.input, 141, FOLLOW_K_RELOAD_in_reloadSearchIndexStatement793);
            this.match(this.input, 155, FOLLOW_K_SEARCH_in_reloadSearchIndexStatement795);
            this.match(this.input, 89, FOLLOW_K_INDEX_in_reloadSearchIndexStatement797);
            this.match(this.input, 127, FOLLOW_K_ON_in_reloadSearchIndexStatement799);
            this.pushFollow(FOLLOW_columnFamilyName_in_reloadSearchIndexStatement805);
            cf = this.gCql.columnFamilyName();
            --this.state._fsp;
            int alt10 = 2;
            int LA10_0 = this.input.LA(1);
            if(LA10_0 == 191) {
               alt10 = 1;
            }

            switch(alt10) {
            case 1:
               this.match(this.input, 191, FOLLOW_K_WITH_in_reloadSearchIndexStatement814);
               this.match(this.input, 128, FOLLOW_K_OPTIONS_in_reloadSearchIndexStatement816);
               this.pushFollow(FOLLOW_optionMap_DseSearchClause_in_reloadSearchIndexStatement820);
               options = this.optionMap_DseSearchClause();
               --this.state._fsp;
            default:
               stmt = searchStatementFactory.reloadSearchIndexStatement(cf, options);
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return stmt;
      } finally {
         ;
      }
   }

   public final IResource searchIndexResource() throws RecognitionException {
      IResource res = null;
      String ks = null;
      CFName cf = null;

      try {
         try {
            int alt30 = true;
            int LA30_0 = this.input.LA(1);
            byte alt30;
            if(LA30_0 == 29) {
               alt30 = 1;
            } else {
               if(LA30_0 != 155) {
                  NoViableAltException nvae = new NoViableAltException("", 30, 0, this.input);
                  throw nvae;
               }

               int LA30_2 = this.input.LA(2);
               if(LA30_2 == 103) {
                  alt30 = 2;
               } else {
                  if(LA30_2 != 89) {
                     int nvaeMark = this.input.mark();

                     try {
                        this.input.consume();
                        NoViableAltException nvae = new NoViableAltException("", 30, 2, this.input);
                        throw nvae;
                     } finally {
                        this.input.rewind(nvaeMark);
                     }
                  }

                  alt30 = 3;
               }
            }

            switch(alt30) {
            case 1:
               this.match(this.input, 29, FOLLOW_K_ALL_in_searchIndexResource1955);
               this.match(this.input, 155, FOLLOW_K_SEARCH_in_searchIndexResource1957);
               this.match(this.input, 90, FOLLOW_K_INDICES_in_searchIndexResource1959);
               res = this.dseResourceFactory.fromName(this.dseResourceFactory.makeResourceName(new String[]{this.getSearchResourceRootName()}));
               break;
            case 2:
               this.match(this.input, 155, FOLLOW_K_SEARCH_in_searchIndexResource1969);
               this.match(this.input, 103, FOLLOW_K_KEYSPACE_in_searchIndexResource1971);
               this.pushFollow(FOLLOW_keyspaceName_in_searchIndexResource1975);
               ks = this.gCql.keyspaceName();
               --this.state._fsp;
               res = this.dseResourceFactory.fromName(this.dseResourceFactory.makeResourceName(new String[]{this.getSearchResourceRootName(), ks}));
               break;
            case 3:
               this.match(this.input, 155, FOLLOW_K_SEARCH_in_searchIndexResource1985);
               this.match(this.input, 89, FOLLOW_K_INDEX_in_searchIndexResource1987);
               this.pushFollow(FOLLOW_columnFamilyName_in_searchIndexResource1991);
               cf = this.gCql.columnFamilyName();
               --this.state._fsp;
               res = this.dseResourceFactory.fromName(this.dseResourceFactory.makeResourceName(new String[]{this.getSearchResourceRootName(), cf.getKeyspace(), cf.getColumnFamily()}));
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

   static {
      int numStates = DFA8_transitionS.length;
      DFA8_transition = new short[numStates][];

      for(int i = 0; i < numStates; ++i) {
         DFA8_transition[i] = DFA.unpackEncodedString(DFA8_transitionS[i]);
      }

      FOLLOW_createSearchIndexStatement_in_dseSearchStatement31 = new BitSet(new long[]{2L});
      FOLLOW_dropSearchIndexStatement_in_dseSearchStatement46 = new BitSet(new long[]{2L});
      FOLLOW_commitSearchIndexStatement_in_dseSearchStatement63 = new BitSet(new long[]{2L});
      FOLLOW_reloadSearchIndexStatement_in_dseSearchStatement78 = new BitSet(new long[]{2L});
      FOLLOW_alterSearchIndexStatement_in_dseSearchStatement93 = new BitSet(new long[]{2L});
      FOLLOW_rebuildSearchIndexStatement_in_dseSearchStatement109 = new BitSet(new long[]{2L});
      FOLLOW_searchIndexResource_in_dseSearchResource136 = new BitSet(new long[]{2L});
      FOLLOW_K_CREATE_in_createSearchIndexStatement170 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_createSearchIndexStatement172 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_createSearchIndexStatement174 = new BitSet(new long[]{0L, -9223372036846387200L});
      FOLLOW_K_IF_in_createSearchIndexStatement177 = new BitSet(new long[]{0L, 576460752303423488L});
      FOLLOW_K_NOT_in_createSearchIndexStatement179 = new BitSet(new long[]{0L, 1024L});
      FOLLOW_K_EXISTS_in_createSearchIndexStatement181 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_createSearchIndexStatement194 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_createSearchIndexStatement200 = new BitSet(new long[]{2L, 0L, -9223372036854775808L});
      FOLLOW_K_WITH_in_createSearchIndexStatement209 = new BitSet(new long[]{40532396646334464L, 0L, 2049L});
      FOLLOW_indexOptions_DseSearchClause_in_createSearchIndexStatement213 = new BitSet(new long[]{2L});
      FOLLOW_K_ALTER_in_alterSearchIndexStatement255 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_alterSearchIndexStatement257 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_alterSearchIndexStatement259 = new BitSet(new long[]{36028797018963968L, 0L, 16777216L});
      FOLLOW_K_SCHEMA_in_alterSearchIndexStatement262 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_CONFIG_in_alterSearchIndexStatement266 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_alterSearchIndexStatement277 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_alterSearchIndexStatement283 = new BitSet(new long[]{134217728L, 64L, 536870912L});
      FOLLOW_K_ADD_in_alterSearchIndexStatement315 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement321 = new BitSet(new long[]{2L, 0L, -9223372036854775808L});
      FOLLOW_K_WITH_in_alterSearchIndexStatement324 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
      FOLLOW_STRING_LITERAL_in_alterSearchIndexStatement330 = new BitSet(new long[]{2L});
      FOLLOW_K_ADD_in_alterSearchIndexStatement390 = new BitSet(new long[]{0L, 2048L});
      FOLLOW_K_FIELD_in_alterSearchIndexStatement392 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement398 = new BitSet(new long[]{2L});
      FOLLOW_K_SET_in_alterSearchIndexStatement456 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement462 = new BitSet(new long[]{0L, 0L, 0L, 9895604649984L});
      FOLLOW_235_in_alterSearchIndexStatement465 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement471 = new BitSet(new long[]{0L, 0L, 0L, 1099511627776L});
      FOLLOW_232_in_alterSearchIndexStatement475 = new BitSet(new long[]{-578751678438571968L, 2677653284601887928L, 2219663287022156025L, 34819L});
      FOLLOW_elementValue_DseSearchClause_in_alterSearchIndexStatement481 = new BitSet(new long[]{2L});
      FOLLOW_K_DROP_in_alterSearchIndexStatement540 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementPath_DseSearchClause_in_alterSearchIndexStatement546 = new BitSet(new long[]{2L, 0L, 0L, 8796093022208L});
      FOLLOW_235_in_alterSearchIndexStatement549 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement555 = new BitSet(new long[]{2L});
      FOLLOW_K_DROP_in_alterSearchIndexStatement615 = new BitSet(new long[]{0L, 2048L});
      FOLLOW_K_FIELD_in_alterSearchIndexStatement617 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_alterSearchIndexStatement623 = new BitSet(new long[]{2L});
      FOLLOW_K_DROP_in_dropSearchIndexStatement678 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_dropSearchIndexStatement680 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_dropSearchIndexStatement682 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_dropSearchIndexStatement690 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_dropSearchIndexStatement696 = new BitSet(new long[]{2L, 0L, -9223372036854775808L});
      FOLLOW_K_WITH_in_dropSearchIndexStatement705 = new BitSet(new long[]{0L, 0L, 1L});
      FOLLOW_K_OPTIONS_in_dropSearchIndexStatement707 = new BitSet(new long[]{0L, 0L, 0L, 281474976710656L});
      FOLLOW_optionMap_DseSearchClause_in_dropSearchIndexStatement711 = new BitSet(new long[]{2L});
      FOLLOW_K_COMMIT_in_commitSearchIndexStatement744 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_commitSearchIndexStatement746 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_commitSearchIndexStatement748 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_commitSearchIndexStatement756 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_commitSearchIndexStatement762 = new BitSet(new long[]{2L});
      FOLLOW_K_RELOAD_in_reloadSearchIndexStatement793 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_reloadSearchIndexStatement795 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_reloadSearchIndexStatement797 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_reloadSearchIndexStatement799 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_reloadSearchIndexStatement805 = new BitSet(new long[]{2L, 0L, -9223372036854775808L});
      FOLLOW_K_WITH_in_reloadSearchIndexStatement814 = new BitSet(new long[]{0L, 0L, 1L});
      FOLLOW_K_OPTIONS_in_reloadSearchIndexStatement816 = new BitSet(new long[]{0L, 0L, 0L, 281474976710656L});
      FOLLOW_optionMap_DseSearchClause_in_reloadSearchIndexStatement820 = new BitSet(new long[]{2L});
      FOLLOW_K_REBUILD_in_rebuildSearchIndexStatement855 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_rebuildSearchIndexStatement857 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_rebuildSearchIndexStatement859 = new BitSet(new long[]{0L, -9223372036854775808L});
      FOLLOW_K_ON_in_rebuildSearchIndexStatement861 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_rebuildSearchIndexStatement867 = new BitSet(new long[]{2L, 0L, -9223372036854775808L});
      FOLLOW_K_WITH_in_rebuildSearchIndexStatement876 = new BitSet(new long[]{0L, 0L, 1L});
      FOLLOW_K_OPTIONS_in_rebuildSearchIndexStatement878 = new BitSet(new long[]{0L, 0L, 0L, 281474976710656L});
      FOLLOW_optionMap_DseSearchClause_in_rebuildSearchIndexStatement882 = new BitSet(new long[]{2L});
      FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause915 = new BitSet(new long[]{4294967298L});
      FOLLOW_K_AND_in_indexOptions_DseSearchClause926 = new BitSet(new long[]{40532396646334464L, 0L, 2049L});
      FOLLOW_indexOption_DseSearchClause_in_indexOptions_DseSearchClause930 = new BitSet(new long[]{4294967298L});
      FOLLOW_columnList_DseSearchClause_in_indexOption_DseSearchClause960 = new BitSet(new long[]{2L});
      FOLLOW_profileList_DseSearchClause_in_indexOption_DseSearchClause972 = new BitSet(new long[]{2L});
      FOLLOW_K_CONFIG_in_indexOption_DseSearchClause982 = new BitSet(new long[]{0L, 0L, 0L, 281474976710656L});
      FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause986 = new BitSet(new long[]{2L});
      FOLLOW_K_OPTIONS_in_indexOption_DseSearchClause996 = new BitSet(new long[]{0L, 0L, 0L, 281474976710656L});
      FOLLOW_optionMap_DseSearchClause_in_indexOption_DseSearchClause1000 = new BitSet(new long[]{2L});
      FOLLOW_K_COLUMNS_in_columnList_DseSearchClause1023 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 35184372090883L});
      FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1033 = new BitSet(new long[]{2L, 0L, 0L, 2147483648L});
      FOLLOW_223_in_columnList_DseSearchClause1045 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 35184372090883L});
      FOLLOW_column_DseSearchClause_in_columnList_DseSearchClause1049 = new BitSet(new long[]{2L, 0L, 0L, 2147483648L});
      FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1085 = new BitSet(new long[]{2L, 0L, 0L, 281492156579840L});
      FOLLOW_226_in_column_DseSearchClause1091 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 35184372090883L});
      FOLLOW_columnName_DseSearchClause_in_column_DseSearchClause1095 = new BitSet(new long[]{2L, 0L, 0L, 281492156579840L});
      FOLLOW_optionMap_DseSearchClause_in_column_DseSearchClause1110 = new BitSet(new long[]{2L});
      FOLLOW_K_PROFILES_in_profileList_DseSearchClause1141 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1151 = new BitSet(new long[]{2L, 0L, 0L, 2147483648L});
      FOLLOW_223_in_profileList_DseSearchClause1163 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_profileName_DseSearchClause_in_profileList_DseSearchClause1167 = new BitSet(new long[]{2L, 0L, 0L, 2147483648L});
      FOLLOW_240_in_optionMap_DseSearchClause1192 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 562949953421315L});
      FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1208 = new BitSet(new long[]{0L, 0L, 0L, 68719476736L});
      FOLLOW_228_in_optionMap_DseSearchClause1210 = new BitSet(new long[]{16908352L, 0L, 0L, 32768L});
      FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1214 = new BitSet(new long[]{0L, 0L, 0L, 562952100904960L});
      FOLLOW_223_in_optionMap_DseSearchClause1230 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3L});
      FOLLOW_optionName_DseSearchClause_in_optionMap_DseSearchClause1234 = new BitSet(new long[]{0L, 0L, 0L, 68719476736L});
      FOLLOW_228_in_optionMap_DseSearchClause1236 = new BitSet(new long[]{16908352L, 0L, 0L, 32768L});
      FOLLOW_optionValue_DseSearchClause_in_optionMap_DseSearchClause1240 = new BitSet(new long[]{0L, 0L, 0L, 562952100904960L});
      FOLLOW_241_in_optionMap_DseSearchClause1256 = new BitSet(new long[]{2L});
      FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1288 = new BitSet(new long[]{2L, 0L, 0L, 17609365913600L});
      FOLLOW_236_in_elementPath_DseSearchClause1300 = new BitSet(new long[]{0L, 0L, 0L, 8796093022208L});
      FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1312 = new BitSet(new long[]{0L, 0L, 0L, 70370891661312L});
      FOLLOW_223_in_elementPath_DseSearchClause1326 = new BitSet(new long[]{0L, 0L, 0L, 8796093022208L});
      FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1330 = new BitSet(new long[]{0L, 0L, 0L, 70370891661312L});
      FOLLOW_238_in_elementPath_DseSearchClause1345 = new BitSet(new long[]{2L, 0L, 0L, 17179869184L});
      FOLLOW_226_in_elementPath_DseSearchClause1364 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_elementPath_DseSearchClause1376 = new BitSet(new long[]{2L, 0L, 0L, 17609365913600L});
      FOLLOW_236_in_elementPath_DseSearchClause1390 = new BitSet(new long[]{0L, 0L, 0L, 8796093022208L});
      FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1404 = new BitSet(new long[]{0L, 0L, 0L, 70370891661312L});
      FOLLOW_223_in_elementPath_DseSearchClause1420 = new BitSet(new long[]{0L, 0L, 0L, 8796093022208L});
      FOLLOW_attribute_DseSearchClause_in_elementPath_DseSearchClause1424 = new BitSet(new long[]{0L, 0L, 0L, 70370891661312L});
      FOLLOW_238_in_elementPath_DseSearchClause1441 = new BitSet(new long[]{2L, 0L, 0L, 17179869184L});
      FOLLOW_235_in_attribute_DseSearchClause1490 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 2051L});
      FOLLOW_elementName_DseSearchClause_in_attribute_DseSearchClause1494 = new BitSet(new long[]{0L, 0L, 0L, 1099511627776L});
      FOLLOW_232_in_attribute_DseSearchClause1496 = new BitSet(new long[]{0L, 0L, 0L, 32768L});
      FOLLOW_STRING_LITERAL_in_attribute_DseSearchClause1500 = new BitSet(new long[]{2L});
      FOLLOW_IDENT_in_elementName_DseSearchClause1525 = new BitSet(new long[]{2L});
      FOLLOW_QUOTED_NAME_in_elementName_DseSearchClause1550 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_keyword_in_elementName_DseSearchClause1569 = new BitSet(new long[]{2L});
      FOLLOW_IDENT_in_columnName_DseSearchClause1594 = new BitSet(new long[]{2L});
      FOLLOW_QUOTED_NAME_in_columnName_DseSearchClause1619 = new BitSet(new long[]{2L});
      FOLLOW_237_in_columnName_DseSearchClause1638 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_keyword_in_columnName_DseSearchClause1664 = new BitSet(new long[]{2L});
      FOLLOW_IDENT_in_elementValue_DseSearchClause1689 = new BitSet(new long[]{2L});
      FOLLOW_QUOTED_NAME_in_elementValue_DseSearchClause1723 = new BitSet(new long[]{2L});
      FOLLOW_optionValue_DseSearchClause_in_elementValue_DseSearchClause1751 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_keyword_in_elementValue_DseSearchClause1763 = new BitSet(new long[]{2L});
      FOLLOW_IDENT_in_profileName_DseSearchClause1797 = new BitSet(new long[]{2L});
      FOLLOW_QUOTED_NAME_in_profileName_DseSearchClause1822 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_keyword_in_profileName_DseSearchClause1841 = new BitSet(new long[]{2L});
      FOLLOW_IDENT_in_optionName_DseSearchClause1866 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_keyword_in_optionName_DseSearchClause1891 = new BitSet(new long[]{2L});
      FOLLOW_K_ALL_in_searchIndexResource1955 = new BitSet(new long[]{0L, 0L, 134217728L});
      FOLLOW_K_SEARCH_in_searchIndexResource1957 = new BitSet(new long[]{0L, 67108864L});
      FOLLOW_K_INDICES_in_searchIndexResource1959 = new BitSet(new long[]{2L});
      FOLLOW_K_SEARCH_in_searchIndexResource1969 = new BitSet(new long[]{0L, 549755813888L});
      FOLLOW_K_KEYSPACE_in_searchIndexResource1971 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_keyspaceName_in_searchIndexResource1975 = new BitSet(new long[]{2L});
      FOLLOW_K_SEARCH_in_searchIndexResource1985 = new BitSet(new long[]{0L, 33554432L});
      FOLLOW_K_INDEX_in_searchIndexResource1987 = new BitSet(new long[]{-578751678455480320L, 2677653284601887928L, 2219663287022156025L, 3075L});
      FOLLOW_columnFamilyName_in_searchIndexResource1991 = new BitSet(new long[]{2L});
   }

   protected class DFA8 extends DFA {
      public DFA8(BaseRecognizer recognizer) {
         this.recognizer = recognizer;
         this.decisionNumber = 8;
         this.eot = Cql_DseSearchParser.DFA8_eot;
         this.eof = Cql_DseSearchParser.DFA8_eof;
         this.min = Cql_DseSearchParser.DFA8_min;
         this.max = Cql_DseSearchParser.DFA8_max;
         this.accept = Cql_DseSearchParser.DFA8_accept;
         this.special = Cql_DseSearchParser.DFA8_special;
         this.transition = Cql_DseSearchParser.DFA8_transition;
      }

      public String getDescription() {
         return "157:7: ( ( K_ADD path= elementPath_DseSearchClause ( K_WITH json= STRING_LITERAL )? ) | ( K_ADD K_FIELD field= elementName_DseSearchClause ) | ( K_SET path= elementPath_DseSearchClause ( '@' attr= elementName_DseSearchClause )? '=' elementValue= elementValue_DseSearchClause ) | ( K_DROP path= elementPath_DseSearchClause ( '@' attr= elementName_DseSearchClause )? ) | ( K_DROP K_FIELD field= elementName_DseSearchClause ) )";
      }
   }

   public static class optionValue_DseSearchClause_return extends ParserRuleReturnScope {
      public optionValue_DseSearchClause_return() {
      }
   }

   public static class optionName_DseSearchClause_return extends ParserRuleReturnScope {
      public String id;

      public optionName_DseSearchClause_return() {
      }
   }

   public static class attribute_DseSearchClause_return extends ParserRuleReturnScope {
      public String key;
      public String value;

      public attribute_DseSearchClause_return() {
      }
   }

   public static class column_DseSearchClause_return extends ParserRuleReturnScope {
      public String name;
      public Map<String, String> options;

      public column_DseSearchClause_return() {
      }
   }

   public static class columnList_DseSearchClause_return extends ParserRuleReturnScope {
      public List<String> columns;
      public Map<String, Map<String, String>> columnOptions;

      public columnList_DseSearchClause_return() {
      }
   }

   public static class indexOption_DseSearchClause_return extends ParserRuleReturnScope {
      public List<String> columnList;
      public Map<String, Map<String, String>> columnOptions;
      public List<String> profileList;
      public Map<String, String> configOptions;
      public Map<String, String> requestOptions;

      public indexOption_DseSearchClause_return() {
      }
   }

   public static class indexOptions_DseSearchClause_return extends ParserRuleReturnScope {
      public List<String> columnList;
      public Map<String, Map<String, String>> columnOptions;
      public List<String> profileList;
      public Map<String, String> configOptions;
      public Map<String, String> requestOptions;

      public indexOptions_DseSearchClause_return() {
      }
   }

   public interface SearchStatementFactory {
      String ERROR_MESSAGE = "Search statements are not supported on this node";

      default ParsedStatement createSearchIndexStatement(CFName cf, boolean ifNotExists, List<String> columns, Map<String, Map<String, String>> columnOptions, List<String> profiles, Map<String, String> configOptions, Map<String, String> requestOptions) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }

      default ParsedStatement alterSearchIndexStatement(CFName cf, boolean config, String verb, XmlPath path, String attribute, String value, String json) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }

      default ParsedStatement dropSearchIndexStatement(CFName cf, Map<String, String> options) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }

      default ParsedStatement commitSearchIndexStatement(CFName cf) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }

      default ParsedStatement reloadSearchIndexStatement(CFName cf, Map<String, String> options) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }

      default ParsedStatement rebuildSearchIndexStatement(CFName cf, Map<String, String> options) throws InvalidRequestException {
         throw new InvalidRequestException("Search statements are not supported on this node");
      }
   }
}
