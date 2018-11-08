package org.apache.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.AuthenticationScheme;
import com.datastax.bdp.cassandra.auth.AuthenticationSchemeResource;
import com.datastax.bdp.cassandra.auth.DseRowResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerSubmissionResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerWorkPoolResource;
import com.datastax.bdp.cassandra.auth.RpcResource;
import com.datastax.bdp.cassandra.cql3.RestrictRowsStatement;
import com.datastax.bdp.cassandra.cql3.UnRestrictRowsStatement;
import com.datastax.bdp.xml.XmlPath;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.DFA;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.AbstractMarker.INRaw;
import org.apache.cassandra.cql3.Constants.Literal;
import org.apache.cassandra.cql3.Operation.RawDeletion;
import org.apache.cassandra.cql3.Operation.RawUpdate;
import org.apache.cassandra.cql3.WhereClause.Builder;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.AlterViewStatement;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.cql3.statements.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.CreateViewStatement;
import org.apache.cassandra.cql3.statements.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.DropIndexStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.DropTableStatement;
import org.apache.cassandra.cql3.statements.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.DropTypeStatement;
import org.apache.cassandra.cql3.statements.DropViewStatement;
import org.apache.cassandra.cql3.statements.GrantPermissionsStatement;
import org.apache.cassandra.cql3.statements.GrantRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Parsed;
import org.apache.cassandra.cql3.statements.CreateTableStatement.RawStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsertJson;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;
import org.apache.cassandra.schema.ColumnMetadata.Raw;
import org.apache.cassandra.utils.Pair;

public class CqlParser extends Parser {
   public static final String[] tokenNames = new String[]{"<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "B", "BOOLEAN", "C", "COMMENT", "D", "DIGIT", "DURATION", "DURATION_UNIT", "E", "EMPTY_QUOTED_NAME", "EXPONENT", "F", "FLOAT", "G", "H", "HEX", "HEXNUMBER", "I", "IDENT", "INTEGER", "J", "K", "K_ADD", "K_AGGREGATE", "K_ALL", "K_ALLOW", "K_ALTER", "K_AND", "K_ANY", "K_APPLY", "K_AS", "K_ASC", "K_ASCII", "K_AUTHENTICATION", "K_AUTHORIZE", "K_BATCH", "K_BEGIN", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_BY", "K_CALL", "K_CALLED", "K_CALLS", "K_CAST", "K_CLUSTERING", "K_COLUMNFAMILY", "K_COLUMNS", "K_COMMIT", "K_COMPACT", "K_CONFIG", "K_CONTAINS", "K_COUNT", "K_COUNTER", "K_CREATE", "K_CUSTOM", "K_DATE", "K_DECIMAL", "K_DEFAULT", "K_DELETE", "K_DESC", "K_DESCRIBE", "K_DETERMINISTIC", "K_DISTINCT", "K_DOUBLE", "K_DROP", "K_DURATION", "K_ENTRIES", "K_EXECUTE", "K_EXISTS", "K_FIELD", "K_FILTERING", "K_FINALFUNC", "K_FLOAT", "K_FOR", "K_FROM", "K_FROZEN", "K_FULL", "K_FUNCTION", "K_FUNCTIONS", "K_GRANT", "K_GROUP", "K_IF", "K_IN", "K_INDEX", "K_INDICES", "K_INET", "K_INITCOND", "K_INPUT", "K_INSERT", "K_INT", "K_INTERNAL", "K_INTO", "K_IS", "K_JSON", "K_KERBEROS", "K_KEY", "K_KEYS", "K_KEYSPACE", "K_KEYSPACES", "K_LANGUAGE", "K_LDAP", "K_LIKE", "K_LIMIT", "K_LIST", "K_LOGIN", "K_MAP", "K_MATERIALIZED", "K_MBEAN", "K_MBEANS", "K_METHOD", "K_MODIFY", "K_MONOTONIC", "K_NEGATIVE_INFINITY", "K_NEGATIVE_NAN", "K_NOLOGIN", "K_NORECURSIVE", "K_NOSUPERUSER", "K_NOT", "K_NULL", "K_OBJECT", "K_OF", "K_ON", "K_OPTIONS", "K_OR", "K_ORDER", "K_PARTITION", "K_PASSWORD", "K_PER", "K_PERMISSION", "K_PERMISSIONS", "K_POSITIVE_INFINITY", "K_POSITIVE_NAN", "K_PRIMARY", "K_PROFILES", "K_REBUILD", "K_RELOAD", "K_REMOTE", "K_RENAME", "K_REPLACE", "K_RESOURCE", "K_RESTRICT", "K_RETURNS", "K_REVOKE", "K_ROLE", "K_ROLES", "K_ROWS", "K_SCHEMA", "K_SCHEME", "K_SCHEMES", "K_SEARCH", "K_SELECT", "K_SET", "K_SFUNC", "K_SMALLINT", "K_STATIC", "K_STORAGE", "K_STYPE", "K_SUBMISSION", "K_SUPERUSER", "K_TEXT", "K_TIME", "K_TIMESTAMP", "K_TIMEUUID", "K_TINYINT", "K_TO", "K_TOKEN", "K_TRIGGER", "K_TRUNCATE", "K_TTL", "K_TUPLE", "K_TYPE", "K_UNLOGGED", "K_UNRESTRICT", "K_UNSET", "K_UPDATE", "K_USE", "K_USER", "K_USERS", "K_USING", "K_UUID", "K_VALUES", "K_VARCHAR", "K_VARINT", "K_VIEW", "K_WHERE", "K_WITH", "K_WORKPOOL", "K_WRITETIME", "L", "LETTER", "M", "MULTILINE_COMMENT", "N", "O", "P", "Q", "QMARK", "QUOTED_NAME", "R", "RANGE", "S", "STRING_LITERAL", "T", "U", "UUID", "V", "W", "WS", "X", "Y", "Z", "'!='", "'%'", "'('", "')'", "'+'", "'+='", "','", "'-'", "'-='", "'.'", "'/'", "':'", "';'", "'<'", "'<='", "'='", "'>'", "'>='", "'@'", "'['", "'\\*'", "']'", "'expr('", "'{'", "'}'"};
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
   public Cql_DseSearchParser gDseSearchParser;
   public Cql_DseCoreParser gDseCoreParser;
   public Cql_Parser gParser;
   protected CqlParser.DFA3 dfa3;
   protected CqlParser.DFA5 dfa5;
   static final String DFA3_eotS = "\n\uffff";
   static final String DFA3_eofS = "\n\uffff";
   static final String DFA3_minS = "\u0001\u001f\u0001\uffff\u0002\u001c\u00013\u0002\u0017\u0001\uffff\u0002\u007f";
   static final String DFA3_maxS = "\u0001µ\u0001\uffff\u0003¶\u0002Ï\u0001\uffff\u0002â";
   static final String DFA3_acceptS = "\u0001\uffff\u0001\u0001\u0005\uffff\u0001\u0002\u0002\uffff";
   static final String DFA3_specialS = "\n\uffff}>";
   static final String[] DFA3_transitionS = new String[]{"\u0001\u0004\t\uffff\u0001\u0001\u0004\uffff\u0001\u0007\u0006\uffff\u0001\u0007\u0005\uffff\u0001\u0002\u0004\uffff\u0001\u0001\u0005\uffff\u0001\u0003\u000e\uffff\u0001\u0001\b\uffff\u0001\u0001\u000e\uffff\u0001\u0001\u001e\uffff\u0002\u0007\u0004\uffff\u0001\u0005\u0001\uffff\u0001\u0001\u0007\uffff\u0001\u0001\u0010\uffff\u0001\u0001\u0004\uffff\u0001\u0006\u0001\uffff\u0002\u0001", "", "\u0001\u0001\u0016\uffff\u0001\u0001\b\uffff\u0001\u0001\u0016\uffff\u0001\u0001\u0005\uffff\u0001\u0001\r\uffff\u0001\u0001\b\uffff\u0001\u0001\u0010\uffff\u0001\u0001\u0013\uffff\u0001\u0001\u0005\uffff\u0001\u0007\u0010\uffff\u0001\u0001\u0003\uffff\u0001\u0001\u0005\uffff\u0001\u0001", "\u0001\u0001\u0016\uffff\u0001\u0001\u001f\uffff\u0001\u0001\u0005\uffff\u0001\u0001\r\uffff\u0001\u0001\b\uffff\u0001\u0001$\uffff\u0001\u0001\u0005\uffff\u0001\u0007\u0010\uffff\u0001\u0001\u0003\uffff\u0001\u0001\u0005\uffff\u0001\u0001", "\u0001\u00013\uffff\u0001\u0001\b\uffff\u0001\u0001$\uffff\u0001\u0001\u0005\uffff\u0001\u0007\u0014\uffff\u0001\u0001\u0005\uffff\u0001\u0001", "\u0001\u0001\u0004\uffff\u0002\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0003\u0001\u0002\uffff\u0003\u0001\u0001\uffff\u0005\u0001\u0001\uffff\f\u0001\u0002\uffff\u0006\u0001\u0001\uffff\u0006\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0002\u0001\u0001\uffff\u0001\u0001\u0003\uffff\u0004\u0001\u0001\uffff\u0002\u0001\u0002\uffff\u0004\u0001\u0001\uffff\u0004\u0001\u0001\uffff\u0003\u0001\u0003\uffff\u0003\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0002\uffff\u0001\u0001\u0002\uffff\u0001\u0001\u0002\uffff\u0005\u0001\u0003\uffff\u0004\u0001\u0002\uffff\u0003\u0001\u0001\uffff\u0002\u0001\u0001\b\u0005\u0001\u0001\uffff\f\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0003\u0001\u0001\uffff\u0002\u0001\u0002\uffff\u0002\u0001\u0001\uffff\u0004\u0001\u0003\uffff\u0002\u0001\t\uffff\u0001\u0001\u0003\uffff\u0001\u0001", "\u0001\u0001\u0004\uffff\u0002\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0001\uffff\u0003\u0001\u0002\uffff\u0003\u0001\u0001\uffff\u0005\u0001\u0001\uffff\f\u0001\u0002\uffff\u0006\u0001\u0001\uffff\u0006\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0002\u0001\u0001\uffff\u0001\u0001\u0003\uffff\u0004\u0001\u0001\uffff\u0002\u0001\u0002\uffff\u0004\u0001\u0001\uffff\u0004\u0001\u0001\uffff\u0003\u0001\u0003\uffff\u0003\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0001\u0002\uffff\u0001\u0001\u0002\uffff\u0001\u0001\u0002\uffff\u0005\u0001\u0003\uffff\u0004\u0001\u0002\uffff\u0003\u0001\u0001\uffff\u0002\u0001\u0001\t\u0005\u0001\u0001\uffff\f\u0001\u0002\uffff\u0001\u0001\u0001\uffff\u0003\u0001\u0001\uffff\u0002\u0001\u0002\uffff\u0002\u0001\u0001\uffff\u0004\u0001\u0003\uffff\u0002\u0001\t\uffff\u0001\u0001\u0003\uffff\u0001\u0001", "", "\u0001\u0007b\uffff\u0001\u0001", "\u0001\u0007b\uffff\u0001\u0001"};
   static final short[] DFA3_eot = DFA.unpackEncodedString("\n\uffff");
   static final short[] DFA3_eof = DFA.unpackEncodedString("\n\uffff");
   static final char[] DFA3_min = DFA.unpackEncodedStringToUnsignedChars("\u0001\u001f\u0001\uffff\u0002\u001c\u00013\u0002\u0017\u0001\uffff\u0002\u007f");
   static final char[] DFA3_max = DFA.unpackEncodedStringToUnsignedChars("\u0001µ\u0001\uffff\u0003¶\u0002Ï\u0001\uffff\u0002â");
   static final short[] DFA3_accept = DFA.unpackEncodedString("\u0001\uffff\u0001\u0001\u0005\uffff\u0001\u0002\u0002\uffff");
   static final short[] DFA3_special = DFA.unpackEncodedString("\n\uffff}>");
   static final short[][] DFA3_transition;
   static final String DFA5_eotS = "\u000b\uffff";
   static final String DFA5_eofS = "\u0001\uffff\u0001\u0002\u0001\uffff\u0001\u0002\u0001\uffff\u0005\u0002\u0001\uffff";
   static final String DFA5_minS = "\u0001\u0017\u0001&\u0001\uffff\u0001P\u0001\uffff\u0005P\u0001\uffff";
   static final String DFA5_maxS = "\u0001Ï\u0001å\u0001\uffff\u0001å\u0001\uffff\u0005å\u0001\uffff";
   static final String DFA5_acceptS = "\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0002\u0005\uffff\u0001\u0003";
   static final String DFA5_specialS = "\u000b\uffff}>";
   static final String[] DFA5_transitionS;
   static final short[] DFA5_eot;
   static final short[] DFA5_eof;
   static final char[] DFA5_min;
   static final char[] DFA5_max;
   static final short[] DFA5_accept;
   static final short[] DFA5_special;
   static final short[][] DFA5_transition;
   public static final BitSet FOLLOW_cqlStatement_in_query757;
   public static final BitSet FOLLOW_229_in_query760;
   public static final BitSet FOLLOW_EOF_in_query764;
   public static final BitSet FOLLOW_dseStatement_in_query776;
   public static final BitSet FOLLOW_229_in_query779;
   public static final BitSet FOLLOW_EOF_in_query783;
   public static final BitSet FOLLOW_dseCoreStatement_in_dseStatement816;
   public static final BitSet FOLLOW_dseSearchStatement_in_dseStatement844;
   public static final BitSet FOLLOW_cassandraResource_in_resource883;
   public static final BitSet FOLLOW_dseCoreResource_in_resource895;
   public static final BitSet FOLLOW_dseSearchResource_in_resource907;
   public static final BitSet FOLLOW_unreserved_function_keyword_in_unreserved_keyword937;
   public static final BitSet FOLLOW_dse_unreserved_keyword_in_unreserved_keyword953;
   public static final BitSet FOLLOW_set_in_unreserved_keyword965;
   public static final BitSet FOLLOW_set_in_dse_unreserved_keyword1016;

   public CqlParser(TokenStream input) {
      this(input, new RecognizerSharedState());
   }

   public CqlParser(TokenStream input, RecognizerSharedState state) {
      super(input, state);
      this.dfa3 = new CqlParser.DFA3(this);
      this.dfa5 = new CqlParser.DFA5(this);
      this.gDseSearchParser = new Cql_DseSearchParser(input, state, this);
      this.gDseCoreParser = new Cql_DseCoreParser(input, state, this);
      this.gParser = new Cql_Parser(input, state, this);
   }

   public void addErrorListener(ErrorListener listener) {
      this.gParser.addErrorListener(listener);
   }

   protected void addRecognitionError(String msg) {
      this.gParser.addRecognitionError(msg);
   }

   public String allowedFunctionName() throws RecognitionException {
      return this.gParser.allowedFunctionName();
   }

   public AlterKeyspaceStatement alterKeyspaceStatement() throws RecognitionException {
      return this.gParser.alterKeyspaceStatement();
   }

   public AlterViewStatement alterMaterializedViewStatement() throws RecognitionException {
      return this.gParser.alterMaterializedViewStatement();
   }

   public AlterRoleStatement alterRoleStatement() throws RecognitionException {
      return this.gParser.alterRoleStatement();
   }

   public ParsedStatement alterSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.alterSearchIndexStatement();
   }

   public AlterTableStatement alterTableStatement() throws RecognitionException {
      return this.gParser.alterTableStatement();
   }

   public AlterTypeStatement alterTypeStatement() throws RecognitionException {
      return this.gParser.alterTypeStatement();
   }

   public AlterRoleStatement alterUserStatement() throws RecognitionException {
      return this.gParser.alterUserStatement();
   }

   public Cql_DseSearchParser.attribute_DseSearchClause_return attribute_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.attribute_DseSearchClause();
   }

   public AuthenticationSchemeResource authenticationSchemeResource() throws RecognitionException {
      return this.gDseCoreParser.authenticationSchemeResource();
   }

   public String basic_unreserved_keyword() throws RecognitionException {
      return this.gParser.basic_unreserved_keyword();
   }

   public Parsed batchStatement() throws RecognitionException {
      return this.gParser.batchStatement();
   }

   public org.apache.cassandra.cql3.statements.ModificationStatement.Parsed batchStatementObjective() throws RecognitionException {
      return this.gParser.batchStatementObjective();
   }

   public IResource cassandraResource() throws RecognitionException {
      return this.gParser.cassandraResource();
   }

   public void cfName(CFName name) throws RecognitionException {
      this.gParser.cfName(name);
   }

   public void cfamColumns(RawStatement expr) throws RecognitionException {
      this.gParser.cfamColumns(expr);
   }

   public void cfamDefinition(RawStatement expr) throws RecognitionException {
      this.gParser.cfamDefinition(expr);
   }

   public void cfamOrdering(CFProperties props) throws RecognitionException {
      this.gParser.cfamOrdering(props);
   }

   public void cfamProperty(CFProperties props) throws RecognitionException {
      this.gParser.cfamProperty(props);
   }

   public boolean cfisStatic() throws RecognitionException {
      return this.gParser.cfisStatic();
   }

   public Raw cident() throws RecognitionException {
      return this.gParser.cident();
   }

   public void collectionColumnOperation(List<Pair<Raw, RawUpdate>> operations, Raw key, org.apache.cassandra.cql3.Term.Raw k) throws RecognitionException {
      this.gParser.collectionColumnOperation(operations, key, k);
   }

   public org.apache.cassandra.cql3.Term.Raw collectionLiteral() throws RecognitionException {
      return this.gParser.collectionLiteral();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw collectionSubSelection(org.apache.cassandra.cql3.selection.Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.collectionSubSelection(receiver);
   }

   public org.apache.cassandra.cql3.CQL3Type.Raw collection_type() throws RecognitionException {
      return this.gParser.collection_type();
   }

   public void columnCondition(List<Pair<Raw, org.apache.cassandra.cql3.conditions.ColumnCondition.Raw>> conditions) throws RecognitionException {
      this.gParser.columnCondition(conditions);
   }

   public CFName columnFamilyName() throws RecognitionException {
      return this.gParser.columnFamilyName();
   }

   public Cql_DseSearchParser.columnList_DseSearchClause_return columnList_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.columnList_DseSearchClause();
   }

   public String columnName_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.columnName_DseSearchClause();
   }

   public void columnOperation(List<Pair<Raw, RawUpdate>> operations) throws RecognitionException {
      this.gParser.columnOperation(operations);
   }

   public void columnOperationDifferentiator(List<Pair<Raw, RawUpdate>> operations, Raw key) throws RecognitionException {
      this.gParser.columnOperationDifferentiator(operations, key);
   }

   public Cql_DseSearchParser.column_DseSearchClause_return column_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.column_DseSearchClause();
   }

   public ParsedStatement commitSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.commitSearchIndexStatement();
   }

   public org.apache.cassandra.cql3.CQL3Type.Raw comparatorType() throws RecognitionException {
      return this.gParser.comparatorType();
   }

   public org.apache.cassandra.cql3.CQL3Type.Raw comparatorTypeWithMultiCellTuple() throws RecognitionException {
      return this.gParser.comparatorTypeWithMultiCellTuple();
   }

   public org.apache.cassandra.cql3.CQL3Type.Raw comparatorTypeWithoutTuples() throws RecognitionException {
      return this.gParser.comparatorTypeWithoutTuples();
   }

   public Literal constant() throws RecognitionException {
      return this.gParser.constant();
   }

   public Operator containsOperator() throws RecognitionException {
      return this.gParser.containsOperator();
   }

   public Cql_Parser.corePermissionName_return corePermissionName() throws RecognitionException {
      return this.gParser.corePermissionName();
   }

   public ParsedStatement cqlStatement() throws RecognitionException {
      return this.gParser.cqlStatement();
   }

   public CreateAggregateStatement createAggregateStatement() throws RecognitionException {
      return this.gParser.createAggregateStatement();
   }

   public CreateFunctionStatement createFunctionStatement() throws RecognitionException {
      return this.gParser.createFunctionStatement();
   }

   public CreateIndexStatement createIndexStatement() throws RecognitionException {
      return this.gParser.createIndexStatement();
   }

   public CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
      return this.gParser.createKeyspaceStatement();
   }

   public CreateViewStatement createMaterializedViewStatement() throws RecognitionException {
      return this.gParser.createMaterializedViewStatement();
   }

   public CreateRoleStatement createRoleStatement() throws RecognitionException {
      return this.gParser.createRoleStatement();
   }

   public ParsedStatement createSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.createSearchIndexStatement();
   }

   public RawStatement createTableStatement() throws RecognitionException {
      return this.gParser.createTableStatement();
   }

   public CreateTriggerStatement createTriggerStatement() throws RecognitionException {
      return this.gParser.createTriggerStatement();
   }

   public CreateTypeStatement createTypeStatement() throws RecognitionException {
      return this.gParser.createTypeStatement();
   }

   public CreateRoleStatement createUserStatement() throws RecognitionException {
      return this.gParser.createUserStatement();
   }

   public void customIndexExpression(Builder clause) throws RecognitionException {
      this.gParser.customIndexExpression(clause);
   }

   public DataResource dataResource() throws RecognitionException {
      return this.gParser.dataResource();
   }

   public RawDeletion deleteOp() throws RecognitionException {
      return this.gParser.deleteOp();
   }

   public List<RawDeletion> deleteSelection() throws RecognitionException {
      return this.gParser.deleteSelection();
   }

   public org.apache.cassandra.cql3.statements.DeleteStatement.Parsed deleteStatement() throws RecognitionException {
      return this.gParser.deleteStatement();
   }

   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
      this.gParser.displayRecognitionError(tokenNames, e);
   }

   public DropAggregateStatement dropAggregateStatement() throws RecognitionException {
      return this.gParser.dropAggregateStatement();
   }

   public DropFunctionStatement dropFunctionStatement() throws RecognitionException {
      return this.gParser.dropFunctionStatement();
   }

   public DropIndexStatement dropIndexStatement() throws RecognitionException {
      return this.gParser.dropIndexStatement();
   }

   public DropKeyspaceStatement dropKeyspaceStatement() throws RecognitionException {
      return this.gParser.dropKeyspaceStatement();
   }

   public DropViewStatement dropMaterializedViewStatement() throws RecognitionException {
      return this.gParser.dropMaterializedViewStatement();
   }

   public DropRoleStatement dropRoleStatement() throws RecognitionException {
      return this.gParser.dropRoleStatement();
   }

   public ParsedStatement dropSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.dropSearchIndexStatement();
   }

   public DropTableStatement dropTableStatement() throws RecognitionException {
      return this.gParser.dropTableStatement();
   }

   public DropTriggerStatement dropTriggerStatement() throws RecognitionException {
      return this.gParser.dropTriggerStatement();
   }

   public DropTypeStatement dropTypeStatement() throws RecognitionException {
      return this.gParser.dropTypeStatement();
   }

   public DropRoleStatement dropUserStatement() throws RecognitionException {
      return this.gParser.dropUserStatement();
   }

   public IResource dseCoreResource() throws RecognitionException {
      return this.gDseCoreParser.dseCoreResource();
   }

   public ParsedStatement dseCoreStatement() throws RecognitionException {
      return this.gDseCoreParser.dseCoreStatement();
   }

   public DseRowResource dseRowResource() throws RecognitionException {
      return this.gDseCoreParser.dseRowResource();
   }

   public IResource dseSearchResource() throws RecognitionException {
      return this.gDseSearchParser.dseSearchResource();
   }

   public ParsedStatement dseSearchStatement() throws RecognitionException {
      return this.gDseSearchParser.dseSearchStatement();
   }

   public final ParsedStatement dseStatement() throws RecognitionException {
      ParsedStatement stmt = null;
      ParsedStatement st = null;

      try {
         try {
            int alt4 = true;
            int LA4_0 = this.input.LA(1);
            byte alt4;
            if(LA4_0 != 46 && LA4_0 != 146 && LA4_0 != 178) {
               if(LA4_0 != 31 && LA4_0 != 53 && LA4_0 != 59 && LA4_0 != 70 && (LA4_0 < 140 || LA4_0 > 141)) {
                  NoViableAltException nvae = new NoViableAltException("", 4, 0, this.input);
                  throw nvae;
               }

               alt4 = 2;
            } else {
               alt4 = 1;
            }

            switch(alt4) {
            case 1:
               this.pushFollow(FOLLOW_dseCoreStatement_in_dseStatement816);
               st = this.dseCoreStatement();
               --this.state._fsp;
               stmt = st;
               break;
            case 2:
               this.pushFollow(FOLLOW_dseSearchStatement_in_dseStatement844);
               st = this.dseSearchStatement();
               --this.state._fsp;
               stmt = st;
            }

            if(stmt != null) {
               stmt.setBoundVariables(this.gParser.bindVariables);
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

   public final String dse_unreserved_keyword() throws RecognitionException {
      String str = null;
      Token k = null;

      try {
         try {
            k = this.input.LT(1);
            if(this.input.LA(1) != 33 && this.input.LA(1) != 38 && this.input.LA(1) != 46 && this.input.LA(1) != 48 && (this.input.LA(1) < 52 || this.input.LA(1) > 53) && this.input.LA(1) != 55 && this.input.LA(1) != 63 && this.input.LA(1) != 75 && this.input.LA(1) != 90 && this.input.LA(1) != 96 && this.input.LA(1) != 100 && this.input.LA(1) != 106 && this.input.LA(1) != 115 && this.input.LA(1) != 125 && (this.input.LA(1) < 139 || this.input.LA(1) > 142) && this.input.LA(1) != 146 && (this.input.LA(1) < 151 || this.input.LA(1) > 155) && this.input.LA(1) != 163 && (this.input.LA(1) < 178 || this.input.LA(1) > 179) && this.input.LA(1) != 192) {
               MismatchedSetException mse = new MismatchedSetException((BitSet)null, this.input);
               throw mse;
            }

            this.input.consume();
            this.state.errorRecovery = false;
            str = k != null?k.getText():null;
         } catch (RecognitionException var7) {
            this.reportError(var7);
            this.recover(this.input, var7);
         }

         return str;
      } finally {
         ;
      }
   }

   public String elementName_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.elementName_DseSearchClause();
   }

   public XmlPath elementPath_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.elementPath_DseSearchClause();
   }

   public String elementValue_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.elementValue_DseSearchClause();
   }

   public FieldIdentifier fident() throws RecognitionException {
      return this.gParser.fident();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw fieldSelectorModifier(org.apache.cassandra.cql3.selection.Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.fieldSelectorModifier(receiver);
   }

   public org.apache.cassandra.cql3.Maps.Literal fullMapLiteral() throws RecognitionException {
      return this.gParser.fullMapLiteral();
   }

   public org.apache.cassandra.cql3.Term.Raw function() throws RecognitionException {
      return this.gParser.function();
   }

   public List<org.apache.cassandra.cql3.Term.Raw> functionArgs() throws RecognitionException {
      return this.gParser.functionArgs();
   }

   public FunctionName functionName() throws RecognitionException {
      return this.gParser.functionName();
   }

   public FunctionResource functionResource() throws RecognitionException {
      return this.gParser.functionResource();
   }

   public Parser[] getDelegates() {
      return new Parser[]{this.gDseSearchParser, this.gDseCoreParser, this.gParser};
   }

   public String getGrammarFileName() {
      return "org/apache/cassandra/cql3/Cql.g";
   }

   public String[] getTokenNames() {
      return tokenNames;
   }

   public GrantPermissionsStatement grantPermissionsStatement() throws RecognitionException {
      return this.gParser.grantPermissionsStatement();
   }

   public GrantRoleStatement grantRoleStatement() throws RecognitionException {
      return this.gParser.grantRoleStatement();
   }

   public void groupByClause(List<org.apache.cassandra.cql3.selection.Selectable.Raw> groups) throws RecognitionException {
      this.gParser.groupByClause(groups);
   }

   public ColumnIdentifier ident() throws RecognitionException {
      return this.gParser.ident();
   }

   public void idxName(IndexName name) throws RecognitionException {
      this.gParser.idxName(name);
   }

   public INRaw inMarker() throws RecognitionException {
      return this.gParser.inMarker();
   }

   public org.apache.cassandra.cql3.Tuples.INRaw inMarkerForTuple() throws RecognitionException {
      return this.gParser.inMarkerForTuple();
   }

   public void indexIdent(List<org.apache.cassandra.cql3.statements.IndexTarget.Raw> targets) throws RecognitionException {
      this.gParser.indexIdent(targets);
   }

   public IndexName indexName() throws RecognitionException {
      return this.gParser.indexName();
   }

   public Cql_DseSearchParser.indexOption_DseSearchClause_return indexOption_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.indexOption_DseSearchClause();
   }

   public Cql_DseSearchParser.indexOptions_DseSearchClause_return indexOptions_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.indexOptions_DseSearchClause();
   }

   public org.apache.cassandra.cql3.statements.ModificationStatement.Parsed insertStatement() throws RecognitionException {
      return this.gParser.insertStatement();
   }

   public org.apache.cassandra.cql3.Term.Raw intValue() throws RecognitionException {
      return this.gParser.intValue();
   }

   public JMXResource jmxResource() throws RecognitionException {
      return this.gParser.jmxResource();
   }

   public ParsedInsertJson jsonInsertStatement(CFName cf) throws RecognitionException {
      return this.gParser.jsonInsertStatement(cf);
   }

   public org.apache.cassandra.cql3.Json.Raw jsonValue() throws RecognitionException {
      return this.gParser.jsonValue();
   }

   public String keyspaceName() throws RecognitionException {
      return this.gParser.keyspaceName();
   }

   public void ksName(KeyspaceElementName name) throws RecognitionException {
      this.gParser.ksName(name);
   }

   public org.apache.cassandra.cql3.Term.Raw listLiteral() throws RecognitionException {
      return this.gParser.listLiteral();
   }

   public ListPermissionsStatement listPermissionsStatement() throws RecognitionException {
      return this.gParser.listPermissionsStatement();
   }

   public ListRolesStatement listRolesStatement() throws RecognitionException {
      return this.gParser.listRolesStatement();
   }

   public ListRolesStatement listUsersStatement() throws RecognitionException {
      return this.gParser.listUsersStatement();
   }

   public org.apache.cassandra.cql3.Term.Raw mapLiteral(org.apache.cassandra.cql3.Term.Raw k) throws RecognitionException {
      return this.gParser.mapLiteral(k);
   }

   public org.apache.cassandra.cql3.Tuples.Raw markerForTuple() throws RecognitionException {
      return this.gParser.markerForTuple();
   }

   public Cql_Parser.mbean_return mbean() throws RecognitionException {
      return this.gParser.mbean();
   }

   public CQL3Type native_type() throws RecognitionException {
      return this.gParser.native_type();
   }

   public ColumnIdentifier non_type_ident() throws RecognitionException {
      return this.gParser.non_type_ident();
   }

   public ColumnIdentifier noncol_ident() throws RecognitionException {
      return this.gParser.noncol_ident();
   }

   public void normalColumnOperation(List<Pair<Raw, RawUpdate>> operations, Raw key) throws RecognitionException {
      this.gParser.normalColumnOperation(operations, key);
   }

   public ParsedInsert normalInsertStatement(CFName cf) throws RecognitionException {
      return this.gParser.normalInsertStatement(cf);
   }

   public Map<String, String> optionMap_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.optionMap_DseSearchClause();
   }

   public Cql_DseSearchParser.optionName_DseSearchClause_return optionName_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.optionName_DseSearchClause();
   }

   public Cql_DseSearchParser.optionValue_DseSearchClause_return optionValue_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.optionValue_DseSearchClause();
   }

   public void orderByClause(Map<Raw, Boolean> orderings) throws RecognitionException {
      this.gParser.orderByClause(orderings);
   }

   public Permission permission() throws RecognitionException {
      return this.gParser.permission();
   }

   public Cql_Parser.permissionDomain_return permissionDomain() throws RecognitionException {
      return this.gParser.permissionDomain();
   }

   public Cql_Parser.permissionName_return permissionName() throws RecognitionException {
      return this.gParser.permissionName();
   }

   public Set<Permission> permissionOrAll() throws RecognitionException {
      return this.gParser.permissionOrAll();
   }

   public void pkDef(RawStatement expr) throws RecognitionException {
      this.gParser.pkDef(expr);
   }

   public List<String> profileList_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.profileList_DseSearchClause();
   }

   public String profileName_DseSearchClause() throws RecognitionException {
      return this.gDseSearchParser.profileName_DseSearchClause();
   }

   public void properties(PropertyDefinitions props) throws RecognitionException {
      this.gParser.properties(props);
   }

   public void property(PropertyDefinitions props) throws RecognitionException {
      this.gParser.property(props);
   }

   public String propertyValue() throws RecognitionException {
      return this.gParser.propertyValue();
   }

   public final ParsedStatement query() throws RecognitionException {
      ParsedStatement stmnt = null;
      ParsedStatement st = null;

      try {
         try {
            int alt3 = true;
            int alt3 = this.dfa3.predict(this.input);
            byte alt2;
            int LA2_0;
            switch(alt3) {
            case 1:
               this.pushFollow(FOLLOW_cqlStatement_in_query757);
               st = this.cqlStatement();
               --this.state._fsp;

               while(true) {
                  alt2 = 2;
                  LA2_0 = this.input.LA(1);
                  if(LA2_0 == 229) {
                     alt2 = 1;
                  }

                  switch(alt2) {
                  case 1:
                     this.match(this.input, 229, FOLLOW_229_in_query760);
                     break;
                  default:
                     this.match(this.input, -1, FOLLOW_EOF_in_query764);
                     stmnt = st;
                     return stmnt;
                  }
               }
            case 2:
               this.pushFollow(FOLLOW_dseStatement_in_query776);
               st = this.dseStatement();
               --this.state._fsp;

               while(true) {
                  alt2 = 2;
                  LA2_0 = this.input.LA(1);
                  if(LA2_0 == 229) {
                     alt2 = 1;
                  }

                  switch(alt2) {
                  case 1:
                     this.match(this.input, 229, FOLLOW_229_in_query779);
                     break;
                  default:
                     this.match(this.input, -1, FOLLOW_EOF_in_query783);
                     stmnt = st;
                     return stmnt;
                  }
               }
            }
         } catch (RecognitionException var9) {
            this.reportError(var9);
            this.recover(this.input, var9);
         }

         return stmnt;
      } finally {
         ;
      }
   }

   public ParsedStatement rebuildSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.rebuildSearchIndexStatement();
   }

   public void recover(IntStream input, RecognitionException re) {
   }

   protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow) throws RecognitionException {
      throw new MismatchedTokenException(ttype, input);
   }

   public void relation(Builder clauses) throws RecognitionException {
      this.gParser.relation(clauses);
   }

   public void relationOrExpression(Builder clause) throws RecognitionException {
      this.gParser.relationOrExpression(clause);
   }

   public Operator relationType() throws RecognitionException {
      return this.gParser.relationType();
   }

   public ParsedStatement reloadSearchIndexStatement() throws RecognitionException {
      return this.gDseSearchParser.reloadSearchIndexStatement();
   }

   public void removeErrorListener(ErrorListener listener) {
      this.gParser.removeErrorListener(listener);
   }

   public Map<FieldIdentifier, FieldIdentifier> renamedColumns() throws RecognitionException {
      return this.gParser.renamedColumns();
   }

   public final IResource resource() throws RecognitionException {
      IResource res = null;
      IResource c = null;
      IResource dc = null;
      IResource ds = null;

      try {
         try {
            int alt5 = true;
            int alt5 = this.dfa5.predict(this.input);
            switch(alt5) {
            case 1:
               this.pushFollow(FOLLOW_cassandraResource_in_resource883);
               c = this.cassandraResource();
               --this.state._fsp;
               res = c;
               break;
            case 2:
               this.pushFollow(FOLLOW_dseCoreResource_in_resource895);
               dc = this.dseCoreResource();
               --this.state._fsp;
               res = dc;
               break;
            case 3:
               this.pushFollow(FOLLOW_dseSearchResource_in_resource907);
               ds = this.dseSearchResource();
               --this.state._fsp;
               res = ds;
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

   public IResource resourceFromInternalName() throws RecognitionException {
      return this.gParser.resourceFromInternalName();
   }

   public GrantPermissionsStatement restrictPermissionsStatement() throws RecognitionException {
      return this.gParser.restrictPermissionsStatement();
   }

   public RestrictRowsStatement restrictRowsStatement() throws RecognitionException {
      return this.gDseCoreParser.restrictRowsStatement();
   }

   public RevokePermissionsStatement revokePermissionsStatement() throws RecognitionException {
      return this.gParser.revokePermissionsStatement();
   }

   public RevokeRoleStatement revokeRoleStatement() throws RecognitionException {
      return this.gParser.revokeRoleStatement();
   }

   public void roleName(RoleName name) throws RecognitionException {
      this.gParser.roleName(name);
   }

   public void roleOption(RoleOptions opts) throws RecognitionException {
      this.gParser.roleOption(opts);
   }

   public void roleOptions(RoleOptions opts) throws RecognitionException {
      this.gParser.roleOptions(opts);
   }

   public RoleResource roleResource() throws RecognitionException {
      return this.gParser.roleResource();
   }

   public RpcResource rpcCallResource() throws RecognitionException {
      return this.gDseCoreParser.rpcCallResource();
   }

   public ParsedStatement rpcCallStatement() throws RecognitionException {
      return this.gDseCoreParser.rpcCallStatement();
   }

   public String rpcObjectName() throws RecognitionException {
      return this.gDseCoreParser.rpcObjectName();
   }

   public Raw schema_cident() throws RecognitionException {
      return this.gParser.schema_cident();
   }

   public AuthenticationScheme scheme() throws RecognitionException {
      return this.gDseCoreParser.scheme();
   }

   public IResource searchIndexResource() throws RecognitionException {
      return this.gDseSearchParser.searchIndexResource();
   }

   public Cql_Parser.selectClause_return selectClause() throws RecognitionException {
      return this.gParser.selectClause();
   }

   public org.apache.cassandra.cql3.statements.SelectStatement.RawStatement selectStatement() throws RecognitionException {
      return this.gParser.selectStatement();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionAddition() throws RecognitionException {
      return this.gParser.selectionAddition();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionFunction() throws RecognitionException {
      return this.gParser.selectionFunction();
   }

   public List<org.apache.cassandra.cql3.selection.Selectable.Raw> selectionFunctionArgs() throws RecognitionException {
      return this.gParser.selectionFunctionArgs();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionGroup() throws RecognitionException {
      return this.gParser.selectionGroup();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionGroupWithField() throws RecognitionException {
      return this.gParser.selectionGroupWithField();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionGroupWithoutField() throws RecognitionException {
      return this.gParser.selectionGroupWithoutField();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionList() throws RecognitionException {
      return this.gParser.selectionList();
   }

   public org.apache.cassandra.cql3.Term.Raw selectionLiteral() throws RecognitionException {
      return this.gParser.selectionLiteral();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionMap(org.apache.cassandra.cql3.selection.Selectable.Raw k1) throws RecognitionException {
      return this.gParser.selectionMap(k1);
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionMapOrSet() throws RecognitionException {
      return this.gParser.selectionMapOrSet();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionMultiplication() throws RecognitionException {
      return this.gParser.selectionMultiplication();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionSet(org.apache.cassandra.cql3.selection.Selectable.Raw t1) throws RecognitionException {
      return this.gParser.selectionSet(t1);
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionTupleOrNestedSelector() throws RecognitionException {
      return this.gParser.selectionTupleOrNestedSelector();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectionTypeHint() throws RecognitionException {
      return this.gParser.selectionTypeHint();
   }

   public RawSelector selector() throws RecognitionException {
      return this.gParser.selector();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw selectorModifier(org.apache.cassandra.cql3.selection.Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.selectorModifier(receiver);
   }

   public List<RawSelector> selectors() throws RecognitionException {
      return this.gParser.selectors();
   }

   public org.apache.cassandra.cql3.Term.Raw setLiteral(org.apache.cassandra.cql3.Term.Raw t) throws RecognitionException {
      return this.gParser.setLiteral(t);
   }

   public org.apache.cassandra.cql3.Term.Raw setOrMapLiteral(org.apache.cassandra.cql3.Term.Raw t) throws RecognitionException {
      return this.gParser.setOrMapLiteral(t);
   }

   public void shorthandColumnOperation(List<Pair<Raw, RawUpdate>> operations, Raw key) throws RecognitionException {
      this.gParser.shorthandColumnOperation(operations, key);
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw sident() throws RecognitionException {
      return this.gParser.sident();
   }

   public org.apache.cassandra.cql3.Term.Raw simpleTerm() throws RecognitionException {
      return this.gParser.simpleTerm();
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw simpleUnaliasedSelector() throws RecognitionException {
      return this.gParser.simpleUnaliasedSelector();
   }

   public List<org.apache.cassandra.cql3.Term.Raw> singleColumnInValues() throws RecognitionException {
      return this.gParser.singleColumnInValues();
   }

   public ResourceManagerSubmissionResource submissionResource() throws RecognitionException {
      return this.gDseCoreParser.submissionResource();
   }

   public org.apache.cassandra.cql3.Term.Raw term() throws RecognitionException {
      return this.gParser.term();
   }

   public org.apache.cassandra.cql3.Term.Raw termAddition() throws RecognitionException {
      return this.gParser.termAddition();
   }

   public org.apache.cassandra.cql3.Term.Raw termGroup() throws RecognitionException {
      return this.gParser.termGroup();
   }

   public org.apache.cassandra.cql3.Term.Raw termMultiplication() throws RecognitionException {
      return this.gParser.termMultiplication();
   }

   public TruncateStatement truncateStatement() throws RecognitionException {
      return this.gParser.truncateStatement();
   }

   public org.apache.cassandra.cql3.Tuples.Literal tupleLiteral() throws RecognitionException {
      return this.gParser.tupleLiteral();
   }

   public List<Raw> tupleOfIdentifiers() throws RecognitionException {
      return this.gParser.tupleOfIdentifiers();
   }

   public List<org.apache.cassandra.cql3.Tuples.Raw> tupleOfMarkersForTuples() throws RecognitionException {
      return this.gParser.tupleOfMarkersForTuples();
   }

   public List<org.apache.cassandra.cql3.Tuples.Literal> tupleOfTupleLiterals() throws RecognitionException {
      return this.gParser.tupleOfTupleLiterals();
   }

   public List<org.apache.cassandra.cql3.CQL3Type.Raw> tuple_types() throws RecognitionException {
      return this.gParser.tuple_types();
   }

   public void typeColumns(CreateTypeStatement expr) throws RecognitionException {
      this.gParser.typeColumns(expr);
   }

   public void udtColumnOperation(List<Pair<Raw, RawUpdate>> operations, Raw key, FieldIdentifier field) throws RecognitionException {
      this.gParser.udtColumnOperation(operations, key, field);
   }

   public org.apache.cassandra.cql3.selection.Selectable.Raw unaliasedSelector() throws RecognitionException {
      return this.gParser.unaliasedSelector();
   }

   public String unreserved_function_keyword() throws RecognitionException {
      return this.gParser.unreserved_function_keyword();
   }

   public final String unreserved_keyword() throws RecognitionException {
      String str = null;
      Token k = null;
      String u = null;
      String d = null;

      try {
         try {
            int alt6 = true;
            byte alt6;
            switch(this.input.LA(1)) {
            case 28:
            case 29:
            case 35:
            case 37:
            case 42:
            case 43:
            case 44:
            case 47:
            case 50:
            case 54:
            case 56:
            case 58:
            case 60:
            case 61:
            case 62:
            case 67:
            case 69:
            case 71:
            case 74:
            case 76:
            case 77:
            case 78:
            case 81:
            case 83:
            case 84:
            case 86:
            case 91:
            case 92:
            case 93:
            case 95:
            case 102:
            case 104:
            case 105:
            case 107:
            case 109:
            case 110:
            case 111:
            case 117:
            case 120:
            case 122:
            case 128:
            case 131:
            case 132:
            case 133:
            case 134:
            case 135:
            case 145:
            case 147:
            case 149:
            case 150:
            case 158:
            case 159:
            case 160:
            case 161:
            case 162:
            case 164:
            case 165:
            case 166:
            case 167:
            case 168:
            case 169:
            case 172:
            case 175:
            case 176:
            case 182:
            case 183:
            case 185:
            case 186:
            case 187:
            case 188:
               alt6 = 1;
               break;
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
            default:
               NoViableAltException nvae = new NoViableAltException("", 6, 0, this.input);
               throw nvae;
            case 33:
            case 38:
            case 46:
            case 48:
            case 52:
            case 53:
            case 55:
            case 63:
            case 75:
            case 90:
            case 96:
            case 100:
            case 106:
            case 115:
            case 125:
            case 139:
            case 140:
            case 141:
            case 142:
            case 146:
            case 151:
            case 152:
            case 153:
            case 154:
            case 155:
            case 163:
            case 178:
            case 179:
            case 192:
               alt6 = 2;
               break;
            case 49:
            case 57:
            case 68:
            case 99:
            case 101:
            case 174:
            case 193:
               alt6 = 3;
            }

            switch(alt6) {
            case 1:
               this.pushFollow(FOLLOW_unreserved_function_keyword_in_unreserved_keyword937);
               u = this.unreserved_function_keyword();
               --this.state._fsp;
               str = u;
               break;
            case 2:
               this.pushFollow(FOLLOW_dse_unreserved_keyword_in_unreserved_keyword953);
               d = this.dse_unreserved_keyword();
               --this.state._fsp;
               str = d;
               break;
            case 3:
               k = this.input.LT(1);
               if(this.input.LA(1) != 49 && this.input.LA(1) != 57 && this.input.LA(1) != 68 && this.input.LA(1) != 99 && this.input.LA(1) != 101 && this.input.LA(1) != 174 && this.input.LA(1) != 193) {
                  MismatchedSetException mse = new MismatchedSetException((BitSet)null, this.input);
                  throw mse;
               }

               this.input.consume();
               this.state.errorRecovery = false;
               str = k != null?k.getText():null;
            }
         } catch (RecognitionException var10) {
            this.reportError(var10);
            this.recover(this.input, var10);
         }

         return str;
      } finally {
         ;
      }
   }

   public RevokePermissionsStatement unrestrictPermissionsStatement() throws RecognitionException {
      return this.gParser.unrestrictPermissionsStatement();
   }

   public UnRestrictRowsStatement unrestrictRowsStatement() throws RecognitionException {
      return this.gDseCoreParser.unrestrictRowsStatement();
   }

   public List<Pair<Raw, org.apache.cassandra.cql3.conditions.ColumnCondition.Raw>> updateConditions() throws RecognitionException {
      return this.gParser.updateConditions();
   }

   public ParsedUpdate updateStatement() throws RecognitionException {
      return this.gParser.updateStatement();
   }

   public UseStatement useStatement() throws RecognitionException {
      return this.gParser.useStatement();
   }

   public RoleName userOrRoleName() throws RecognitionException {
      return this.gParser.userOrRoleName();
   }

   public void userPassword(RoleOptions opts) throws RecognitionException {
      this.gParser.userPassword(opts);
   }

   public UTName userTypeName() throws RecognitionException {
      return this.gParser.userTypeName();
   }

   public Cql_Parser.username_return username() throws RecognitionException {
      return this.gParser.username();
   }

   public org.apache.cassandra.cql3.UserTypes.Literal usertypeLiteral() throws RecognitionException {
      return this.gParser.usertypeLiteral();
   }

   public void usingClause(org.apache.cassandra.cql3.Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClause(attrs);
   }

   public void usingClauseDelete(org.apache.cassandra.cql3.Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClauseDelete(attrs);
   }

   public void usingClauseObjective(org.apache.cassandra.cql3.Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClauseObjective(attrs);
   }

   public org.apache.cassandra.cql3.Term.Raw value() throws RecognitionException {
      return this.gParser.value();
   }

   public Builder whereClause() throws RecognitionException {
      return this.gParser.whereClause();
   }

   public ResourceManagerWorkPoolResource workPoolResource() throws RecognitionException {
      return this.gDseCoreParser.workPoolResource();
   }

   static {
      int numStates = DFA3_transitionS.length;
      DFA3_transition = new short[numStates][];

      int i;
      for(i = 0; i < numStates; ++i) {
         DFA3_transition[i] = DFA.unpackEncodedString(DFA3_transitionS[i]);
      }

      DFA5_transitionS = new String[]{"\u0001\u0002\u0004\uffff\u0001\u0002\u0001\u0001\u0003\uffff\u0001\u0006\u0001\uffff\u0001\u0002\u0001\uffff\u0002\u0002\u0003\uffff\u0003\u0002\u0001\uffff\r\u0002\u0001\uffff\u0004\u0002\u0003\uffff\u0003\u0002\u0001\uffff\u0001\u0002\u0002\uffff\u0005\u0002\u0002\uffff\u0001\u0002\u0001\uffff\u0002\u0002\u0001\uffff\u0001\u0002\u0003\uffff\u0004\u0002\u0001\uffff\u0001\u0002\u0001\u0003\u0002\uffff\u0001\u0002\u0001\u0003\u0005\u0002\u0001\u0003\u0001\u0002\u0001\uffff\u0003\u0002\u0001\uffff\u0003\u0002\u0001\uffff\u0001\u0002\u0002\uffff\u0001\u0002\u0001\uffff\u0001\u0002\u0002\uffff\u0001\u0002\u0002\uffff\u0001\u0002\u0002\uffff\u0005\u0002\u0003\uffff\u0003\u0002\u0001\u0005\u0002\uffff\u0003\u0002\u0001\uffff\u0006\u0002\u0001\t\u0002\uffff\u0005\u0002\u0001\b\u0006\u0002\u0002\uffff\u0001\u0002\u0001\uffff\u0003\u0002\u0001\uffff\u0002\u0002\u0002\uffff\u0002\u0002\u0001\uffff\u0004\u0002\u0003\uffff\u0001\u0007\u0001\u0002\b\uffff\u0002\u0002\u0003\uffff\u0001\u0004", "\u0001\u0004)\uffff\u0001\u0002\u0003\uffff\u0001\u0002\u0013\uffff\u0001\u0002\t\uffff\u0001\u0002\u0006\uffff\u0001\u0002\u0004\uffff\u0001\u0002\u000f\uffff\u0001\u0004\u0007\uffff\u0001\u0002\u0004\uffff\u0001\n\u000e\uffff\u0001\u00027\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "", "\u0001\u0002(\uffff\u0001\u0002\u0004\uffff\u0001\u0002\u001a\uffff\u0001\u0004\u0010\uffff\u0001\u00027\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "", "\u0001\u0002\"\uffff\u0001\u0004\u0005\uffff\u0001\u0002\u0003\uffff\u0001\u0004\u0001\u0002+\uffff\u0001\u00027\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "\u0001\u0002(\uffff\u0001\u0002\u0004\uffff\u0001\u0002$\uffff\u0001\u0004\u0006\uffff\u0001\u0002\u0015\uffff\u0001\u0004!\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "\u0001\u0002(\uffff\u0001\u0002\u0004\uffff\u0001\u0002+\uffff\u0001\u0002$\uffff\u0001\u0004\u0012\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "\u0001\u0002(\uffff\u0001\u0002\u0004\uffff\u0001\u0002+\uffff\u0001\u0002$\uffff\u0001\u0004\u0012\uffff\u0001\u0002\u0002\uffff\u0001\u0002", "\u0001\u0002\b\uffff\u0001\n\r\uffff\u0001\n\u0011\uffff\u0001\u0002\u0004\uffff\u0001\u0002+\uffff\u0001\u00027\uffff\u0001\u0002\u0002\uffff\u0001\u0002", ""};
      DFA5_eot = DFA.unpackEncodedString("\u000b\uffff");
      DFA5_eof = DFA.unpackEncodedString("\u0001\uffff\u0001\u0002\u0001\uffff\u0001\u0002\u0001\uffff\u0005\u0002\u0001\uffff");
      DFA5_min = DFA.unpackEncodedStringToUnsignedChars("\u0001\u0017\u0001&\u0001\uffff\u0001P\u0001\uffff\u0005P\u0001\uffff");
      DFA5_max = DFA.unpackEncodedStringToUnsignedChars("\u0001Ï\u0001å\u0001\uffff\u0001å\u0001\uffff\u0005å\u0001\uffff");
      DFA5_accept = DFA.unpackEncodedString("\u0002\uffff\u0001\u0001\u0001\uffff\u0001\u0002\u0005\uffff\u0001\u0003");
      DFA5_special = DFA.unpackEncodedString("\u000b\uffff}>");
      numStates = DFA5_transitionS.length;
      DFA5_transition = new short[numStates][];

      for(i = 0; i < numStates; ++i) {
         DFA5_transition[i] = DFA.unpackEncodedString(DFA5_transitionS[i]);
      }

      FOLLOW_cqlStatement_in_query757 = new BitSet(new long[]{0L, 0L, 0L, 137438953472L});
      FOLLOW_229_in_query760 = new BitSet(new long[]{0L, 0L, 0L, 137438953472L});
      FOLLOW_EOF_in_query764 = new BitSet(new long[]{2L});
      FOLLOW_dseStatement_in_query776 = new BitSet(new long[]{0L, 0L, 0L, 137438953472L});
      FOLLOW_229_in_query779 = new BitSet(new long[]{0L, 0L, 0L, 137438953472L});
      FOLLOW_EOF_in_query783 = new BitSet(new long[]{2L});
      FOLLOW_dseCoreStatement_in_dseStatement816 = new BitSet(new long[]{2L});
      FOLLOW_dseSearchStatement_in_dseStatement844 = new BitSet(new long[]{2L});
      FOLLOW_cassandraResource_in_resource883 = new BitSet(new long[]{2L});
      FOLLOW_dseCoreResource_in_resource895 = new BitSet(new long[]{2L});
      FOLLOW_dseSearchResource_in_resource907 = new BitSet(new long[]{2L});
      FOLLOW_unreserved_function_keyword_in_unreserved_keyword937 = new BitSet(new long[]{2L});
      FOLLOW_dse_unreserved_keyword_in_unreserved_keyword953 = new BitSet(new long[]{2L});
      FOLLOW_set_in_unreserved_keyword965 = new BitSet(new long[]{2L});
      FOLLOW_set_in_dse_unreserved_keyword1016 = new BitSet(new long[]{2L});
   }

   protected class DFA5 extends DFA {
      public DFA5(BaseRecognizer recognizer) {
         this.recognizer = recognizer;
         this.decisionNumber = 5;
         this.eot = CqlParser.DFA5_eot;
         this.eof = CqlParser.DFA5_eof;
         this.min = CqlParser.DFA5_min;
         this.max = CqlParser.DFA5_max;
         this.accept = CqlParser.DFA5_accept;
         this.special = CqlParser.DFA5_special;
         this.transition = CqlParser.DFA5_transition;
      }

      public String getDescription() {
         return "185:1: resource returns [IResource res] : (c= cassandraResource |dc= dseCoreResource |ds= dseSearchResource );";
      }
   }

   protected class DFA3 extends DFA {
      public DFA3(BaseRecognizer recognizer) {
         this.recognizer = recognizer;
         this.decisionNumber = 3;
         this.eot = CqlParser.DFA3_eot;
         this.eof = CqlParser.DFA3_eof;
         this.min = CqlParser.DFA3_min;
         this.max = CqlParser.DFA3_max;
         this.accept = CqlParser.DFA3_accept;
         this.special = CqlParser.DFA3_special;
         this.transition = CqlParser.DFA3_transition;
      }

      public String getDescription() {
         return "174:1: query returns [ParsedStatement stmnt] : (st= cqlStatement ( ';' )* EOF |st= dseStatement ( ';' )* EOF );";
      }
   }
}
