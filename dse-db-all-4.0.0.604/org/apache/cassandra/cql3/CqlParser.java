package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.AlterViewStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.cql3.statements.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.CreateViewStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
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
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;

public class CqlParser extends Parser {
   public static final String[] tokenNames = new String[]{"<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "B", "BOOLEAN", "C", "COMMENT", "D", "DIGIT", "DURATION", "DURATION_UNIT", "E", "EMPTY_QUOTED_NAME", "EXPONENT", "F", "FLOAT", "G", "H", "HEX", "HEXNUMBER", "I", "IDENT", "INTEGER", "J", "K", "K_ADD", "K_AGGREGATE", "K_ALL", "K_ALLOW", "K_ALTER", "K_AND", "K_APPLY", "K_AS", "K_ASC", "K_ASCII", "K_AUTHORIZE", "K_BATCH", "K_BEGIN", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_BY", "K_CALLED", "K_CAST", "K_CLUSTERING", "K_COLUMNFAMILY", "K_COMPACT", "K_CONTAINS", "K_COUNT", "K_COUNTER", "K_CREATE", "K_CUSTOM", "K_DATE", "K_DECIMAL", "K_DEFAULT", "K_DELETE", "K_DESC", "K_DESCRIBE", "K_DETERMINISTIC", "K_DISTINCT", "K_DOUBLE", "K_DROP", "K_DURATION", "K_ENTRIES", "K_EXECUTE", "K_EXISTS", "K_FILTERING", "K_FINALFUNC", "K_FLOAT", "K_FOR", "K_FROM", "K_FROZEN", "K_FULL", "K_FUNCTION", "K_FUNCTIONS", "K_GRANT", "K_GROUP", "K_IF", "K_IN", "K_INDEX", "K_INET", "K_INITCOND", "K_INPUT", "K_INSERT", "K_INT", "K_INTO", "K_IS", "K_JSON", "K_KEY", "K_KEYS", "K_KEYSPACE", "K_KEYSPACES", "K_LANGUAGE", "K_LIKE", "K_LIMIT", "K_LIST", "K_LOGIN", "K_MAP", "K_MATERIALIZED", "K_MBEAN", "K_MBEANS", "K_MODIFY", "K_MONOTONIC", "K_NEGATIVE_INFINITY", "K_NEGATIVE_NAN", "K_NOLOGIN", "K_NORECURSIVE", "K_NOSUPERUSER", "K_NOT", "K_NULL", "K_OF", "K_ON", "K_OPTIONS", "K_OR", "K_ORDER", "K_PARTITION", "K_PASSWORD", "K_PER", "K_PERMISSION", "K_PERMISSIONS", "K_POSITIVE_INFINITY", "K_POSITIVE_NAN", "K_PRIMARY", "K_RENAME", "K_REPLACE", "K_RESOURCE", "K_RESTRICT", "K_RETURNS", "K_REVOKE", "K_ROLE", "K_ROLES", "K_SELECT", "K_SET", "K_SFUNC", "K_SMALLINT", "K_STATIC", "K_STORAGE", "K_STYPE", "K_SUPERUSER", "K_TEXT", "K_TIME", "K_TIMESTAMP", "K_TIMEUUID", "K_TINYINT", "K_TO", "K_TOKEN", "K_TRIGGER", "K_TRUNCATE", "K_TTL", "K_TUPLE", "K_TYPE", "K_UNLOGGED", "K_UNRESTRICT", "K_UNSET", "K_UPDATE", "K_USE", "K_USER", "K_USERS", "K_USING", "K_UUID", "K_VALUES", "K_VARCHAR", "K_VARINT", "K_VIEW", "K_WHERE", "K_WITH", "K_WRITETIME", "L", "LETTER", "M", "MULTILINE_COMMENT", "N", "O", "P", "Q", "QMARK", "QUOTED_NAME", "R", "RANGE", "S", "STRING_LITERAL", "T", "U", "UUID", "V", "W", "WS", "X", "Y", "Z", "'!='", "'%'", "'('", "')'", "'+'", "'+='", "','", "'-'", "'-='", "'.'", "'/'", "':'", "';'", "'<'", "'<='", "'='", "'>'", "'>='", "'['", "'\\*'", "']'", "'expr('", "'{'", "'}'"};
   public static final int EOF = -1;
   public static final int T__192 = 192;
   public static final int T__193 = 193;
   public static final int T__194 = 194;
   public static final int T__195 = 195;
   public static final int T__196 = 196;
   public static final int T__197 = 197;
   public static final int T__198 = 198;
   public static final int T__199 = 199;
   public static final int T__200 = 200;
   public static final int T__201 = 201;
   public static final int T__202 = 202;
   public static final int T__203 = 203;
   public static final int T__204 = 204;
   public static final int T__205 = 205;
   public static final int T__206 = 206;
   public static final int T__207 = 207;
   public static final int T__208 = 208;
   public static final int T__209 = 209;
   public static final int T__210 = 210;
   public static final int T__211 = 211;
   public static final int T__212 = 212;
   public static final int T__213 = 213;
   public static final int T__214 = 214;
   public static final int T__215 = 215;
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
   public static final int K_APPLY = 33;
   public static final int K_AS = 34;
   public static final int K_ASC = 35;
   public static final int K_ASCII = 36;
   public static final int K_AUTHORIZE = 37;
   public static final int K_BATCH = 38;
   public static final int K_BEGIN = 39;
   public static final int K_BIGINT = 40;
   public static final int K_BLOB = 41;
   public static final int K_BOOLEAN = 42;
   public static final int K_BY = 43;
   public static final int K_CALLED = 44;
   public static final int K_CAST = 45;
   public static final int K_CLUSTERING = 46;
   public static final int K_COLUMNFAMILY = 47;
   public static final int K_COMPACT = 48;
   public static final int K_CONTAINS = 49;
   public static final int K_COUNT = 50;
   public static final int K_COUNTER = 51;
   public static final int K_CREATE = 52;
   public static final int K_CUSTOM = 53;
   public static final int K_DATE = 54;
   public static final int K_DECIMAL = 55;
   public static final int K_DEFAULT = 56;
   public static final int K_DELETE = 57;
   public static final int K_DESC = 58;
   public static final int K_DESCRIBE = 59;
   public static final int K_DETERMINISTIC = 60;
   public static final int K_DISTINCT = 61;
   public static final int K_DOUBLE = 62;
   public static final int K_DROP = 63;
   public static final int K_DURATION = 64;
   public static final int K_ENTRIES = 65;
   public static final int K_EXECUTE = 66;
   public static final int K_EXISTS = 67;
   public static final int K_FILTERING = 68;
   public static final int K_FINALFUNC = 69;
   public static final int K_FLOAT = 70;
   public static final int K_FOR = 71;
   public static final int K_FROM = 72;
   public static final int K_FROZEN = 73;
   public static final int K_FULL = 74;
   public static final int K_FUNCTION = 75;
   public static final int K_FUNCTIONS = 76;
   public static final int K_GRANT = 77;
   public static final int K_GROUP = 78;
   public static final int K_IF = 79;
   public static final int K_IN = 80;
   public static final int K_INDEX = 81;
   public static final int K_INET = 82;
   public static final int K_INITCOND = 83;
   public static final int K_INPUT = 84;
   public static final int K_INSERT = 85;
   public static final int K_INT = 86;
   public static final int K_INTO = 87;
   public static final int K_IS = 88;
   public static final int K_JSON = 89;
   public static final int K_KEY = 90;
   public static final int K_KEYS = 91;
   public static final int K_KEYSPACE = 92;
   public static final int K_KEYSPACES = 93;
   public static final int K_LANGUAGE = 94;
   public static final int K_LIKE = 95;
   public static final int K_LIMIT = 96;
   public static final int K_LIST = 97;
   public static final int K_LOGIN = 98;
   public static final int K_MAP = 99;
   public static final int K_MATERIALIZED = 100;
   public static final int K_MBEAN = 101;
   public static final int K_MBEANS = 102;
   public static final int K_MODIFY = 103;
   public static final int K_MONOTONIC = 104;
   public static final int K_NEGATIVE_INFINITY = 105;
   public static final int K_NEGATIVE_NAN = 106;
   public static final int K_NOLOGIN = 107;
   public static final int K_NORECURSIVE = 108;
   public static final int K_NOSUPERUSER = 109;
   public static final int K_NOT = 110;
   public static final int K_NULL = 111;
   public static final int K_OF = 112;
   public static final int K_ON = 113;
   public static final int K_OPTIONS = 114;
   public static final int K_OR = 115;
   public static final int K_ORDER = 116;
   public static final int K_PARTITION = 117;
   public static final int K_PASSWORD = 118;
   public static final int K_PER = 119;
   public static final int K_PERMISSION = 120;
   public static final int K_PERMISSIONS = 121;
   public static final int K_POSITIVE_INFINITY = 122;
   public static final int K_POSITIVE_NAN = 123;
   public static final int K_PRIMARY = 124;
   public static final int K_RENAME = 125;
   public static final int K_REPLACE = 126;
   public static final int K_RESOURCE = 127;
   public static final int K_RESTRICT = 128;
   public static final int K_RETURNS = 129;
   public static final int K_REVOKE = 130;
   public static final int K_ROLE = 131;
   public static final int K_ROLES = 132;
   public static final int K_SELECT = 133;
   public static final int K_SET = 134;
   public static final int K_SFUNC = 135;
   public static final int K_SMALLINT = 136;
   public static final int K_STATIC = 137;
   public static final int K_STORAGE = 138;
   public static final int K_STYPE = 139;
   public static final int K_SUPERUSER = 140;
   public static final int K_TEXT = 141;
   public static final int K_TIME = 142;
   public static final int K_TIMESTAMP = 143;
   public static final int K_TIMEUUID = 144;
   public static final int K_TINYINT = 145;
   public static final int K_TO = 146;
   public static final int K_TOKEN = 147;
   public static final int K_TRIGGER = 148;
   public static final int K_TRUNCATE = 149;
   public static final int K_TTL = 150;
   public static final int K_TUPLE = 151;
   public static final int K_TYPE = 152;
   public static final int K_UNLOGGED = 153;
   public static final int K_UNRESTRICT = 154;
   public static final int K_UNSET = 155;
   public static final int K_UPDATE = 156;
   public static final int K_USE = 157;
   public static final int K_USER = 158;
   public static final int K_USERS = 159;
   public static final int K_USING = 160;
   public static final int K_UUID = 161;
   public static final int K_VALUES = 162;
   public static final int K_VARCHAR = 163;
   public static final int K_VARINT = 164;
   public static final int K_VIEW = 165;
   public static final int K_WHERE = 166;
   public static final int K_WITH = 167;
   public static final int K_WRITETIME = 168;
   public static final int L = 169;
   public static final int LETTER = 170;
   public static final int M = 171;
   public static final int MULTILINE_COMMENT = 172;
   public static final int N = 173;
   public static final int O = 174;
   public static final int P = 175;
   public static final int Q = 176;
   public static final int QMARK = 177;
   public static final int QUOTED_NAME = 178;
   public static final int R = 179;
   public static final int RANGE = 180;
   public static final int S = 181;
   public static final int STRING_LITERAL = 182;
   public static final int T = 183;
   public static final int U = 184;
   public static final int UUID = 185;
   public static final int V = 186;
   public static final int W = 187;
   public static final int WS = 188;
   public static final int X = 189;
   public static final int Y = 190;
   public static final int Z = 191;
   public Cql_Parser gParser;
   public static final BitSet FOLLOW_cqlStatement_in_query77 = new BitSet(new long[]{0L, 0L, 0L, 4096L});
   public static final BitSet FOLLOW_204_in_query80 = new BitSet(new long[]{0L, 0L, 0L, 4096L});
   public static final BitSet FOLLOW_EOF_in_query84 = new BitSet(new long[]{2L});
   public static final BitSet FOLLOW_cassandraResource_in_resource109 = new BitSet(new long[]{2L});

   public CqlParser(TokenStream input) {
      this(input, new RecognizerSharedState());
   }

   public CqlParser(TokenStream input, RecognizerSharedState state) {
      super(input, state);
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

   public AlterTableStatement alterTableStatement() throws RecognitionException {
      return this.gParser.alterTableStatement();
   }

   public AlterTypeStatement alterTypeStatement() throws RecognitionException {
      return this.gParser.alterTypeStatement();
   }

   public AlterRoleStatement alterUserStatement() throws RecognitionException {
      return this.gParser.alterUserStatement();
   }

   public String basic_unreserved_keyword() throws RecognitionException {
      return this.gParser.basic_unreserved_keyword();
   }

   public BatchStatement.Parsed batchStatement() throws RecognitionException {
      return this.gParser.batchStatement();
   }

   public ModificationStatement.Parsed batchStatementObjective() throws RecognitionException {
      return this.gParser.batchStatementObjective();
   }

   public IResource cassandraResource() throws RecognitionException {
      return this.gParser.cassandraResource();
   }

   public void cfName(CFName name) throws RecognitionException {
      this.gParser.cfName(name);
   }

   public void cfamColumns(CreateTableStatement.RawStatement expr) throws RecognitionException {
      this.gParser.cfamColumns(expr);
   }

   public void cfamDefinition(CreateTableStatement.RawStatement expr) throws RecognitionException {
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

   public ColumnMetadata.Raw cident() throws RecognitionException {
      return this.gParser.cident();
   }

   public void collectionColumnOperation(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations, ColumnMetadata.Raw key, Term.Raw k) throws RecognitionException {
      this.gParser.collectionColumnOperation(operations, key, k);
   }

   public Term.Raw collectionLiteral() throws RecognitionException {
      return this.gParser.collectionLiteral();
   }

   public Selectable.Raw collectionSubSelection(Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.collectionSubSelection(receiver);
   }

   public CQL3Type.Raw collection_type() throws RecognitionException {
      return this.gParser.collection_type();
   }

   public void columnCondition(List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions) throws RecognitionException {
      this.gParser.columnCondition(conditions);
   }

   public CFName columnFamilyName() throws RecognitionException {
      return this.gParser.columnFamilyName();
   }

   public void columnOperation(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations) throws RecognitionException {
      this.gParser.columnOperation(operations);
   }

   public void columnOperationDifferentiator(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations, ColumnMetadata.Raw key) throws RecognitionException {
      this.gParser.columnOperationDifferentiator(operations, key);
   }

   public CQL3Type.Raw comparatorType() throws RecognitionException {
      return this.gParser.comparatorType();
   }

   public CQL3Type.Raw comparatorTypeWithMultiCellTuple() throws RecognitionException {
      return this.gParser.comparatorTypeWithMultiCellTuple();
   }

   public CQL3Type.Raw comparatorTypeWithoutTuples() throws RecognitionException {
      return this.gParser.comparatorTypeWithoutTuples();
   }

   public Constants.Literal constant() throws RecognitionException {
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

   public CreateTableStatement.RawStatement createTableStatement() throws RecognitionException {
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

   public void customIndexExpression(WhereClause.Builder clause) throws RecognitionException {
      this.gParser.customIndexExpression(clause);
   }

   public DataResource dataResource() throws RecognitionException {
      return this.gParser.dataResource();
   }

   public Operation.RawDeletion deleteOp() throws RecognitionException {
      return this.gParser.deleteOp();
   }

   public List<Operation.RawDeletion> deleteSelection() throws RecognitionException {
      return this.gParser.deleteSelection();
   }

   public DeleteStatement.Parsed deleteStatement() throws RecognitionException {
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

   public FieldIdentifier fident() throws RecognitionException {
      return this.gParser.fident();
   }

   public Selectable.Raw fieldSelectorModifier(Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.fieldSelectorModifier(receiver);
   }

   public Maps.Literal fullMapLiteral() throws RecognitionException {
      return this.gParser.fullMapLiteral();
   }

   public Term.Raw function() throws RecognitionException {
      return this.gParser.function();
   }

   public List<Term.Raw> functionArgs() throws RecognitionException {
      return this.gParser.functionArgs();
   }

   public FunctionName functionName() throws RecognitionException {
      return this.gParser.functionName();
   }

   public FunctionResource functionResource() throws RecognitionException {
      return this.gParser.functionResource();
   }

   public Parser[] getDelegates() {
      return new Parser[]{this.gParser};
   }

   public String getGrammarFileName() {
      return "Cql.g";
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

   public void groupByClause(List<Selectable.Raw> groups) throws RecognitionException {
      this.gParser.groupByClause(groups);
   }

   public ColumnIdentifier ident() throws RecognitionException {
      return this.gParser.ident();
   }

   public void idxName(IndexName name) throws RecognitionException {
      this.gParser.idxName(name);
   }

   public AbstractMarker.INRaw inMarker() throws RecognitionException {
      return this.gParser.inMarker();
   }

   public Tuples.INRaw inMarkerForTuple() throws RecognitionException {
      return this.gParser.inMarkerForTuple();
   }

   public void indexIdent(List<IndexTarget.Raw> targets) throws RecognitionException {
      this.gParser.indexIdent(targets);
   }

   public IndexName indexName() throws RecognitionException {
      return this.gParser.indexName();
   }

   public ModificationStatement.Parsed insertStatement() throws RecognitionException {
      return this.gParser.insertStatement();
   }

   public Term.Raw intValue() throws RecognitionException {
      return this.gParser.intValue();
   }

   public JMXResource jmxResource() throws RecognitionException {
      return this.gParser.jmxResource();
   }

   public UpdateStatement.ParsedInsertJson jsonInsertStatement(CFName cf) throws RecognitionException {
      return this.gParser.jsonInsertStatement(cf);
   }

   public Json.Raw jsonValue() throws RecognitionException {
      return this.gParser.jsonValue();
   }

   public String keyspaceName() throws RecognitionException {
      return this.gParser.keyspaceName();
   }

   public void ksName(KeyspaceElementName name) throws RecognitionException {
      this.gParser.ksName(name);
   }

   public Term.Raw listLiteral() throws RecognitionException {
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

   public Term.Raw mapLiteral(Term.Raw k) throws RecognitionException {
      return this.gParser.mapLiteral(k);
   }

   public Tuples.Raw markerForTuple() throws RecognitionException {
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

   public void normalColumnOperation(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations, ColumnMetadata.Raw key) throws RecognitionException {
      this.gParser.normalColumnOperation(operations, key);
   }

   public UpdateStatement.ParsedInsert normalInsertStatement(CFName cf) throws RecognitionException {
      return this.gParser.normalInsertStatement(cf);
   }

   public void orderByClause(Map<ColumnMetadata.Raw, Boolean> orderings) throws RecognitionException {
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

   public void pkDef(CreateTableStatement.RawStatement expr) throws RecognitionException {
      this.gParser.pkDef(expr);
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
         this.pushFollow(FOLLOW_cqlStatement_in_query77);
         st = this.cqlStatement();
         --this.state._fsp;

         while(true) {
            int alt1 = 2;
            int LA1_0 = this.input.LA(1);
            if(LA1_0 == 204) {
               alt1 = 1;
            }

            switch(alt1) {
            case 1:
               this.match(this.input, 204, FOLLOW_204_in_query80);
               break;
            default:
               this.match(this.input, -1, FOLLOW_EOF_in_query84);
               stmnt = st;
               return stmnt;
            }
         }
      } catch (RecognitionException var8) {
         this.reportError(var8);
         this.recover(this.input, var8);
         return stmnt;
      } finally {
         ;
      }
   }

   public void recover(IntStream input, RecognitionException re) {
   }

   protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow) throws RecognitionException {
      throw new MismatchedTokenException(ttype, input);
   }

   public void relation(WhereClause.Builder clauses) throws RecognitionException {
      this.gParser.relation(clauses);
   }

   public void relationOrExpression(WhereClause.Builder clause) throws RecognitionException {
      this.gParser.relationOrExpression(clause);
   }

   public Operator relationType() throws RecognitionException {
      return this.gParser.relationType();
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

      try {
         try {
            this.pushFollow(FOLLOW_cassandraResource_in_resource109);
            c = this.cassandraResource();
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

   public IResource resourceFromInternalName() throws RecognitionException {
      return this.gParser.resourceFromInternalName();
   }

   public GrantPermissionsStatement restrictPermissionsStatement() throws RecognitionException {
      return this.gParser.restrictPermissionsStatement();
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

   public ColumnMetadata.Raw schema_cident() throws RecognitionException {
      return this.gParser.schema_cident();
   }

   public Cql_Parser.selectClause_return selectClause() throws RecognitionException {
      return this.gParser.selectClause();
   }

   public SelectStatement.RawStatement selectStatement() throws RecognitionException {
      return this.gParser.selectStatement();
   }

   public Selectable.Raw selectionAddition() throws RecognitionException {
      return this.gParser.selectionAddition();
   }

   public Selectable.Raw selectionFunction() throws RecognitionException {
      return this.gParser.selectionFunction();
   }

   public List<Selectable.Raw> selectionFunctionArgs() throws RecognitionException {
      return this.gParser.selectionFunctionArgs();
   }

   public Selectable.Raw selectionGroup() throws RecognitionException {
      return this.gParser.selectionGroup();
   }

   public Selectable.Raw selectionGroupWithField() throws RecognitionException {
      return this.gParser.selectionGroupWithField();
   }

   public Selectable.Raw selectionGroupWithoutField() throws RecognitionException {
      return this.gParser.selectionGroupWithoutField();
   }

   public Selectable.Raw selectionList() throws RecognitionException {
      return this.gParser.selectionList();
   }

   public Term.Raw selectionLiteral() throws RecognitionException {
      return this.gParser.selectionLiteral();
   }

   public Selectable.Raw selectionMap(Selectable.Raw k1) throws RecognitionException {
      return this.gParser.selectionMap(k1);
   }

   public Selectable.Raw selectionMapOrSet() throws RecognitionException {
      return this.gParser.selectionMapOrSet();
   }

   public Selectable.Raw selectionMultiplication() throws RecognitionException {
      return this.gParser.selectionMultiplication();
   }

   public Selectable.Raw selectionSet(Selectable.Raw t1) throws RecognitionException {
      return this.gParser.selectionSet(t1);
   }

   public Selectable.Raw selectionTupleOrNestedSelector() throws RecognitionException {
      return this.gParser.selectionTupleOrNestedSelector();
   }

   public Selectable.Raw selectionTypeHint() throws RecognitionException {
      return this.gParser.selectionTypeHint();
   }

   public RawSelector selector() throws RecognitionException {
      return this.gParser.selector();
   }

   public Selectable.Raw selectorModifier(Selectable.Raw receiver) throws RecognitionException {
      return this.gParser.selectorModifier(receiver);
   }

   public List<RawSelector> selectors() throws RecognitionException {
      return this.gParser.selectors();
   }

   public Term.Raw setLiteral(Term.Raw t) throws RecognitionException {
      return this.gParser.setLiteral(t);
   }

   public Term.Raw setOrMapLiteral(Term.Raw t) throws RecognitionException {
      return this.gParser.setOrMapLiteral(t);
   }

   public void shorthandColumnOperation(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations, ColumnMetadata.Raw key) throws RecognitionException {
      this.gParser.shorthandColumnOperation(operations, key);
   }

   public Selectable.Raw sident() throws RecognitionException {
      return this.gParser.sident();
   }

   public Term.Raw simpleTerm() throws RecognitionException {
      return this.gParser.simpleTerm();
   }

   public Selectable.Raw simpleUnaliasedSelector() throws RecognitionException {
      return this.gParser.simpleUnaliasedSelector();
   }

   public List<Term.Raw> singleColumnInValues() throws RecognitionException {
      return this.gParser.singleColumnInValues();
   }

   public Term.Raw term() throws RecognitionException {
      return this.gParser.term();
   }

   public Term.Raw termAddition() throws RecognitionException {
      return this.gParser.termAddition();
   }

   public Term.Raw termGroup() throws RecognitionException {
      return this.gParser.termGroup();
   }

   public Term.Raw termMultiplication() throws RecognitionException {
      return this.gParser.termMultiplication();
   }

   public TruncateStatement truncateStatement() throws RecognitionException {
      return this.gParser.truncateStatement();
   }

   public Tuples.Literal tupleLiteral() throws RecognitionException {
      return this.gParser.tupleLiteral();
   }

   public List<ColumnMetadata.Raw> tupleOfIdentifiers() throws RecognitionException {
      return this.gParser.tupleOfIdentifiers();
   }

   public List<Tuples.Raw> tupleOfMarkersForTuples() throws RecognitionException {
      return this.gParser.tupleOfMarkersForTuples();
   }

   public List<Tuples.Literal> tupleOfTupleLiterals() throws RecognitionException {
      return this.gParser.tupleOfTupleLiterals();
   }

   public List<CQL3Type.Raw> tuple_types() throws RecognitionException {
      return this.gParser.tuple_types();
   }

   public void typeColumns(CreateTypeStatement expr) throws RecognitionException {
      this.gParser.typeColumns(expr);
   }

   public void udtColumnOperation(List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> operations, ColumnMetadata.Raw key, FieldIdentifier field) throws RecognitionException {
      this.gParser.udtColumnOperation(operations, key, field);
   }

   public Selectable.Raw unaliasedSelector() throws RecognitionException {
      return this.gParser.unaliasedSelector();
   }

   public String unreserved_function_keyword() throws RecognitionException {
      return this.gParser.unreserved_function_keyword();
   }

   public String unreserved_keyword() throws RecognitionException {
      return this.gParser.unreserved_keyword();
   }

   public RevokePermissionsStatement unrestrictPermissionsStatement() throws RecognitionException {
      return this.gParser.unrestrictPermissionsStatement();
   }

   public List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> updateConditions() throws RecognitionException {
      return this.gParser.updateConditions();
   }

   public UpdateStatement.ParsedUpdate updateStatement() throws RecognitionException {
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

   public UserTypes.Literal usertypeLiteral() throws RecognitionException {
      return this.gParser.usertypeLiteral();
   }

   public void usingClause(Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClause(attrs);
   }

   public void usingClauseDelete(Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClauseDelete(attrs);
   }

   public void usingClauseObjective(Attributes.Raw attrs) throws RecognitionException {
      this.gParser.usingClauseObjective(attrs);
   }

   public Term.Raw value() throws RecognitionException {
      return this.gParser.value();
   }

   public WhereClause.Builder whereClause() throws RecognitionException {
      return this.gParser.whereClause();
   }
}
