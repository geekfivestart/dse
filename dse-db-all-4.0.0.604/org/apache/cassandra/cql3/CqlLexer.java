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
   public static final int Tokens = 216;
   List<Token> tokens;
   private final List<ErrorListener> listeners;
   public Cql_Lexer gLexer;
   protected CqlLexer.DFA1 dfa1;
   static final String DFA1_eotS = "\u0005\uffff\u0001\u0017\u0001\uffff\u0001\u0019\u0001\u001a\u0001\u001b\u0002\uffff\u0001\u001d\u0001\uffff\u0001\u001f\u0003\uffff\u0001\u0015\r\uffff\u0003\u0015\u0001\uffff";
   static final String DFA1_eofS = "$\uffff";
   static final String DFA1_minS = "\u0001\t\u0004\uffff\u0001=\u0001\uffff\u0001-\u0001.\u0001*\u0002\uffff\u0001=\u0001\uffff\u0001=\u0003\uffff\u0001x\r\uffff\u0001p\u0001r\u0001(\u0001\uffff";
   static final String DFA1_maxS = "\u0001}\u0004\uffff\u0001=\u0001\uffff\u0001n\u0001.\u0001/\u0002\uffff\u0001=\u0001\uffff\u0001=\u0003\uffff\u0001x\r\uffff\u0001p\u0001r\u0001(\u0001\uffff";
   static final String DFA1_acceptS = "\u0001\uffff\u0001\u0001\u0001\u0002\u0001\u0003\u0001\u0004\u0001\uffff\u0001\u0007\u0003\uffff\u0001\f\u0001\r\u0001\uffff\u0001\u0010\u0001\uffff\u0001\u0013\u0001\u0014\u0001\u0015\u0001\uffff\u0001\u0017\u0001\u0018\u0001\u0019\u0001\u0006\u0001\u0005\u0001\t\u0001\b\u0001\n\u0001\u000b\u0001\u000f\u0001\u000e\u0001\u0012\u0001\u0011\u0003\uffff\u0001\u0016";
   static final String DFA1_specialS = "$\uffff}>";
   static final String[] DFA1_transitionS = new String[]{"\u0002\u0015\u0002\uffff\u0001\u0015\u0012\uffff\u0001\u0015\u0001\u0001\u0001\u0015\u0001\uffff\u0001\u0015\u0001\u0002\u0001\uffff\u0001\u0015\u0001\u0003\u0001\u0004\u0001\u0010\u0001\u0005\u0001\u0006\u0001\u0007\u0001\b\u0001\t\n\u0015\u0001\n\u0001\u000b\u0001\f\u0001\r\u0001\u000e\u0001\u0015\u0001\uffff\u001a\u0015\u0001\u000f\u0001\uffff\u0001\u0011\u0003\uffff\u0004\u0015\u0001\u0012\u0015\u0015\u0001\u0013\u0001\uffff\u0001\u0014", "", "", "", "", "\u0001\u0016", "", "\u0001\u0015\u0002\uffff\n\u0015\u0003\uffff\u0001\u0018\u000b\uffff\u0001\u0015\u0004\uffff\u0001\u0015\u0001\uffff\u0001\u0015\u0018\uffff\u0001\u0015\u0004\uffff\u0001\u0015", "\u0001\u0015", "\u0001\u0015\u0004\uffff\u0001\u0015", "", "", "\u0001\u001c", "", "\u0001\u001e", "", "", "", "\u0001 ", "", "", "", "", "", "", "", "", "", "", "", "", "", "\u0001!", "\u0001\"", "\u0001#", ""};
   static final short[] DFA1_eot = DFA.unpackEncodedString("\u0005\uffff\u0001\u0017\u0001\uffff\u0001\u0019\u0001\u001a\u0001\u001b\u0002\uffff\u0001\u001d\u0001\uffff\u0001\u001f\u0003\uffff\u0001\u0015\r\uffff\u0003\u0015\u0001\uffff");
   static final short[] DFA1_eof = DFA.unpackEncodedString("$\uffff");
   static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars("\u0001\t\u0004\uffff\u0001=\u0001\uffff\u0001-\u0001.\u0001*\u0002\uffff\u0001=\u0001\uffff\u0001=\u0003\uffff\u0001x\r\uffff\u0001p\u0001r\u0001(\u0001\uffff");
   static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars("\u0001}\u0004\uffff\u0001=\u0001\uffff\u0001n\u0001.\u0001/\u0002\uffff\u0001=\u0001\uffff\u0001=\u0003\uffff\u0001x\r\uffff\u0001p\u0001r\u0001(\u0001\uffff");
   static final short[] DFA1_accept = DFA.unpackEncodedString("\u0001\uffff\u0001\u0001\u0001\u0002\u0001\u0003\u0001\u0004\u0001\uffff\u0001\u0007\u0003\uffff\u0001\f\u0001\r\u0001\uffff\u0001\u0010\u0001\uffff\u0001\u0013\u0001\u0014\u0001\u0015\u0001\uffff\u0001\u0017\u0001\u0018\u0001\u0019\u0001\u0006\u0001\u0005\u0001\t\u0001\b\u0001\n\u0001\u000b\u0001\u000f\u0001\u000e\u0001\u0012\u0001\u0011\u0003\uffff\u0001\u0016");
   static final short[] DFA1_special = DFA.unpackEncodedString("$\uffff}>");
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
      return "/home/jenkins/workspace/Util_DSEBuildGradle/dse-db/src/antlr/Cql__.g";
   }

   public final void mT__192() throws RecognitionException {
      int _type = 192;
      int _channel = 0;
      this.match("!=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__193() throws RecognitionException {
      int _type = 193;
      int _channel = 0;
      this.match(37);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__194() throws RecognitionException {
      int _type = 194;
      int _channel = 0;
      this.match(40);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__195() throws RecognitionException {
      int _type = 195;
      int _channel = 0;
      this.match(41);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__196() throws RecognitionException {
      int _type = 196;
      int _channel = 0;
      this.match(43);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__197() throws RecognitionException {
      int _type = 197;
      int _channel = 0;
      this.match("+=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__198() throws RecognitionException {
      int _type = 198;
      int _channel = 0;
      this.match(44);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__199() throws RecognitionException {
      int _type = 199;
      int _channel = 0;
      this.match(45);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__200() throws RecognitionException {
      int _type = 200;
      int _channel = 0;
      this.match("-=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__201() throws RecognitionException {
      int _type = 201;
      int _channel = 0;
      this.match(46);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__202() throws RecognitionException {
      int _type = 202;
      int _channel = 0;
      this.match(47);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__203() throws RecognitionException {
      int _type = 203;
      int _channel = 0;
      this.match(58);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__204() throws RecognitionException {
      int _type = 204;
      int _channel = 0;
      this.match(59);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__205() throws RecognitionException {
      int _type = 205;
      int _channel = 0;
      this.match(60);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__206() throws RecognitionException {
      int _type = 206;
      int _channel = 0;
      this.match("<=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__207() throws RecognitionException {
      int _type = 207;
      int _channel = 0;
      this.match(61);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__208() throws RecognitionException {
      int _type = 208;
      int _channel = 0;
      this.match(62);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__209() throws RecognitionException {
      int _type = 209;
      int _channel = 0;
      this.match(">=");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__210() throws RecognitionException {
      int _type = 210;
      int _channel = 0;
      this.match(91);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__211() throws RecognitionException {
      int _type = 211;
      int _channel = 0;
      this.match(42);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__212() throws RecognitionException {
      int _type = 212;
      int _channel = 0;
      this.match(93);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__213() throws RecognitionException {
      int _type = 213;
      int _channel = 0;
      this.match("expr(");
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__214() throws RecognitionException {
      int _type = 214;
      int _channel = 0;
      this.match(123);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public final void mT__215() throws RecognitionException {
      int _type = 215;
      int _channel = 0;
      this.match(125);
      this.state.type = _type;
      this.state.channel = _channel;
   }

   public void mTokens() throws RecognitionException {
      int alt1 = this.dfa1.predict(this.input);
      switch(alt1) {
      case 1:
         this.mT__192();
         break;
      case 2:
         this.mT__193();
         break;
      case 3:
         this.mT__194();
         break;
      case 4:
         this.mT__195();
         break;
      case 5:
         this.mT__196();
         break;
      case 6:
         this.mT__197();
         break;
      case 7:
         this.mT__198();
         break;
      case 8:
         this.mT__199();
         break;
      case 9:
         this.mT__200();
         break;
      case 10:
         this.mT__201();
         break;
      case 11:
         this.mT__202();
         break;
      case 12:
         this.mT__203();
         break;
      case 13:
         this.mT__204();
         break;
      case 14:
         this.mT__205();
         break;
      case 15:
         this.mT__206();
         break;
      case 16:
         this.mT__207();
         break;
      case 17:
         this.mT__208();
         break;
      case 18:
         this.mT__209();
         break;
      case 19:
         this.mT__210();
         break;
      case 20:
         this.mT__211();
         break;
      case 21:
         this.mT__212();
         break;
      case 22:
         this.mT__213();
         break;
      case 23:
         this.mT__214();
         break;
      case 24:
         this.mT__215();
         break;
      case 25:
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
         return "1:1: Tokens : ( T__192 | T__193 | T__194 | T__195 | T__196 | T__197 | T__198 | T__199 | T__200 | T__201 | T__202 | T__203 | T__204 | T__205 | T__206 | T__207 | T__208 | T__209 | T__210 | T__211 | T__212 | T__213 | T__214 | T__215 | Lexer. Tokens );";
      }
   }
}
