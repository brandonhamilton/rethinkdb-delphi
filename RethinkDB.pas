{============================================================================================================================}

Unit RethinkDB;
{
  RethinkDB Client Driver
  =======================

  The MIT License (MIT)

  Copyright (c) 2015 Brandon Hamilton

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
}

Interface

Uses System.Generics.Collections, SysUtils, Classes, SyncObjs, IdTCPClient, IdGlobal, DBXJson;

Const

  DRIVER_VERSION = '1.16.0';
  VERSION_V0_4   = $400c2d20;
  PROTOCOL_JSON  = $7e6970c7;

Type

  TConstArray = Array of TVarRec;

  TTermType = (
    TERMTYPE_DATUM = 1,
    TERMTYPE_MAKE_ARRAY = 2,
    TERMTYPE_MAKE_OBJ   = 3,
    TERMTYPE_VAR          = 10,
    TERMTYPE_JAVASCRIPT   = 11,
    TERMTYPE_UUID = 169,
    TERMTYPE_HTTP = 153,
    TERMTYPE_ERROR        = 12,
    TERMTYPE_IMPLICIT_VAR = 13,
    TERMTYPE_DB    = 14,
    TERMTYPE_TABLE = 15,
    TERMTYPE_GET   = 16,
    TERMTYPE_GET_ALL = 78,
    TERMTYPE_EQ  = 17,
    TERMTYPE_NE  = 18,
    TERMTYPE_LT  = 19,
    TERMTYPE_LE  = 20,
    TERMTYPE_GT  = 21,
    TERMTYPE_GE  = 22,
    TERMTYPE_NOT = 23,
    TERMTYPE_ADD = 24,
    TERMTYPE_SUB = 25,
    TERMTYPE_MUL = 26,
    TERMTYPE_DIV = 27,
    TERMTYPE_MOD = 28,
    TERMTYPE_FLOOR = 183,
    TERMTYPE_CEIL = 184,
    TERMTYPE_ROUND = 185,
    TERMTYPE_APPEND = 29,
    TERMTYPE_PREPEND = 80,
    TERMTYPE_DIFFERENCE = 95,
    TERMTYPE_SET_INSERT = 88,
    TERMTYPE_SET_INTERSECTION = 89,
    TERMTYPE_SET_UNION = 90,
    TERMTYPE_SET_DIFFERENCE = 91,
    TERMTYPE_SLICE  = 30,
    TERMTYPE_SKIP  = 70,
    TERMTYPE_LIMIT = 71,
    TERMTYPE_OFFSETS_OF = 87,
    TERMTYPE_CONTAINS = 93,
    TERMTYPE_GET_FIELD  = 31,
    TERMTYPE_KEYS = 94,
    TERMTYPE_OBJECT = 143,
    TERMTYPE_HAS_FIELDS = 32,
    TERMTYPE_WITH_FIELDS = 96,
    TERMTYPE_PLUCK    = 33,
    TERMTYPE_WITHOUT  = 34,
    TERMTYPE_MERGE    = 35,
    TERMTYPE_BETWEEN_DEPRECATED = 36,
    TERMTYPE_BETWEEN   = 182,
    TERMTYPE_REDUCE    = 37,
    TERMTYPE_MAP       = 38,
    TERMTYPE_FILTER    = 39,
    TERMTYPE_CONCAT_MAP = 40,
    TERMTYPE_ORDER_BY   = 41,
    TERMTYPE_DISTINCT  = 42,
    TERMTYPE_COUNT     = 43,
    TERMTYPE_IS_EMPTY = 86,
    TERMTYPE_UNION     = 44,
    TERMTYPE_NTH       = 45,
    TERMTYPE_BRACKET    = 170,
    TERMTYPE_INNER_JOIN = 48,
    TERMTYPE_OUTER_JOIN = 49,
    TERMTYPE_EQ_JOIN    = 50,
    TERMTYPE_ZIP        = 72,
    TERMTYPE_RANGE      = 173,
    TERMTYPE_INSERT_AT  = 82,
    TERMTYPE_DELETE_AT  = 83,
    TERMTYPE_CHANGE_AT  = 84,
    TERMTYPE_SPLICE_AT  = 85,
    TERMTYPE_COERCE_TO  = 51,
    TERMTYPE_TYPE_OF    = 52,
    TERMTYPE_UPDATE     = 53,
    TERMTYPE_DELETE     = 54,
    TERMTYPE_REPLACE    = 55,
    TERMTYPE_INSERT     = 56,
    TERMTYPE_DB_CREATE     = 57,
    TERMTYPE_DB_DROP       = 58,
    TERMTYPE_DB_LIST       = 59,
    TERMTYPE_TABLE_CREATE  = 60,
    TERMTYPE_TABLE_DROP    = 61,
    TERMTYPE_TABLE_LIST    = 62,
    TERMTYPE_CONFIG  = 174,
    TERMTYPE_STATUS  = 175,
    TERMTYPE_WAIT    = 177,
    TERMTYPE_RECONFIGURE   = 176,
    TERMTYPE_REBALANCE     = 179,
    TERMTYPE_SYNC          = 138,
    TERMTYPE_INDEX_CREATE = 75,
    TERMTYPE_INDEX_DROP   = 76,
    TERMTYPE_INDEX_LIST   = 77,
    TERMTYPE_INDEX_STATUS = 139,
    TERMTYPE_INDEX_WAIT = 140,
    TERMTYPE_INDEX_RENAME = 156,
    TERMTYPE_FUNCALL  = 64,
    TERMTYPE_BRANCH  = 65,
    TERMTYPE_OR      = 66,
    TERMTYPE_AND     = 67,
    TERMTYPE_FOR_EACH = 68,
    TERMTYPE_FUNC = 69,
    TERMTYPE_ASC = 73,
    TERMTYPE_DESC = 74,
    TERMTYPE_INFO = 79,
    TERMTYPE_MATCH = 97,
    TERMTYPE_UPCASE   = 141,
    TERMTYPE_DOWNCASE = 142,
    TERMTYPE_SAMPLE = 81,
    TERMTYPE_DEFAULT = 92,
    TERMTYPE_JSON = 98,
    TERMTYPE_TO_JSON_STRING = 172,
    TERMTYPE_ISO8601 = 99,
    TERMTYPE_TO_ISO8601 = 100,
    TERMTYPE_EPOCH_TIME = 101,
    TERMTYPE_TO_EPOCH_TIME = 102,
    TERMTYPE_NOW = 103,
    TERMTYPE_IN_TIMEZONE = 104,
    TERMTYPE_DURING = 105,
    TERMTYPE_DATE = 106,
    TERMTYPE_TIME_OF_DAY = 126,
    TERMTYPE_TIMEZONE = 127,
    TERMTYPE_YEAR = 128,
    TERMTYPE_MONTH = 129,
    TERMTYPE_DAY = 130,
    TERMTYPE_DAY_OF_WEEK = 131,
    TERMTYPE_DAY_OF_YEAR = 132,
    TERMTYPE_HOURS = 133,
    TERMTYPE_MINUTES = 134,
    TERMTYPE_SECONDS = 135,
    TERMTYPE_TIME = 136,
    TERMTYPE_MONDAY = 107,
    TERMTYPE_TUESDAY = 108,
    TERMTYPE_WEDNESDAY = 109,
    TERMTYPE_THURSDAY = 110,
    TERMTYPE_FRIDAY = 111,
    TERMTYPE_SATURDAY = 112,
    TERMTYPE_SUNDAY = 113,
    TERMTYPE_JANUARY = 114,
    TERMTYPE_FEBRUARY = 115,
    TERMTYPE_MARCH = 116,
    TERMTYPE_APRIL = 117,
    TERMTYPE_MAY = 118,
    TERMTYPE_JUNE = 119,
    TERMTYPE_JULY = 120,
    TERMTYPE_AUGUST = 121,
    TERMTYPE_SEPTEMBER = 122,
    TERMTYPE_OCTOBER = 123,
    TERMTYPE_NOVEMBER = 124,
    TERMTYPE_DECEMBER = 125,
    TERMTYPE_LITERAL = 137,
    TERMTYPE_GROUP = 144,
    TERMTYPE_SUM = 145,
    TERMTYPE_AVG = 146,
    TERMTYPE_MIN = 147,
    TERMTYPE_MAX = 148,
    TERMTYPE_SPLIT = 149,
    TERMTYPE_UNGROUP = 150,
    TERMTYPE_RANDOM = 151,
    TERMTYPE_CHANGES = 152,
    TERMTYPE_ARGS = 154,
    TERMTYPE_BINARY = 155,
    TERMTYPE_GEOJSON = 157,
    TERMTYPE_TO_GEOJSON = 158,
    TERMTYPE_POINT = 159,
    TERMTYPE_LINE = 160,
    TERMTYPE_POLYGON = 161,
    TERMTYPE_DISTANCE = 162,
    TERMTYPE_INTERSECTS = 163,
    TERMTYPE_INCLUDES = 164,
    TERMTYPE_CIRCLE = 165,
    TERMTYPE_GET_INTERSECTING = 166,
    TERMTYPE_FILL = 167,
    TERMTYPE_GET_NEAREST = 168,
    TERMTYPE_POLYGON_SUB = 171,
    TERMTYPE_MINVAL = 180,
    TERMTYPE_MAXVAL = 181
  );

  TResultType = (
    RESULT_JSON = 1,
    RESULT_STREAM
  );

  TRethinkDbConnection = Class;

  TRQLQuery = Class;
  TRQLEq  = Class;
  TRQLNe  = Class;
  TRQLLt  = Class;
  TRQLLe  = Class;
  TRQLGt  = Class;
  TRQLGe  = Class;
  TRQLAdd = Class;
  TRQLSub = Class;
  TRQLMul = Class;
  TRQLDiv = Class;
  TRQLMod = Class;
  TRQLAnd = Class;
  TRQLOr  = Class;
  TRQLNot = Class;
  TRQLFloor = Class;
  TRQLCeil = Class;
  TRQLRound = Class;
  TRQLContains = Class;
  TRQLHasFields = Class;
  TRQLWithFields = Class;
  TRQLKeys = Class;
  TRQLChanges = Class;
  TRQLPluck = Class;
  TRQLWithout = Class;
  TRQLFunCall = Class;
  TRQLDefault = Class;
  TRQLUpdate = Class;
  TRQLReplace = Class;
  TRQLDelete = Class;
  TRQLCoerceTo = Class;
  TRQLUngroup = Class;
  TRQLTypeOf = Class;
  TRQLMerge = Class;
  TRQLAppend = Class;
  TRQLPrepend = Class;
  TRQLDifference = Class;
  TRQLSetInsert = Class;
  TRQLSetUnion = Class;
  TRQLSetIntersection = Class;
  TRQLSetDifference = Class;
  TRQLGetField = Class;
  TRQLNth = Class;
  TRQLToJsonString = Class;
  TRQLMatch = Class;
  TRQLSplit = Class;
  TRQLUpcase = Class;
  TRQLDowncase = Class;
  TRQLIsEmpty = Class;
  TRQLOffsetsOf = Class;
  TRQLSlice = Class;
  TRQLSkip = Class;
  TRQLLimit = Class;
  TRQLReduce = Class;
  TRQLSum = Class;
  TRQLAvg = Class;
  TRQLMin = Class;
  TRQLMax = Class;
  TRQLMap = Class;
  TRQLFilter = Class;
  TRQLConcatMap = Class;
  TRQLOrderBy = Class;
  TRQLBetween = Class;
  TRQLDistinct = Class;
  TRQLCount = Class;
  TRQLUnion = Class;
  TRQLInnerJoin = Class;
  TRQLOuterJoin = Class;
  TRQLEqJoin = Class;
  TRQLZip = Class;
  TRQLGroup = Class;
  TRQLForEach = Class;
  TRQLInfo = Class;
  TRQLInsertAt = Class;
  TRQLSpliceAt = Class;
  TRQLDeleteAt = Class;
  TRQLChangeAt = Class;
  TRQLSample = Class;
  TRQLToISO8601 = Class;
  TRQLToEpochTime = Class;
  TRQLDuring = Class;
  TRQLDate = Class;
  TRQLTimeOfDay = Class;
  TRQLTimezone = Class;
  TRQLYear = Class;
  TRQLMonth = Class;
  TRQLDay = Class;
  TRQLDayOfWeek = Class;
  TRQLDayOfYear = Class;
  TRQLHours = Class;
  TRQLMinutes = Class;
  TRQLSeconds = Class;
  TRQLInTimezone = Class;
  TRQLToGeoJson = Class;
  TRQLDistance = Class;
  TRQLIntersects = Class;
  TRQLIncludes = Class;
  TRQLFill = Class;
  TRQLPolygonSub = Class;
  TRQLUserError = Class;
  TRQLTable = Class;
  TRQLBracket = Class;

  TRQLResult = Class(TObject)
  Private
    FType: TResultType;
    FProfile: TJSONValue;
  Protected
    Constructor Create( T: TResultType; P: TJSONValue = Nil );
  Public
    Class Function convertToDateTime( Const reqlObject: TJSONObject ): TDateTime; static;
    Class Function convertToBytes( Const reqlObject: TJSONObject ): TIdBytes; Overload; static;
    Class Procedure convertToBytes( Const reqlObject: TJSONObject; stream: TStream  ); Overload; static;
  End;

  TRQLQuery = Class(TObject)
  Private
    FTermType: TTermType;
    FTypeString: String;
    FArgs: Array of TRQLQuery;
    FOptArgs: Array of TPair<String,TRQLQuery>;
    Function Build: TJSONValue; virtual;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; virtual; abstract;
    Function getFieldAt( index: Integer ): TRQLBracket;
  Public
    Constructor Create( TermType: TTermType; TypeString: String = '' ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of TRQLQuery ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of TRQLQuery ); Overload;

    Function ToString: String;

    Function run( conn: TRethinkDbConnection ): TRQLResult; Overload;
    Function run( conn: TRethinkDbConnection; Const options: Array of Const ): TRQLResult; Overload;

    Function eq( Const other: TRQLQuery   ): TRQLEq; Overload;
    Function eq( Const other: Variant     ): TRQLEq; Overload;
    Function ne( Const other: TRQLQuery   ): TRQLNe; Overload;
    Function ne( Const other: Variant     ): TRQLNe; Overload;
    Function lt( Const other: TRQLQuery   ): TRQLLt; Overload;
    Function lt( Const other: Variant     ): TRQLLt; Overload;
    Function le( Const other: TRQLQuery   ): TRQLLe; Overload;
    Function le( Const other: Variant     ): TRQLLe; Overload;
    Function gt( Const other: TRQLQuery   ): TRQLGt; Overload;
    Function gt( Const other: Variant     ): TRQLGt; Overload;
    Function ge( Const other: TRQLQuery   ): TRQLGe; Overload;
    Function ge( Const other: Variant     ): TRQLGe; Overload;
    Function add( Const other: TRQLQuery  ): TRQLAdd; Overload;
    Function add( Const other: Variant    ): TRQLAdd; Overload;
    Function sub( Const other: TRQLQuery  ): TRQLSub; Overload;
    Function sub( Const other: Variant    ): TRQLSub; Overload;
    Function mul( Const other: TRQLQuery  ): TRQLMul; Overload;
    Function mul( Const other: Variant    ): TRQLMul; Overload;
    Function div_( Const other: TRQLQuery ): TRQLDiv; Overload;
    Function div_( Const other: Variant   ): TRQLDiv; Overload;
    Function mod_( Const other: TRQLQuery ): TRQLMod; Overload;
    Function mod_( Const other: Variant   ): TRQLMod; Overload;
    Function floor: TRQLFloor;
    Function ceil: TRQLCeil;
    Function round: TRQLRound;
    Function and_( Const other: TRQLQuery  ): TRQLAnd; Overload;
    Function and_( Const other: Variant    ): TRQLAnd; Overload;
    Function or_( Const other: TRQLQuery   ): TRQLOr; Overload;
    Function or_( Const other: Variant     ): TRQLOr; Overload;
    Function not_: TRQLNot;
    Function contains( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLContains; Overload;
    Function contains( Const f: TRQLQuery ): TRQLContains; Overload;
    Function hasFields( Const key: String ): TRQLHasFields; Overload;
    Function hasFields( Const keys: Array of Const ): TRQLHasFields; Overload;
    Function withFields( Const key: String ): TRQLWithFields; Overload;
    Function withFields( Const keys: Array of Const ): TRQLWithFields; Overload;
    Function keys: TRQLKeys;
    Function changes( Const squash: Boolean; includeStates: Boolean = False ): TRQLChanges; Overload;
    Function changes( Const squash: Double; includeStates: Boolean = False ): TRQLChanges; Overload;
    Function changes( Const includeStates: Boolean = False ): TRQLChanges; Overload;
    Function pluck( Const key: String ): TRQLPluck; Overload;
    Function pluck( Const keys: Array of Const ): TRQLPluck; Overload;
    Function without( Const key: String ): TRQLWithout; Overload;
    Function without( Const keys: Array of Const ): TRQLWithout; Overload;
    Function do_( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLFunCall; Overload;
    Function do_( Const f: TRQLQuery ): TRQLFunCall; Overload;
    Function default( Const q : TRQLQuery ): TRQLDefault; Overload;
    Function default( Const v : Variant )  : TRQLDefault; Overload;
    Function default( Const a : Array of Variant ): TRQLDefault; Overload;
    Function default( Const a : Array of Const ): TRQLDefault; Overload;
    Function default( Const a : TJSONValue ):  TRQLDefault; Overload;
    Function update( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLUpdate; Overload;
    Function update( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLUpdate; Overload;
    Function replace( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLReplace; Overload;
    Function replace( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLReplace; Overload;
    Function delete( durability: String = 'hard'; returnChanges : Boolean = False ): TRQLDelete;
    Function coerceTo( Const t: String ): TRQLCoerceTo;
    Function ungroup: TRQLUngroup;
    Function typeOf: TRQLTypeOf;
    Function merge( Const item: TRQLQuery ): TRQLMerge; Overload;
    Function merge( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMerge; Overload;
    Function append( Const q : TRQLQuery ): TRQLAppend; Overload;
    Function append( Const v : Variant )  : TRQLAppend; Overload;
    Function append( Const a : Array of Variant ): TRQLAppend; Overload;
    Function append( Const a : Array of Const ): TRQLAppend; Overload;
    Function append( Const a : TJSONValue ):  TRQLAppend; Overload;
    Function prepend( Const q : TRQLQuery ): TRQLPrepend; Overload;
    Function prepend( Const v : Variant )  : TRQLPrepend; Overload;
    Function prepend( Const a : Array of Variant ): TRQLPrepend; Overload;
    Function prepend( Const a : Array of Const ): TRQLPrepend; Overload;
    Function prepend( Const a : TJSONValue ):  TRQLPrepend; Overload;
    Function difference( Const q: TRQLQuery ): TRQLDifference; Overload;
    Function difference( Const q : Array of Const ): TRQLDifference; Overload;
    Function setInsert( Const q: TRQLQuery ): TRQLSetInsert; Overload;
    Function setInsert( Const q : Array of Const ): TRQLSetInsert; Overload;
    Function setUnion( Const q: TRQLQuery ): TRQLSetUnion; Overload;
    Function setUnion( Const q : Array of Const ): TRQLSetUnion; Overload;
    Function setIntersection( Const q: TRQLQuery ): TRQLSetIntersection; Overload;
    Function setIntersection( Const q : Array of Const ): TRQLSetIntersection; Overload;
    Function setDifference( Const q: TRQLQuery ): TRQLSetDifference; Overload;
    Function setDifference( Const q : Array of Const ): TRQLSetDifference; Overload;
    Function getField( Const key: String ): TRQLGetField;
    Function nth( Const index: Integer ): TRQLNth;
    Function toJson: TRQLToJsonString;
    Function toJsonString: TRQLToJsonString;
    Function match( Const regex: String ): TRQLMatch;
    Function split: TRQLSplit; Overload;
    Function split( maxSplits: Integer ): TRQLSplit; Overload;
    Function split( Const separator: String ): TRQLSplit; Overload;
    Function split( Const separator: String; maxSplits: Integer ): TRQLSplit; Overload;
    Function upcase: TRQLUpcase;
    Function downcase: TRQLDowncase;
    Function isEmpty: TRQLIsEmpty;
    Function offsetsOf( Const q : TRQLQuery ): TRQLOffsetsOf; Overload;
    Function offsetsOf( Const v : Variant )  : TRQLOffsetsOf; Overload;
    Function offsetsOf( Const a : Array of Variant ): TRQLOffsetsOf; Overload;
    Function offsetsOf( Const a : Array of Const ): TRQLOffsetsOf; Overload;
    Function offsetsOf( Const a : TJSONValue ):  TRQLOffsetsOf; Overload;
    Function offsetsOf( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLOffsetsOf; Overload;
    Function slice( Const startIndex: Integer; leftBound: String = 'closed'; rightBound: String = 'open' ): TRQLSlice;  Overload;
    Function slice( Const startIndex, endIndex: Integer; leftBound: String = 'closed'; rightBound: String = 'open' ): TRQLSlice;  Overload;
    Function skip( Const n: Integer ): TRQLSkip;
    Function limit( Const n: Integer ): TRQLLimit;
    Function reduce( Const f: TRQLQuery ): TRQLReduce; Overload;
    Function reduce( Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLReduce; Overload;
    Function sum( Const field: String ): TRQLSum; Overload;
    Function sum( Const f: TRQLQuery ): TRQLSum; Overload;
    Function sum( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLSum; Overload;
    Function avg( Const field: String ): TRQLAvg; Overload;
    Function avg( Const f: TRQLQuery ): TRQLAvg; Overload;
    Function avg( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLAvg; Overload;
    Function min( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLMin; Overload;
    Function min( Const f: TRQLQuery ): TRQLMin; Overload;
    Function min( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMin; Overload;
    Function max( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLMax; Overload;
    Function max( Const f: TRQLQuery ): TRQLMax; Overload;
    Function max( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMax; Overload;
    Function map( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMap; Overload;
    Function map( Const f: TRQLQuery ): TRQLMap; Overload;
    Function filter( Const f: TFunc<TRQLQuery, TRQLQuery>; default: Boolean = False ): TRQLFilter; Overload;
    Function filter( Const f: TRQLQuery; default: Boolean = False ): TRQLFilter; Overload;
    Function filter( Const f: TFunc<TRQLQuery, TRQLQuery>; default: TRQLUserError ): TRQLFilter; Overload;
    Function filter( Const f: TRQLQuery; default: TRQLUserError ): TRQLFilter; Overload;
    Function concatMap( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLConcatMap; Overload;
    Function concatMap( Const f: TRQLQuery ): TRQLConcatMap; Overload;
    Function orderBy( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLOrderBy; Overload;
    Function orderBy( Const keys: Array of String ): TRQLOrderBy; Overload;
    Function orderBy( Const keys: Array of Const ): TRQLOrderBy; Overload;
    Function between( Const lowerKey, upperKey: String; index: String = 'id'; leftBound: String = 'closed'; rightBound: String = 'open'): TRQLBetween;
    Function distinct: TRQLDistinct; Overload;
    Function distinct( Const index: String ): TRQLDistinct; Overload;
    Function count( Const q : TRQLQuery ): TRQLCount; Overload;
    Function count( Const v : Variant )  : TRQLCount; Overload;
    Function count( Const a : Array of Variant ): TRQLCount; Overload;
    Function count( Const a : Array of Const ): TRQLCount; Overload;
    Function count( Const a : TJSONValue ):  TRQLCount; Overload;
    Function count( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLCount; Overload;
    Function union( Const sequence: TRQLQuery ): TRQLUnion; Overload;
    Function union( Const sequences: Array of Const ): TRQLUnion; Overload;
    Function union( Const sequences: Array of TRQLQuery ): TRQLUnion; Overload;
    Function innerJoin( Const other: TRQLQuery; Const predicate: TFunc<TRQLQuery, TRQLQuery> ): TRQLInnerJoin; Overload;
    Function innerJoin( Const other: TRQLQuery; Const predicate: TRQLQuery ): TRQLInnerJoin; Overload;
    Function outerJoin( Const other: TRQLQuery; Const predicate: TFunc<TRQLQuery, TRQLQuery> ): TRQLOuterJoin; Overload;
    Function outerJoin( Const other: TRQLQuery; Const predicate: TRQLQuery ): TRQLOuterJoin; Overload;
    Function eqJoin( Const leftField: String; Const rightTable: TRQLTable; Const index: String = 'id' ): TRQLEqJoin;
    Function zip: TRQLZip;
    Function group( Const q : TRQLQuery; Const index: String = ''; Const multi: Boolean = False ): TRQLGroup; Overload;
    Function group( Const v : Variant; Const index: String = ''; Const multi: Boolean = False )  : TRQLGroup; Overload;
    Function group( Const a : Array of Variant; Const index: String = ''; Const multi: Boolean = False ): TRQLGroup; Overload;
    Function group( Const a : Array of Const; Const index: String = ''; Const multi: Boolean = False ): TRQLGroup; Overload;
    Function group( Const a : TJSONValue; Const index: String = ''; Const multi: Boolean = False ):  TRQLGroup; Overload;
    Function group( Const f: TFunc<TRQLQuery, TRQLQuery>; Const index: String = ''; Const multi: Boolean = False ): TRQLGroup; Overload;
    Function forEach( Const f: TRQLQuery ): TRQLForEach; Overload;
    Function forEach( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLForEach; Overload;
    Function info: TRQLInfo;
    Function insertAt( Const index: Integer; Const value : TRQLQuery ): TRQLInsertAt; Overload;
    Function insertAt( Const index: Integer; Const value : Variant )  : TRQLInsertAt; Overload;
    Function insertAt( Const index: Integer; Const value : Array of Variant ): TRQLInsertAt; Overload;
    Function insertAt( Const index: Integer; Const value : Array of Const ): TRQLInsertAt; Overload;
    Function insertAt( Const index: Integer; Const value : TJSONValue ):  TRQLInsertAt; Overload;
    Function spliceAt( Const index: Integer; Const value : TRQLQuery ): TRQLSpliceAt; Overload;
    Function spliceAt( Const index: Integer; Const value : Array of Variant ): TRQLSpliceAt; Overload;
    Function spliceAt( Const index: Integer; Const value : Array of Const ): TRQLSpliceAt; Overload;
    Function deleteAt( Const index: Integer ): TRQLDeleteAt; Overload;
    Function deleteAt( Const index, endIndex: Integer ): TRQLDeleteAt; Overload;
    Function changeAt( Const index: Integer; Const value : TRQLQuery ): TRQLChangeAt; Overload;
    Function changeAt( Const index: Integer; Const value : Variant )  : TRQLChangeAt; Overload;
    Function changeAt( Const index: Integer; Const value : Array of Variant ): TRQLChangeAt; Overload;
    Function changeAt( Const index: Integer; Const value : Array of Const ): TRQLChangeAt; Overload;
    Function changeAt( Const index: Integer; Const value : TJSONValue ):  TRQLChangeAt; Overload;
    Function sample( Const n: Integer ): TRQLSample;
    Function toISO8601: TRQLToISO8601;
    Function toEpochTime: TRQLToEpochTime;
    Function during( Const startTime, endTime: TRQLQuery; leftBound: String = 'closed'; rightBound: String = 'open'): TRQLDuring;
    Function date: TRQLDate;
    Function timeOfDay: TRQLTimeOfDay;
    Function timezone: TRQLTimezone;
    Function year: TRQLYear;
    Function month: TRQLMonth;
    Function day: TRQLDay;
    Function dayOfWeek: TRQLDayOfWeek;
    Function dayOfYear: TRQLDayOfYear;
    Function hours: TRQLHours;
    Function minutes: TRQLMinutes;
    Function seconds: TRQLSeconds;
    Function inTimezone( Const timezone: String ): TRQLInTimezone;
    Function toGeoJson: TRQLToGeoJson;
    Function distance( Const geometry: TRQLQuery; geoSystem: String = 'WGS84'; geoUnit: String = 'm' ) : TRQLDistance;
    Function intersects( Const geometry: TRQLQuery ): TRQLIntersects;
    Function includes( Const geometry: TRQLQuery ): TRQLIncludes;
    Function fill: TRQLFill;
    Function polygonSub( Const polygon: TRQLQuery ): TRQLPolygonSub;
    Property fields[index: Integer]: TRQLBracket Read getFieldAt; Default;
  End;

  TRQLTopLevelQuery = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLMethodQuery = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLFunc = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( Func: TFunc<TRQLQuery> ); Overload;
    Constructor Create( Func: TFunc<TRQLQuery, TRQLQuery> ); Overload;
    Constructor Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ); Overload;
    Constructor Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ); Overload;
    Constructor Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ); Overload;
  End;

  TRQLFunCall = Class(TRQLQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Args: Array of TRQLQuery ); Overload;
    Constructor Create( Const Args: Array of TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery> ); Overload;
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLBracketQuery = Class(TRQLMethodQuery)
  Private
    FBracketOperator : Boolean;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; BracketOperator: Boolean ); Overload;
    Constructor Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; Const OptArgs: Array of Const; BracketOperator: Boolean  );  Overload;
  End;

  TRQLBooleanOperatorQuery = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLOr  = Class(TRQLBooleanOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLAnd = Class(TRQLBooleanOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const);
  End;

  TRQLNot = Class(TRQLQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLBinaryOperatorQuery = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLBinaryCompareOperatorQuery = Class(TRQLBinaryOperatorQuery);

  TRQLEq = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLNe = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLLt = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLLe = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGt = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGe = Class (TRQLBinaryCompareOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLAdd = Class (TRQLBinaryOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSub = Class (TRQLBinaryOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMul = Class (TRQLBinaryOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDiv = Class (TRQLBinaryOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMod = Class (TRQLBinaryOperatorQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLFloor = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLCeil = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const);
  End;

  TRQLRound = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLAppend = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLPrepend = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDifference = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSetInsert = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSetUnion = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSetIntersection = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSetDifference = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGetField = Class(TRQLBracketQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSkip = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLLimit = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSlice = Class(TRQLBracketQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLBracket = Class(TRQLBracketQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLContains = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLHasFields = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const obj: TRQLQuery; Const Args: Array of Const );
  End;

  TRQLWithFields = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const obj: TRQLQuery; Const Args: Array of Const );
  End;

  TRQLKeys = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLObject = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLPluck = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const obj: TRQLQuery; Const Args: Array of Const );
  End;

  TRQLWithout = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const obj: TRQLQuery; Const Args: Array of Const );
  End;

  TRQLMerge = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLBetween = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLGet = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGetAll = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of String ); Overload;
  End;

  TRQLGetIntersecting = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLGetNearest = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLReduce = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSum = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLAvg = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMin = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLMax = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLMap = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Args: Array of TRQLQuery ); Overload;
  End;

  TRQLFilter = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLConcatMap = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLOrderBy = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of String ); Overload;
  End;

  TRQLDistinct = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLCount = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUnion = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of TRQLQuery ); Overload;
  End;

  TRQLNth = Class(TRQLBracketQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMatch = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLToJSONString = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSplit = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUpcase = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDowncase = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLOffsetsOf = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLIsEmpty = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGroup = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLInnerJoin = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLOuterJoin = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLEqJoin = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLZip = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLCoerceTo = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUngroup = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTypeOf = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUpdate = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLDelete = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLReplace = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLInsert = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of TJSONValue; Const OptArgs: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const ); Overload;
  End;

  TRQLIndexCreate = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLIndexDrop = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLIndexRename = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLIndexList = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLIndexStatus = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of String ); Overload;
  End;

  TRQLIndexWait = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of Const ); Overload;
    Constructor Create( Const Obj: TRQLQuery; Const Args: Array of String ); Overload;
  End;

  TRQLForEach = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLInsertAt = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSpliceAt = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDeleteAt = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLChangeAt = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSample = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLRange = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLToISO8601 = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDuring = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLDate = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTimeOfDay = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTimezone = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLYear = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMonth = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDay = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDayOfWeek = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDayOfYear = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLHours = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLMinutes = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLSeconds = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTime = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLISO8601 = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLEpochTime = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLNow = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLInTimezone = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLToEpochTime = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLGeoJson = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLToGeoJson = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLPoint = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLLine = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Args: Array of TRQLQuery ); Overload;
  End;

  TRQLPolygon = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const ); Overload;
    Constructor Create( Const Args: Array of TRQLQuery ); Overload;
  End;

  TRQLDistance = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLIntersects = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLIncludes = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLCircle = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLFill = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLPolygonSub = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLAsc = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLDesc = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLLiteral = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLDatum = Class(TRQLQuery)
  Private
    FValue: TJSONValue;
    Function Build: TJSONValue; Override;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( Value: TJSONValue );
  End;

  TRQLMakeObj = Class(TRQLQuery)
  Private
    Function Build: TJSONValue; Override;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( Const Obj: TDictionary<String, TRQLQuery> ); Overload;
    Constructor Create( Const Obj: TJSONObject ); Overload;
    Constructor Create( Const Arr: Array of Const ); Overload;
  End;

  TRQLMakeArray = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create; Overload;
    Constructor Create( Const Args: Array of TRQLQuery ); Overload;
    Constructor Create( Const Arr: TJSONArray ); Overload;
    Constructor Create( Const Arr: Array of Variant ); Overload;
    Constructor Create( Const Arr: Array of Const ); Overload;
  End;

  TRQLVar = Class(TRQLQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLImplicitVar = Class(TRQLQuery)
  Public
    Constructor Create; Overload;
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const ); Overload;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TRQLTableCreate = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLTableCreateTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLTableDrop = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTableDropTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTableList = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLTableListTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUUID = Class(TRQLMethodQuery)
  Public
    Constructor Create; Overload;
    Constructor Create( Const Args: Array of Const ); Overload;
  End;

  TRQLArgs = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLBinary = Class(TRQLTopLevelQuery)
  Private
    FData: RawByteString;
    Function Build: TJSONValue; Override;
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  Public
    Constructor Create( Stream: TStream ); Overload;
    Constructor Create( Data  : TIdBytes ); Overload;
    Constructor Create( Data  : TRQLQuery ); Overload;
    Constructor Create( Data  : String ); Overload;
  End;

  TRQLJavascript = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLHTTP = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLJSON = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLUserError = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLRandom = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLChanges = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLDefault = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLInfo = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLConfig = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLStatus = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLWait = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLWaitTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLReconfigure = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLReconfigureTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLRebalance = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLRebalanceTL = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLSync = Class(TRQLMethodQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLBranch = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );
  End;

  TRQLTable = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const; Const OptArgs: Array of Const );

    Function insert( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert; Overload;
    Function insert( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert; Overload;
    Function insert( Const items: Array of Const; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert; Overload;
    Function insert( Const items: Array of TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert; Overload;
    Function insert( Const items: Array of TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert; Overload;
    Function get( Const key: String ): TRQLGet;
    Function getAll( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLGetAll; Overload;
    Function getAll( Const keys: Array of Const ): TRQLGetAll; Overload;
    Function getAll( Const keys: Array of String ): TRQLGetAll; Overload;
    Function indexCreate( Const indexName: String; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate; Overload;
    Function indexCreate( Const indexName: String; Const f : TFunc<TRQLQuery, TRQLQuery>; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate; Overload;
    Function indexCreate( Const indexName: String;  Const f : TRQLQuery; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate; Overload;
    Function indexDrop( Const indexName: String ): TRQLIndexDrop;
    Function indexRename( Const oldName, newName: String; Const overwrite: Boolean = False ): TRQLIndexRename;
    Function indexList: TRQLIndexList;
    Function indexStatus: TRQLIndexStatus; Overload;
    Function indexStatus( Const index: String ): TRQLIndexStatus; Overload;
    Function indexStatus( Const indexes: Array of Const ): TRQLIndexStatus; Overload;
    Function indexStatus( Const indexes: Array of String ): TRQLIndexStatus; Overload;
    Function indexWait: TRQLIndexWait; Overload;
    Function indexWait( Const index: String ): TRQLIndexWait; Overload;
    Function indexWait( Const indexes: Array of Const ): TRQLIndexWait; Overload;
    Function indexWait( Const indexes: Array of String ): TRQLIndexWait; Overload;
    Function status: TRQLStatus;
    Function config: TRQLConfig;
    Function wait( Const wait_for : String = 'ready_for_writes'; Const timeout: Integer = -1 ): TRQLWait;
    Function reconfigure( Const shards: Integer; Const replicas: Integer; Const dryRun : Boolean = False ): TRQLReconfigure; Overload;
    Function reconfigure( Const shards: Integer; Const replicas: Array of Const; Const primaryReplicaTag: String; Const dryRun : Boolean = False ): TRQLReconfigure; Overload;
    Function rebalance: TRQLRebalance;
    Function sync: TRQLSync;
    Function getIntersecting( Const geometry: TRQLQuery; Const index: String ): TRQLGetIntersecting;
    Function getNearest( Const point: TRQLQuery; Const index: String; Const maxResults: Integer = 100; Const maxDist: Integer = 100000; Const geoSystem: String = 'WGS84'; Const geoUnit: String = 'm'): TRQLGetNearest;
    Function uuid: TRQLUUID;
  End;

  TRQLDB = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );

    Function table( Const name: String ): TRQLTable; Overload;
    Function table( Const name: String; Const options: Array of Const ): TRQLTable; Overload;
    Function tableCreate( Const tableName: String; Const options: Array of Const ): TRQLTableCreate;
    Function tableDrop( Const tableName : String ): TRQLTableDrop;
    Function tableList: TRQLTableList;

    Function config: TRQLConfig;
    Function rebalance: TRQLRebalance;
    Function reconfigure(Const shards: Integer; Const replicas: Integer; Const dry_run : Boolean = False ): TRQLReconfigure; Overload;
    Function reconfigure(Const shards: Integer; Const replicas: Array of Const; Const primary_replica_tag: String; Const dry_run : Boolean = False ): TRQLReconfigure; Overload;
    Function wait(Const wait_for : String = 'ready_for_writes'; Const timeout: Integer = -1): TRQLWait;

  End;

  TRQLDBCreate = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDBDrop = Class(TRQLTopLevelQuery)
  Public
    Constructor Create( Const Args: Array of Const );
  End;

  TRQLDBList = Class(TRQLTopLevelQuery)
  Public
    Constructor Create;
  End;

  TRQLConstant = Class(TRQLQuery)
  Private
    Function Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String; Override;
  End;

  TFrameType = (
    FRAMETYPE_POS = 1,
    FRAMETYPE_OPT
  );

  TRethinkDbFrame = Class(TObject)
  Private
    FType     : TFrameType;
    FPosition : Int64;
    FOptional : UTF8String;
  Public
    Constructor Create( FT: TFrameType; Pos: Int64; Opt: UTF8String );
    Property FrameType: TFrameType Read FType;
    Property Position : Int64 Read FPosition;
    Property Optional : UTF8String Read FOptional;
  End;

  TRethinkDbBacktrace = Class(TObject)
  Private
    FFrames: Array of TRethinkDbFrame;
  End;

  ERQLError               = Class(Exception);
  ERQLDriverError         = Class(ERQLError);
  ERQLTimeoutError        = Class(ERQLError)
  Public
    Constructor Create;
  End;

  ERQLQueryError          = Class(ERQLError)
  Private
    FFrames: TRethinkDbBacktrace;
    FTerm  : TRQLQuery;
  Public
    Constructor Create(Const Msg: String; Const Term: TRQLQuery; Frames: TRethinkDbBacktrace);
  End;
  ERQLClientError         = Class(ERQLQueryError);
  ERQLCompileError        = Class(ERQLQueryError);
  ERQLRuntimeError        = Class(ERQLQueryError);
  ERQLCursorEmpty         = Class(ERQLQueryError)
  Public
    Constructor Create( Term: TRQLQuery );
  End;

  TQueryType = (
    QUERY_START = 1,
    QUERY_CONTINUE,
    QUERY_STOP,
    QUERY_NOREPLY_WAIT
  );

  TRethinkDbQuery = Class(TObject)
  Private
    FType   : TQueryType;
    FToken  : Int64;
    FTerm   : TRQLQuery;
    FOptions: TJSONObject;
  Public
    Constructor Create( QueryType: TQueryType; Token: Int64; Term: TRQLQuery = Nil); Overload;
    Constructor Create( QueryType: TQueryType; Token: Int64; Term: TRQLQuery; Options: TJSONObject); Overload;
    Destructor  Destroy; override;
    Procedure   Serialize( S: TStream );

    Property Token: Int64 Read FToken;
    Property Term : TRQLQuery Read FTerm;
    Property Options: TJSONObject Read FOptions;
  End;

  TResponseType = (
    RESPONSE_SUCCESS_ATOM = 1,
    RESPONSE_SUCCESS_SEQUENCE,
    RESPONSE_SUCCESS_PARTIAL,
    RESPONSE_WAIT_COMPLETE,
    RESPONSE_SUCCESS_FEED,
    RESPONSE_CLIENT_ERROR = 16,
    RESPONSE_COMPILE_ERROR,
    RESPONSE_RUNTIME_ERROR
  );

  TRethinkDbResponse = Class(TObject)
  Private
    FToken     : Int64;
    FType      : TResponseType;
    FData      : TJSONArray;
    FBacktrace : TRethinkDbBacktrace;
    FProfile   : TJSONValue;
  Public
    Constructor Create( Token: Int64; Response: UTF8String );
    Destructor  Destroy; override;
    Function    MakeError( Query: TRethinkDbQuery ): ERQLError;
    Property    ResponseType: TResponseType read FType;
    Property    Data: TJSONArray read FData;
    Property    Backtrace: TRethinkDbBacktrace read FBacktrace;
    Property    Profile: TJSONValue read FProfile;
    Property    Token: Int64 read FToken;
  End;

  TRethinkDbCursor = Class;

  TRethinkDbConnection = Class(TObject)
  Private
    FSocket    : TIdTCPClient;
    FData      : TMemoryStream;
    FLock      : TCriticalSection;
    FClosing   : Boolean;
    FNextToken : Int64;

    FHost      : String;
    FPort      : Word;
    FDb        : String;
    FAuthKey   : UTF8String;
    FConnectTimeout : LongInt;

    FCursorCache : TDictionary<Int64, TRethinkDbCursor>;

    Function GetNewToken: Int64;

    Function RunQuery( Query: TRethinkDbQuery; NoReply: Boolean ): TRQLResult;
    Function GetResponse( Token: Int64 ): TRethinkDbResponse;

    Function DoStart   ( Term: TRQLQuery; Options:  TJSONObject ): TRQLResult;
    Function DoContinue( Cursor: TRethinkDbCursor ): TRQLResult;
    Function DoStop    ( Cursor: TRethinkDbCursor ): TRQLResult;

    Procedure close( Const noreply_wait: Boolean; Token: Int64); Overload;

    Function IsOpen: Boolean;
    Procedure CheckOpen;
    Procedure Connect;

  Public
    Constructor Create( Const host: String; Const port: Word; Const db: String; Const auth_key: String; Const timeout: LongInt );
    Destructor Destroy; Override;

    Procedure close( Const noreply_wait: Boolean = True ); Overload;

    Procedure reconnect( Const noreply_wait: Boolean = True );
    Procedure use( Const db_name: String );
    Function  noreply_wait: TRQLResult;

    Property Closing: Boolean Read FClosing;
    Property Connected: Boolean Read IsOpen;
    Property DB: String Read FDb;
    Property Port: Word Read FPort;
    Property Host: String Read FHost;
  End;

  TRethinkDbCursorEnumerator = Class(TObject)
  Private
    FCursor: TRethinkDbCursor;
    FCurrentItem: TJSONValue;
  Public
    Constructor Create( Cursor: TRethinkDbCursor );
    Function GetCurrent: TJSONValue;
    Function MoveNext: Boolean;
    Property Current: TJSONValue Read GetCurrent;
  End;


  TRQLDocument = Class(TRQLResult)
  Private
    FValue : TJSONValue;
  Public
    Constructor Create( Value: TJSONValue; Profile: TJSONValue = Nil );
    Property Value: TJSONValue Read FValue;
  End;

  TRethinkDbCursor = Class(TRQLResult)
  Private
    FConnection : TRethinkDbConnection;
    FQuery      : TRethinkDbQuery;

    FItems               : TJSONArray;
    FOutstandingRequests : Byte;
    FThreshold           : Word;
    FError               : ERQLError;

    Procedure MaybeFetchBatch;
    Procedure AddResponse( Response: TRethinkDbResponse );
    Procedure SetError( Msg: String );
    Function  GetNext(Timeout : LongInt = -1): TJSONValue;
  Public
    Constructor Create( Connection: TRethinkDbConnection; Query: TRethinkDbQuery; Profile: TJSONValue = Nil );
    Destructor  Destroy; override;

    Function  next(Const wait: Boolean = True): TJSONValue; Overload;
    Function  next(Const timeout: Integer): TJSONValue; Overload;
    Procedure close;

    Function GetEnumerator: TRethinkDbCursorEnumerator;

    Property Query: TRethinkDbQuery Read FQuery;
    Property Connection: TRethinkDbConnection Read FConnection;
  End;

  TRethinkDB = Class(TObject)
  Public
    Class Function connect(host: String = 'localhost'; port: Word = 28015; db: String = 'test'; auth_key: String = ''; timeout: LongInt = 20): TRethinkDbConnection; static;

    Class Function expr(Const q : TRQLQuery): TRQLQuery; Overload; static;
    Class Function expr(Const v : Variant)  : TRQLQuery; Overload; static;
    Class Function expr(Const a : Array of Variant): TRQLQuery; Overload; static;
    Class Function expr(Const a : Array of Const): TRQLQuery; Overload; static;
    Class Function expr(Const a : TJSONValue):  TRQLQuery; Overload; static;
    Class Function expr(Const f : TFunc<TRQLQuery>): TRQLQuery; Overload; static;
    Class Function expr(Const f : TFunc<TRQLQuery, TRQLQuery>): TRQLQuery; Overload; static;
    Class Function expr(Const f : TFunc<TRQLQuery, TRQLQuery, TRQLQuery>): TRQLQuery; Overload; static;
    Class Function expr(Const f : TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery>): TRQLQuery; Overload; static;
    Class Function expr(Const f : TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery>): TRQLQuery; Overload; static;
    Class Function expr(Const v : TVarRec) : TRQLQuery; Overload; static;
    Class Function expr(Const b; Count: Longint): TRQLQuery; Overload; static;
    Class Function expr(Const b : TArray<Byte>): TRQLQuery; Overload; static;
    Class Function expr(Const s : TStream): TRQLQuery; Overload; static;
    Class Function expr(Const d : TDateTime): TRQLQuery; Overload; static;
    Class Function expr(Const d : TTime): TRQLQuery; Overload; static;
    Class Function expr(Const d : TDate): TRQLQuery; Overload; static;

    Class Function makeArray(Const a : Array of Const): TRQLQuery; static;
    Class Function makeObject(Const a : Array of Const): TRQLQuery; static;

    Class Function js( Const jsString: String ): TRQLJavascript; Overload;  static;
    Class Function js( Const jsString: String; Const options: Array of Const): TRQLJavascript; Overload; static;
    Class Function http( Const url: String ): TRQLHTTP; Overload; static;
    Class Function http( Const url: String; Const options: Array of Const): TRQLHTTP; Overload; static;
    Class Function http( Const url: TRQLQuery ): TRQLHTTP; Overload; static;
    Class Function http( Const url: TRQLQuery; Const options: Array of Const): TRQLHTTP; Overload; static;
    Class Function json( Const jsonString: String ): TRQLJSON; static;
    Class Function args( Const arr : Array of Variant ): TRQLArgs; static;
    Class Function error( Const msg: String ): TRQLUserError; static;
    Class Function random: TRQLRandom; Overload; static;
    Class Function random( upper: Integer ): TRQLRandom; Overload; static;
    Class Function random( lower: Integer; upper: Integer): TRQLRandom; Overload; static;
    Class Function random( upper: Double ): TRQLRandom; Overload; static;
    Class Function random( lower: Double; upper: Double ): TRQLRandom; Overload; static;
    Class Function do_( Const f: TFunc<TRQLQuery> ): TRQLFunCall; Overload; static;
    Class Function do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLFunCall; Overload; static;
    Class Function do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall; Overload; static;
    Class Function do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall; Overload; static;
    Class Function do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall; Overload; static;
    Class Function row: TRQLImplicitVar; static;
    Class Function branch( Const test, trueBranch, falseBranch: TRQLQuery ): TRQLBranch; static;

    Class Function map( Const sequence: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMap; Overload; static;
    Class Function map( Const sequence1, sequence2: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap; Overload; static;
    Class Function map( Const sequence1, sequence2, sequence3: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap; Overload; static;
    Class Function map( Const sequence1, sequence2, sequence3, sequence4: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap; Overload; static;
    Class Function map( Const sequence, f: TRQLQuery ): TRQLMap; Overload; static;
    Class Function map( Const args: Array of TRQLQuery ): TRQLMap; Overload; static;
    Class Function object_( Const a: Array of Const ): TRQLObject; static;
    Class Function binary( Const stream: TStream ): TRQLBinary; Overload; static;
    Class Function binary( Const data: TIdBytes ): TRQLBinary; Overload; static;
    Class Function binary( Const data : UTF8String ): TRQLBinary; Overload; static;
    Class Function binary( Const data : TRQLQuery ): TRQLBinary; Overload; static;
    Class Function uuid: TRQLUUID; static;
    Class Function typeOf: TRQLTypeOf; static;
    Class Function info: TRQLInfo; static;
    Class Function range: TRQLRange; Overload; static;
    Class Function range( endValue: Integer ): TRQLRange; Overload; static;
    Class Function range( startValue, endValue: Integer ): TRQLRange; Overload; static;
    Class Function literal( Const a: Array of Const ): TRQLLiteral; static;

    Class Function asc( Const key: String ): TRQLAsc; Overload; static;
    Class Function asc( Const f: TRQLQuery ): TRQLAsc; Overload; static;
    Class Function asc( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLAsc; Overload; static;
    Class Function desc( Const key: String ): TRQLDesc; Overload; static;
    Class Function desc( Const f: TRQLQuery ): TRQLDesc; Overload; static;
    Class Function desc( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLDesc; Overload; static;

    Class Function db( Const dbName: String ): TRQLDB;  static;
    Class Function dbCreate( Const dbName: String ): TRQLQuery;  static;
    Class Function dbDrop( Const dbName: String ): TRQLQuery;  static;
    Class Function dbList: TRQLQuery; static;

    Class Function table( Const name: String ): TRQLTable; Overload; static;
    Class Function table( Const name: String; Const options: Array of Const): TRQLTable; Overload; static;
    Class Function tableCreate( Const tableName: String ): TRQLTableCreateTL; Overload; static;
    Class Function tableCreate( Const tableName: String; Const options: Array of Const ): TRQLTableCreateTL; Overload; static;
    Class Function tableDrop( Const tableName : String ): TRQLTableDropTL; static;
    Class Function tableList: TRQLTableListTL; static;

    Class Function wait(Const waitFor : String = 'ready_for_writes'; Const timeout: Integer = -1): TRQLWaitTL; static;
    Class Function reconfigure(Const shards: Integer; Const replicas: Integer; Const dryRun : Boolean = False ): TRQLReconfigureTL; Overload; static;
    Class Function reconfigure(Const shards: Integer; Const replicas: Array of Const; Const primaryReplicaTag: String; Const dryRun : Boolean = False ): TRQLReconfigureTL; Overload; static;
    Class Function rebalance: TRQLRebalanceTL; static;

    Class Function eq( Const first, second: TRQLQuery ): TRQLEq; Overload; static;
    Class Function eq( Const first, second: Variant   ): TRQLEq; Overload; static;
    Class Function eq( Const args: TRQLArgs           ): TRQLEq; Overload; static;
    Class Function eq( Const args: Array of Const     ): TRQLEq; Overload; static;
    Class Function ne( Const first, second: TRQLQuery ): TRQLNe; Overload; static;
    Class Function ne( Const first, second: Variant   ): TRQLNe; Overload; static;
    Class Function ne( Const args: TRQLArgs           ): TRQLNe; Overload; static;
    Class Function ne( Const args: Array of Const     ): TRQLNe; Overload; static;
    Class Function lt( Const first, second: TRQLQuery ): TRQLLt; Overload; static;
    Class Function lt( Const first, second: Variant   ): TRQLLt; Overload; static;
    Class Function lt( Const args: TRQLArgs           ): TRQLLt; Overload; static;
    Class Function lt( Const args: Array of Const     ): TRQLLt; Overload; static;
    Class Function le( Const first, second: TRQLQuery ): TRQLLe; Overload; static;
    Class Function le( Const first, second: Variant   ): TRQLLe; Overload; static;
    Class Function le( Const args: TRQLArgs           ): TRQLLe; Overload; static;
    Class Function le( Const args: Array of Const     ): TRQLLe; Overload; static;
    Class Function gt( Const first, second: TRQLQuery ): TRQLGt; Overload; static;
    Class Function gt( Const first, second: Variant   ): TRQLGt; Overload; static;
    Class Function gt( Const args: TRQLArgs           ): TRQLGt; Overload; static;
    Class Function gt( Const args: Array of Const     ): TRQLGt; Overload; static;
    Class Function ge( Const first, second: TRQLQuery ): TRQLGe; Overload; static;
    Class Function ge( Const first, second: Variant   ): TRQLGe; Overload; static;
    Class Function ge( Const args: TRQLArgs           ): TRQLGe; Overload; static;
    Class Function ge( Const args: Array of Const     ): TRQLGe; Overload; static;

    Class Function and_( Const first, second: TRQLQuery ): TRQLAnd; Overload; static;
    Class Function and_( Const first, second: Variant   ): TRQLAnd; Overload; static;
    Class Function and_( Const args: TRQLArgs           ): TRQLAnd; Overload; static;
    Class Function and_( Const args: Array of Const     ): TRQLAnd; Overload; static;
    Class Function or_( Const first, second: TRQLQuery  ): TRQLOr; Overload; static;
    Class Function or_( Const first, second: Variant    ): TRQLOr; Overload; static;
    Class Function or_( Const args: TRQLArgs            ): TRQLOr; Overload; static;
    Class Function or_( Const args: Array of Const      ): TRQLOr; Overload; static;
    Class Function not_( Const value: TRQLQuery         ): TRQLNot; Overload; static;
    Class Function not_( Const value: Variant           ): TRQLNot; Overload; static;

    Class Function add( Const first, second: TRQLQuery ): TRQLAdd; Overload; static;
    Class Function add( Const first, second: Variant   ): TRQLAdd; Overload; static;
    Class Function add( Const args: TRQLArgs           ): TRQLAdd; Overload; static;
    Class Function add( Const args: Array of Const     ): TRQLAdd; Overload; static;
    Class Function sub( Const first, second: TRQLQuery ): TRQLSub; Overload; static;
    Class Function sub( Const first, second: Variant   ): TRQLSub; Overload; static;
    Class Function sub( Const args: TRQLArgs           ): TRQLSub; Overload; static;
    Class Function sub( Const args: Array of Const     ): TRQLSub; Overload; static;
    Class Function mul( Const first, second: TRQLQuery ): TRQLMul; Overload; static;
    Class Function mul( Const first, second: Variant   ): TRQLMul; Overload; static;
    Class Function mul( Const args: TRQLArgs           ): TRQLMul; Overload; static;
    Class Function mul( Const args: Array of Const     ): TRQLMul; Overload; static;
    Class Function div_( Const first, second: TRQLQuery ): TRQLDiv; Overload; static;
    Class Function div_( Const first, second: Variant   ): TRQLDiv; Overload; static;
    Class Function div_( Const args: TRQLArgs           ): TRQLDiv; Overload; static;
    Class Function div_( Const args: Array of Const     ): TRQLDiv; Overload; static;
    Class Function mod_( Const first, second: TRQLQuery ): TRQLMod; Overload; static;
    Class Function mod_( Const first, second: Variant   ): TRQLMod; Overload; static;
    Class Function mod_( Const args: TRQLArgs           ): TRQLMod; Overload; static;
    Class Function mod_( Const args: Array of Const     ): TRQLMod; Overload; static;

    Class Function floor( Const value: TRQLQuery ): TRQLFloor; Overload; static;
    Class Function floor( Const value: Variant   ): TRQLFloor; Overload; static;
    Class Function ceil( Const value: TRQLQuery  ): TRQLCeil; Overload; static;
    Class Function ceil( Const value: Variant    ): TRQLCeil; Overload; static;
    Class Function round( Const value: TRQLQuery ): TRQLRound; Overload; static;
    Class Function round( Const value: Variant   ): TRQLRound; Overload; static;

    Class Function time( Const year, month, day: Word; Const timezone: String ): TRQLTime; Overload; static;
    Class Function time( Const year, month, day, hour, minutes, seconds: Word; Const timezone: String ): TRQLTime; Overload; static;
    Class Function iso8601( Const iso8601Date: String; Const defaultTimezone: String = '' ): TRQLISO8601; Overload; static;
    Class Function epochTime( Const t: double ): TRQLEpochTime; Overload; static;
    Class Function now: TRQLNow; Overload; static;

    Class Function monday: TRQLConstant; Overload; static;
    Class Function tuesday: TRQLConstant; Overload; static;
    Class Function wednesday: TRQLConstant; Overload; static;
    Class Function thursday: TRQLConstant; Overload; static;
    Class Function friday: TRQLConstant; Overload; static;
    Class Function saturday: TRQLConstant; Overload; static;
    Class Function sunday: TRQLConstant; Overload; static;

    Class Function january: TRQLConstant; Overload; static;
    Class Function february: TRQLConstant; Overload; static;
    Class Function march: TRQLConstant; Overload; static;
    Class Function april: TRQLConstant; Overload; static;
    Class Function may: TRQLConstant; Overload; static;
    Class Function june: TRQLConstant; Overload; static;
    Class Function july: TRQLConstant; Overload; static;
    Class Function august: TRQLConstant; Overload; static;
    Class Function september: TRQLConstant; Overload; static;
    Class Function october: TRQLConstant; Overload; static;
    Class Function november: TRQLConstant; Overload; static;
    Class Function december: TRQLConstant; Overload; static;

    Class Function minval: TRQLConstant; Overload; static;
    Class Function maxval: TRQLConstant; Overload; static;

    Class Function geojson( Const geojson: TJSONObject ): TRQLGeoJson; static;
    Class Function point( Const longitude, latitude: Double ): TRQLPoint; static;
    Class Function line( Const point1, point2: TRQLQuery ): TRQLLine; Overload; static;
    Class Function line( Const points: Array of TRQLQuery ): TRQLLine; Overload; static;
    Class Function polygon( Const point1, point2: TRQLQuery ): TRQLPolygon; Overload; static;
    Class Function polygon( Const points: Array of TRQLQuery ): TRQLPolygon; Overload; static;
    Class Function distance( Const point1, point2: TRQLQuery; Const geoSystem: String = 'WGS84'; Const geoUnit: String = 'm'): TRQLDistance; static;
    Class Function intersects( Const point1, point2: TRQLQuery ): TRQLIntersects; static;
    Class Function circle( Const point: TRQLQuery; radius: Double; numVertices: Integer = 32; geoSystem: String = 'WGS84'; geoUnit: String = 'm'; fill: Boolean = True ): TRQLCircle; static;
  End;

Type r = TRethinkDB;

Function PrintQuery( Query: TRQLQuery ): String;

Implementation

Uses System.TypInfo, System.Variants, XSBuiltIns, Encddecd, IdCoderMIME;

(** Workaround for broken TJSONString.ToString
    http://qc.embarcadero.com/wc/qcmain.aspx?d=119779 **)
Function JSONToString(obj: TJSONAncestor): String;
Var Bytes: TBytes; Len: Integer;
Begin
  SetLength(Bytes, obj.EstimatedByteSize);
  Len := obj.ToBytes(Bytes, 0);
  Result := TEncoding.UTF8.GetString(Bytes, 0, Len);
End;

Function VariantToJSONValue(Const Item: Variant): TJSONValue; Overload;
Var a: Array of Variant;
Begin
  Case VarType( Item ) And varTypeMask of
    varSmallInt,
    varInteger,
    varSingle,
    varDouble,
    varCurrency,
    varDate,
    varShortInt,
    varByte,
    varWord,
    varLongWord,
    varInt64,
    varUInt64:   Result := TJSONNumber.Create( Item );
    varOleStr,
    varUString,
    varString:   Result := TJSONString.Create( Item );
    varArray: Begin
      a := Item;
      Result := VariantToJSONValue( a );
    End;
    varBoolean:  Begin
      If Item.VBoolean
        Then Result := TJSONTrue.Create
        Else Result := TJSONFalse.Create;
    End;
    Else Result := TJSONNull.Create;
  End;
End;

Function VariantToJSONValue(Const Item: Array of Variant): TJSONValue; Overload;
Var I : Integer;
Begin
  Result := TJSONArray.Create;
  For I := Low(Item) To High(Item)
    Do (Result as TJSONArray).AddElement( VariantToJSONValue( Item[I] ) );
End;

Function VarRecToJSONValue(Const Item: TVarRec): TJSONValue;
Var K : TTypeKind;
Begin
  Case Item.VType Of
    vtInteger:    Result := TJSONNumber.Create( Item.VInteger );
    vtExtended:   Result := TJSONNumber.Create( Item.VExtended^ );
    vtCurrency:   Result := TJSONNumber.Create( Item.VCurrency^ );
    vtVariant:    Result := VariantToJSONValue( Item.VVariant^ );
    vtInt64:      Result := TJSONNumber.Create( Item.VInt64^ );
    vtPChar:      Result := TJSONString.Create( String( Item.VPChar^ ) );
    vtString:     Result := TJSONString.Create( ShortString( Item.VString^ ) );
    vtAnsiString: Result := TJSONString.Create( AnsiString( Item.VAnsiString ) );
    vtPWideChar:  Result := TJSONString.Create( WideString( Item.VPWideChar^ ) );
    vtWideString: Result := TJSONString.Create( WideString( Item.VWideString ) );
    vtUnicodeString: Result := TJSONString.Create( UnicodeString(Item.VUnicodeString) );
    vtBoolean: Begin
      If Item.VBoolean
        Then Result := TJSONTrue.Create
        Else Result := TJSONFalse.Create;
    End;
    vtObject: Begin
      If Item.VObject is TJSONValue
          Then Result := Item.VObject as TJSONValue
          Else Result := TJSONNull.Create;
    End
    Else
      Result := TJSONNull.Create;
  End;
End;

Function VarRecToRQLQuery(Const Item: TVarRec): TRQLQuery;
Begin
  If (Item.VType = vtObject) And ( Item.VObject is TRQLQuery )
    Then Result := Item.VObject as TRQLQuery
    Else Result := TRQLDatum.Create( VarRecToJSONValue( Item ) );
End;

Function VarRecToString(Const Item: TVarRec): String;
Begin
  Case Item.VType Of
    vtInteger:    Result := IntToStr( Item.VInteger );
    vtExtended:   Result := FloatToStr( Item.VExtended^ );
    vtCurrency:   Result := FloatToStr( Item.VCurrency^ );
    vtInt64:      Result := IntToStr( Item.VInt64^ );
    vtPChar:      Result := String( Item.VPChar^ );
    vtString:     Result := ShortString( Item.VString^ );
    vtAnsiString: Result := AnsiString( Item.VAnsiString );
    vtPWideChar:  Result := WideString( Item.VPWideChar^ );
    vtWideString: Result := WideString( Item.VWideString );
    vtUnicodeString: Result := UnicodeString(Item.VUnicodeString);
    vtBoolean: Begin
      If Item.VBoolean
        Then Result := 'True'
        Else Result := 'False'
    End;
    Else
      Result := '';
  End;
End;

Function VarRecIsString(Const Item: TVarRec): Boolean;
Begin
  Case Item.VType Of
    vtString,
    vtAnsiString,
    vtWideString,
    vtUnicodeString: Result := True;
  Else
    Result := False;
  End;
End;

Function NeedsWrap( Term: TRQLQuery ): Boolean; Inline;
Begin
  Result := (Term is TRQLDatum) Or (Term is TRQLMakeArray) Or (Term is TRQLMakeObj);
End;

Function IVarScan( Q: TRQLQuery ): Boolean;
Var I: Integer;
Begin
  Result := False;

  If (Q is TRQLImplicitVar)
    Then
      Begin
        Result := True;
        Exit;
      End;

  For I := Low( Q.FArgs ) To High( Q.FArgs ) Do
    If IVarScan( Q.FArgs[I] )
      Then
        Begin
          Result := True;
          Exit;
        End;

  For I := Low( Q.FOptArgs ) To High( Q.FOptArgs ) Do
    If IVarScan( Q.FOptArgs[I].Value )
      Then
        Begin
          Result := True;
          Exit;
        End;
End;

Function FuncWrap( Q: TRQLQuery ): TRQLQuery;
Begin
  If IVarScan( Q )
    Then Result := TRQLFunc.Create( Function( X: TRQLQuery ): TRQLQuery Begin Result := Q; End )
    Else Result := Q;
End;

Function PrintQuery( Query: TRQLQuery ): String;
Var Args: Array of String; OptArgs: Array of TPair<ShortString, String>; I : Integer;
Begin
  SetLength( Args, Length( Query.FArgs ) );
  For I := Low(Query.FArgs) To High(Query.FArgs)
    Do Args[I] := PrintQuery(Query.FArgs[I]);
  SetLength( OptArgs, Length( Query.FOptArgs ) );
  For I := Low(Query.FOptArgs) To High(Query.FOptArgs)
    Do OptArgs[I] := TPair<ShortString, String>.Create( Query.FOptArgs[I].Key, PrintQuery(Query.FOptArgs[I].Value) );
  Result := Query.Compose(Args, OptArgs);
End;

(** Errors **)
Constructor ERQLCursorEmpty.Create( Term: TRQLQuery );
Begin
  Inherited Create( 'Cursor is empty.', Term, Nil);
End;

Constructor ERQLTimeoutError.Create;
Begin
  Inherited Create('Operation timed out.');
End;

Constructor ERQLQueryError.Create(Const Msg: String; Const Term: TRQLQuery; Frames: TRethinkDbBacktrace);
Begin
  If Term <> Nil
    Then Inherited Create( Msg + ' in:' + sLineBreak + PrintQuery(Term) )
    Else Inherited Create( Msg );
  FTerm   := Term;
  FFrames := Frames;
End;

Constructor TRethinkDbFrame.Create( FT: TFrameType; Pos: Int64; Opt: UTF8String );
Begin
  Inherited Create;
  FType     := FT;
  FPosition := Pos;
  FOptional := Opt;
End;

Constructor TRQLResult.Create(T: TResultType; P: TJSONValue = Nil );
Begin
  Inherited Create;
  FType    := T;
  FProfile := P;
End;

Procedure _checkReQLType( Const reqlObject: TJSONObject; Const expectedType: String ); Inline;
Var p: TJSONPair;
Begin
  p := reqlObject.Get('$reql_type$');
  If p = Nil
    Then Raise ERQLDriverError.Create('Object does not contain a ReQL data type')
    Else If CompareStr( JSONToString(p.JsonValue), expectedType) <> 0
      Then Raise ERQLDriverError.Create('Unknown pseudo-type ' + JSONToString(p.JsonValue));
End;

Function _extractValue( Const reqlObject: TJSONObject; Const name: String ): TJSONValue; Inline;
Var p: TJSONPair;
Begin
  p := reqlObject.Get(name);
  If p = Nil
    Then Raise ERQLDriverError.Create('Object does not have expected field ' + name)
    Else Result := p.JsonValue;
End;

Class Function TRQLResult.convertToDateTime( Const reqlObject: TJSONObject ): TDateTime;
Var timestamp: Double; timezone: String; P: Integer; tz: SmallInt;
Begin
  _checkReQLType( reqlObject, '"TIME"' );
  timestamp := (_extractValue( reqlObject, 'epoch_time' ) as TJSONNumber).AsDouble;
  If reqlObject.Get('timezone') <> nil
    Then
      Begin
        Try
          timezone := JSONToString( reqlObject.Get('timezone').JsonValue );
          P := Pos( ':', timezone );
          tz := ( StrToInt( Copy( timezone, 3, P - 3 ) ) * 60 +  StrToInt( Copy( timezone, P + 1, Length(timezone) - P - 1 ) ) ) * 60;
          If timezone[2] = '-'
            Then tz := tz*(-1);
          timestamp := timestamp + tz;
        Except
          Raise ERQLDriverError.Create('Unable to interpret timezone ' + timezone);
        End;
      End;
  Result := (timestamp / 86400) + 25569.0;
End;

Class Function TRQLResult.convertToBytes( Const reqlObject: TJSONObject ): TIdBytes;
Begin
  _checkReQLType( reqlObject, '"BINARY"' );
  Result := TIdDecoderMIME.DecodeBytes( JSONToString( _extractValue( reqlObject, 'data' ) ) );
End;

Class Procedure TRQLResult.convertToBytes( Const reqlObject: TJSONObject; stream: TStream );
Begin
  _checkReQLType( reqlObject, '"BINARY"' );
  TIdDecoderMIME.DecodeStream( JSONToString( _extractValue( reqlObject, 'data' ) ), stream );
End;

Constructor TRQLDocument.Create( Value: TJSONValue; Profile: TJSONValue = Nil );
Begin
  Inherited Create( RESULT_JSON, Profile );
  FValue := Value;
End;

(** Query **)
Constructor TRethinkDbQuery.Create( QueryType: TQueryType; Token: Int64; Term: TRQLQuery = Nil);
Begin
  Inherited Create;
  FType    := QueryType;
  FToken   := Token;
  FTerm    := Term;
  FOptions := TJSONObject.Create;
End;

Constructor TRethinkDbQuery.Create( QueryType: TQueryType; Token: Int64; Term: TRQLQuery; Options: TJSONObject);
Var I: Integer;
Begin
  Inherited Create;
  FType    := QueryType;
  FToken   := Token;
  FTerm    := Term;
  FOptions := Options;
End;

Destructor TRethinkDbQuery.Destroy;
Begin
  If FTerm <> Nil
    Then FTerm.Destroy;
  If FOptions <> Nil
    Then FOptions.Destroy;
  Inherited;
End;

Procedure TRethinkDbQuery.Serialize( S: TStream );
Var L: LongWord; Wrapped_Query: TJSONArray; Wrapped_Query_String: UTF8String;
Begin
  S.Write(FToken, 8); // Token
  Wrapped_Query := TJSONArray.Create( TJSONNumber.Create(Integer(FType)) );
  If FTerm <> Nil
    Then
      Begin
        Wrapped_Query.AddElement( FTerm.Build );
        Wrapped_Query.AddElement( FOptions );
      End;
  Wrapped_Query_String := JSONToString(Wrapped_Query);
  L := TEncoding.UTF8.GetByteCount(Wrapped_Query_String);
  S.Write(L, 4);                                              // Length of Query
  S.Write( TEncoding.UTF8.GetBytes(Wrapped_Query_String), L); // Query
End;

(** Response **)
Constructor TRethinkDbResponse.Create( Token: Int64; Response: UTF8String );
Var Response_Object: TJSONObject; Backtrace_Array: TJSONValue;
Begin
  Inherited Create;
  FToken := Token;
  Response_Object := TJSONObject.ParseJSONValue( Response ) as TJsonObject;
  FType := TResponseType( (Response_Object.Get('t').JsonValue as TJSONNumber).AsInt );
  FData := (Response_Object.Get('r').JsonValue as TJSONArray);
  FData.Owned := False;
  If Response_Object.Get('b') <> Nil
    Then
      Begin
        Backtrace_Array := Response_Object.Get('b').JsonValue;
       // TODO: Create backtrace
      End
  Else FBacktrace := Nil;
  If Response_Object.Get('p') <> Nil
    Then
      Begin
        FProfile   := Response_Object.Get('p').JsonValue;
        FProfile.Owned := False;
      End;
  Response_Object.Destroy;
End;

Destructor TRethinkDbResponse.Destroy;
Begin
  FData.Destroy;
  If (FBacktrace <> Nil)
    Then FBacktrace.Destroy;
  If (FProfile <> Nil)
    Then FProfile.Destroy;
  Inherited;
End;

Function TRethinkDbResponse.MakeError( Query: TRethinkDbQuery ): ERQLError;
Begin
  Case FType Of
    RESPONSE_CLIENT_ERROR  : Result := ERQLClientError.Create( JSONToString(FData.Get(0)), Query.Term, FBacktrace);
    RESPONSE_COMPILE_ERROR : Result := ERQLCompileError.Create( JSONToString(FData.Get(0)), Query.Term, FBacktrace );
    RESPONSE_RUNTIME_ERROR : Result := ERQLRuntimeError.Create( JSONToString(FData.Get(0)), Query.Term, FBacktrace );
  Else
    Result := ERQLDriverError.Create( 'Unknown response type ' + IntToStr(Integer(FType)) + ' encountered in a response.' )
  End;
End;

(** Cursor **)
Constructor TRethinkDbCursor.Create(Connection: TRethinkDbConnection; Query: TRethinkDbQuery; Profile: TJSONValue = Nil);
Begin
  Inherited Create( RESULT_STREAM, Profile );
  FConnection := Connection;
  FQuery      := Query;
  FItems      := TJSONArray.Create;
  FOutstandingRequests := 1;
  FThreshold  := 0;
  FError      := Nil;

  FConnection.FCursorCache.Add( FQuery.Token, Self );
End;

Destructor TRethinkDbCursor.Destroy;
Begin
  FItems.Destroy;
  FConnection.FCursorCache.Remove( Query.Token );
  Inherited;
End;

Procedure TRethinkDbCursor.close;
Begin
  If FError = Nil
    Then
      Begin
        FError := ERQLCursorEmpty.Create( Query.Term );
        If Connection.IsOpen
          Then
            Begin
              Inc(FOutstandingRequests);
              Connection.DoStop(Self);
            End;
      End;
End;

Procedure TRethinkDbCursor.SetError( Msg: String );
Begin
  If (FError = Nil)
    Then
      Begin
        FError := ERQLRuntimeError.Create(Msg, Query.Term, Nil);
        AddResponse( TRethinkDbResponse.Create( Query.Token, '{"t":' + IntToStr(Integer(RESPONSE_SUCCESS_SEQUENCE)) +',"r":[]}' ) );
      End;
End;

Procedure TRethinkDbCursor.AddResponse( Response: TRethinkDbResponse );
Var Item: TJSONValue;
Begin
  Dec(FOutstandingRequests);
  FThreshold := Response.Data.Size;
  If FError = Nil
    Then
      Begin
        Case Response.ResponseType Of
          RESPONSE_SUCCESS_PARTIAL: Begin
            For Item in Response.Data
              Do FItems.AddElement(Item);
          End;

          RESPONSE_SUCCESS_SEQUENCE: Begin
            For Item in Response.Data
              Do FItems.AddElement(Item);
            FError :=  ERQLCursorEmpty.Create( Query.Term );
          End;

        Else
          FError := Response.MakeError( FQuery );
        End;
      End;
  MaybeFetchBatch;
  If (FOutstandingRequests = 0) And (FError <> Nil)
    Then FConnection.FCursorCache.Remove( Response.Token );
End;

Procedure TRethinkDbCursor.MaybeFetchBatch;
Begin
  If (FError = Nil) And (FItems.Size <= FThreshold) And (FOutstandingRequests = 0)
    Then
      Begin
        Inc(FOutstandingRequests);
        FConnection.DoContinue(Self);
      End;
End;

Function TRethinkDbCursor.GetNext(Timeout : LongInt = -1): TJSONValue;
Begin
  // TODO: Implement timeout
  While FItems.Size = 0 Do
    Begin
      MaybeFetchBatch;
      If FError <> Nil
        Then Raise FError;
      FConnection.GetResponse( FQuery.Token );
    End;
  Result := FItems.Get(0);
  FItems.Remove(0);
End;

Function TRethinkDbCursor.next(Const wait: Boolean = True): TJSONValue;
Begin
  If wait
    Then Result := GetNext
    Else Result := GetNext( 0 );
End;

Function TRethinkDbCursor.next(Const timeout: Integer): TJSONValue;
Begin
  Result := GetNext;
End;

Function TRethinkDbCursor.GetEnumerator: TRethinkDbCursorEnumerator;
Begin
  Result := TRethinkDbCursorEnumerator.Create( Self );
End;

Constructor TRethinkDbCursorEnumerator.Create( Cursor: TRethinkDbCursor );
Begin
  Inherited Create;
  FCursor := Cursor;
  FCurrentItem := Nil;
End;

Function TRethinkDbCursorEnumerator.GetCurrent: TJSONValue;
Begin
  Result := FCurrentItem;
End;

Function TRethinkDbCursorEnumerator.MoveNext: Boolean;
Begin
  Try
    FCurrentItem := FCursor.next;
    Result := True;
  Except
    Result := False;
  End;
End;

(** Connection **)
Constructor TRethinkDbConnection.Create(Const host: String; Const port: Word; Const db: String; Const auth_key: String; Const timeout: LongInt);
Begin
  Inherited Create;
  FSocket          := TIdTCPClient.Create(nil);
  FSocket.UseNagle := False;
  FData            := TMemoryStream.Create;
  FLock            := TCriticalSection.Create;
  FClosing         := False;
  FNextToken       := 1;
  FCursorCache     := TDictionary<Int64, TRethinkDbCursor>.Create;
  FConnectTimeout  := timeout;
  FAuthKey         := auth_key;
  FDb              := db;
  FHost            := host;
  FPort            := port;
End;

Destructor TRethinkDbConnection.Destroy;
Begin
  FreeAndNil(FSocket);
  FreeAndNil(FData);
  FreeAndNil(FLock);
  FreeAndNil(FCursorCache);
  Inherited;
End;

Procedure TRethinkDbConnection.Connect;
Var I: Integer; Response: UTF8String;
Begin
  FLock.Enter;
  Try
    FSocket.Host := Host;
    FSocket.Port := Port;
    FSocket.Connect;

    FData.Position := 0;

    I := Integer( VERSION_V0_4 ); // Version
    FData.Write(I, 4);

    I := Length(FAuthKey);        // Auth Key
    FData.Write(I, 4);
    FData.Write(FAuthKey[1],I);   // Protocol
    I := Integer( PROTOCOL_JSON );
    FData.Write(I, 4);
    I := FData.Position;

    // Send Auth Request
    FData.Position := 0;
    FSocket.IOHandler.Write(FData, 0 );

    // Get Auth Response
    Response := FSocket.IOHandler.ReadLn(#0, IndyUTF8Encoding);
    If Response <> 'SUCCESS'
      Then
        Begin
          FSocket.Disconnect;
          Raise ERQLDriverError.Create('Server dropped connection with message: "' + Response + '"');
        End;

  Finally
    FLock.Leave;
  End;
End;

Procedure TRethinkDbConnection.close( Const noreply_wait: Boolean = True );
Var T: Int64;
Begin
  If FSocket.Connected
    Then
      Begin
        T := GetNewToken;
        FNextToken := 1;
        Close(  NoReply_Wait, T );
      End;
End;

Procedure TRethinkDbConnection.close( Const noreply_wait: Boolean; Token: Int64 );
Var Cursors: TDictionary<Int64, TRethinkDBCursor>;
    C: TRethinkDBCursor;
Begin
  FClosing := True;

  // Close all outstanding cursors
  Cursors := TDictionary<Int64, TRethinkDBCursor>.Create(FCursorCache);
  For C In Cursors.Values
    Do C.SetError('Connection is closed.');
  FCursorCache.Clear;

  Try
    If NoReply_Wait
      Then RunQuery( TRethinkdbQuery.Create( QUERY_NOREPLY_WAIT, Token ), False );
  Finally
    FSocket.Disconnect;
  End;
End;

Function TRethinkDbConnection.IsOpen: Boolean;
Begin
  Result := FSocket.Connected;
End;

Procedure TRethinkDbConnection.CheckOpen;
Begin
  If Not FSocket.Connected
    Then Raise ERQLDriverError.Create('Connection is closed.');
End;

Function TRethinkDbConnection.noreply_wait: TRQLResult;
Begin
  CheckOpen;
  Result := RunQuery( TRethinkDbQuery.Create( QUERY_NOREPLY_WAIT, GetNewToken ),  False);
End;

Procedure TRethinkDbConnection.use( Const db_name: String );
Begin
  FDb := db_name;
End;

Procedure TRethinkDbConnection.reconnect( Const noreply_wait: Boolean = True );
Begin
  Close( noreply_wait );
  Connect;
End;

Function TRethinkDbConnection.GetNewToken: Int64;
Begin
  Result := FNextToken;
  Inc(FNextToken);
End;

Function TRethinkDbConnection.RunQuery( Query: TRethinkDbQuery; NoReply: Boolean ): TRQLResult;
Var Response: TRethinkDbResponse;
    Error   : ERQLError;
Begin
  Result := Nil;
  FLock.Enter;
  Try
    FData.Clear;
    // Serialize query
    Query.Serialize( FData );
    // Send query request
    FSocket.IOHandler.Write(FData, 0);

    If NoReply
      Then Exit;

    // Retreive response
    Response := GetResponse( Query.Token );

    Case Response.ResponseType Of
      RESPONSE_SUCCESS_ATOM: Begin
        // Single Datum
        Result := TRQLDocument.Create( Response.Data.Get(0), Response.Profile );
        Query.Destroy;
      End;
      RESPONSE_SUCCESS_SEQUENCE,
      RESPONSE_SUCCESS_PARTIAL: Begin
        // More data to come
        Result := TRethinkDbCursor.Create(Self, Query, Response.Profile );
        (Result as TRethinkDbCursor).AddResponse( Response );
        Exit;
      End;
      RESPONSE_WAIT_COMPLETE: Begin
        // Done waiting
        Query.Destroy;
      End;
      RESPONSE_CLIENT_ERROR,
      RESPONSE_COMPILE_ERROR,
      RESPONSE_RUNTIME_ERROR: Begin
        // Error occurred
        Error := Response.MakeError( Query );
        Query.Destroy;
        Raise Error;
      End;
    End;
  Finally
    FLock.Leave;
  End;
End;

Function TRethinkDbConnection.GetResponse( Token: Int64 ): TRethinkDbResponse;
Var L: LongWord;
    Response_Token  : Int64;
    Response_Length : Integer;
    Cursor  : TRethinkDbCursor;
Begin
  While True Do
    Begin
      // Get Query Response
      Response_Token  := FSocket.IOHandler.ReadInt64(false);
      Response_Length := FSocket.IOHandler.ReadLongInt(false);

      // Parse Response
      Result := TRethinkDbResponse.Create( Response_Token, FSocket.IOHandler.ReadString( Response_Length, IndyUTF8Encoding ) );

      // Add results to current Cursor if it exists
      Cursor := Nil;
      If FCursorCache.TryGetValue( Result.Token, Cursor )
        Then Cursor.AddResponse( Result );

      If (Result.Token = Token)
        Then Exit
        Else If (Cursor = Nil)
          Then
            Begin
              Close(False);
              Raise ERQLDriverError.Create('Unexpected response received.');
            End;
    End;
End;

Function TRethinkDbConnection.DoStart( Term: TRQLQuery; Options: TJSONObject ): TRQLResult;
Var Query: TRethinkDbQuery; db_name: TJSONPair; db_option: TRQLDB;
Begin
  CheckOpen;
  db_name := Options.Get('db');
  If (db_name <> Nil)
    Then Options.AddPair( 'db', TRQLDB.Create( [ JSONToString(db_name.JsonValue) ] ).Build )
    Else If  Length( DB ) > 0
      Then Options.AddPair( 'db', TRQLDB.Create( [DB] ).Build );
  Query := TRethinkDbQuery.Create( QUERY_START, GetNewToken, Term, Options );
  Result := RunQuery( Query, False );
End;

Function TRethinkDbConnection.DoContinue( Cursor: TRethinkDbCursor ): TRQLResult;
Var Query: TRethinkDbQuery;
Begin
  CheckOpen;
  Query := TRethinkDbQuery.Create( QUERY_CONTINUE, Cursor.Query.Token );
  Result := RunQuery( Query, True );
End;

Function TRethinkDbConnection.DoStop( Cursor: TRethinkDbCursor ): TRQLResult;
Var Query: TRethinkDbQuery;
Begin
  CheckOpen;
  Query := TRethinkDbQuery.Create( QUERY_STOP, Cursor.Query.Token );
  Result := RunQuery( Query, True );
End;

(** RQL Query **)
Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String = '' );
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;
  SetLength(FArgs, 0);
  SetLength(FOptArgs, 0);
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; Const OptArgs: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Length(Args));
  For I := Low(Args) To High(Args)
    Do FArgs[I] := VarRecToRQLQuery( Args[I] );

  SetLength(FOptArgs, Length(OptArgs) div 2);
  I := Low(OptArgs);
  While I <= High(OptArgs) Do
    Begin
      FOptArgs[I div 2] := TPair<String,TRQLQuery>.Create( VarRecToString(OptArgs[I]), VarRecToRQLQuery(OptArgs[I+1]) );
      Inc(I, 2);
    End;
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of Const; Const OptArgs: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := Obj;
  For I := Low(Args) To High(Args)
    Do FArgs[Succ(I)] := VarRecToRQLQuery( Args[I] );

  SetLength(FOptArgs, Length(OptArgs) div 2);
  I := Low(OptArgs);
  While I <= High(OptArgs) Do
    Begin
      FOptArgs[I div 2] := TPair<String,TRQLQuery>.Create( VarRecToString(OptArgs[I]), VarRecToRQLQuery(OptArgs[I+1]) );
      Inc(I, 2);
    End;
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := Obj;
  For I := Low(Args) To High(Args)
    Do FArgs[Succ(I)] := Args[I];

  SetLength(FOptArgs, Length(OptArgs) div 2);
  I := Low(OptArgs);
  While I <= High(OptArgs) Do
    Begin
      FOptArgs[I div 2] := TPair<String,TRQLQuery>.Create( VarRecToString(OptArgs[I]), VarRecToRQLQuery(OptArgs[I+1]) );
      Inc(I, 2);
    End;
End;


Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Length(Args));
  For I := Low(Args) To High(Args)
    Do FArgs[I] := Args[I];

  SetLength(FOptArgs, Length(OptArgs) div 2);
  I := Low(OptArgs);
  While I <= High(OptArgs) Do
    Begin
      FOptArgs[I div 2] := TPair<String,TRQLQuery>.Create( VarRecToString(OptArgs[I]), VarRecToRQLQuery(OptArgs[I+1]) );
      Inc(I, 2);
    End;
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Length(Args));
  For I := Low(Args) To High(Args)
    Do FArgs[I] := VarRecToRQLQuery( Args[I] );

  SetLength(FOptArgs, 0);
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of Const );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := Obj;
  For I := Low(Args) To High(Args)
    Do FArgs[Succ(I)] := VarRecToRQLQuery( Args[I] );

  SetLength(FOptArgs, 0);
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Obj: TRQLQuery; Const Args: Array of TRQLQuery );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := Obj;
  For I := Low(Args) To High(Args)
    Do FArgs[Succ(I)] := Args[I];

  SetLength(FOptArgs, 0);
End;

Constructor TRQLQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of TRQLQuery );
Var I: Integer;
Begin
  Inherited Create;
  FTermType   := TermType;
  FTypeString := TypeString;

  SetLength(FArgs, Length(Args));
  For I := Low(Args) To High(Args)
    Do FArgs[I] := Args[I];

  SetLength(FOptArgs, 0);
End;

Function TRQLQuery.Build: TJSONValue;
Var Arg: TRQLQuery; ArgList: TJSONArray; OptDict: TJSONObject; OptArg: TPair<String,TRQLQuery>;
Begin
  Result := TJSONArray.Create(  TJSONNumber.Create(Integer(FTermType)) );
  ArgList := TJSONArray.Create;
  For Arg in FArgs
    Do ArgList.AddElement( Arg.Build );
  (Result as TJSONArray).AddElement(ArgList);
  If Length(FOptArgs) > 0
    Then
      Begin
        OptDict := TJSONObject.Create;
        For OptArg in FOptArgs
          Do OptDict.AddPair( OptArg.Key, OptArg.Value.Build );
        (Result as TJSONArray).AddElement(OptDict);
      End;
End;

Function TRQLQuery.run( conn: TRethinkDbConnection ): TRQLResult;
Begin
  Result := run( conn, [] );
End;

Function TRQLQuery.run( conn: TRethinkDbConnection; Const options: Array of Const ): TRQLResult;
Var I : Integer; OptionsObject : TJSONObject;
Begin
  If conn = Nil
    Then Raise ERQLDriverError.Create('RqlQuery.run must be given a connection to run on.');
  OptionsObject := TJSONObject.Create;
  I := Low(Options);
  While I <= High(Options) Do
    Begin
      OptionsObject.AddPair( VarRecToString(Options[I]), VarRecToJSONValue(Options[I+1]) );
      Inc(I, 2);
    End;
  Result := conn.DoStart( self, OptionsObject );
End;

Function TRQLQuery.ToString: String;
Begin
  Result := JSONToString( Build );
End;

Function TRQLQuery.getFieldAt( index: Integer ): TRQLBracket;
Begin
  Result := TRQLBracket.Create([ Self, index ]);
End;

Function TRQLQuery.eq( Const other: TRQLQuery ): TRQLEq;
Begin
  Result := TRQLEq.Create([self, other]);
End;

Function TRQLQuery.eq( Const other: Variant ): TRQLEq;
Begin
  Result := TRQLEq.Create([self, r.expr(other)]);
End;

Function TRQLQuery.ne( Const other: TRQLQuery ): TRQLNe;
Begin
  Result := TRQLNe.Create([self, other]);
End;

Function TRQLQuery.ne( Const other: Variant ): TRQLNe;
Begin
  Result := TRQLNe.Create([self, r.expr(other)]);
End;

Function TRQLQuery.lt( Const other: TRQLQuery ): TRQLLt;
Begin
  Result := TRQLLt.Create([self, other]);
End;

Function TRQLQuery.lt( Const other: Variant ): TRQLLt;
Begin
  Result := TRQLLt.Create([self, r.expr(other)]);
End;

Function TRQLQuery.le( Const other: TRQLQuery ): TRQLLe;
Begin
  Result := TRQLLe.Create([self, other]);
End;

Function TRQLQuery.le( Const other: Variant ): TRQLLe;
Begin
  Result := TRQLLe.Create([self, r.expr(other)]);
End;

Function TRQLQuery.gt( Const other: TRQLQuery ): TRQLGt;
Begin
  Result := TRQLGt.Create([self, other]);
End;

Function TRQLQuery.gt( Const other: Variant ): TRQLGt;
Begin
  Result := TRQLGt.Create([self, r.expr(other)]);
End;

Function TRQLQuery.ge( Const other: TRQLQuery ): TRQLGe;
Begin
  Result := TRQLGe.Create([self, other]);
End;

Function TRQLQuery.ge( Const other: Variant ): TRQLGe;
Begin
  Result := TRQLGe.Create([self, r.expr(other)]);
End;

Function TRQLQuery.add( Const other: TRQLQuery ): TRQLAdd;
Begin
  Result := TRQLAdd.Create([self, other]);
End;

Function TRQLQuery.add( Const other: Variant ): TRQLAdd;
Begin
  Result := TRQLAdd.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.sub( Const other: TRQLQuery ): TRQLSub;
Begin
  Result := TRQLSub.Create([self, other]);
End;

Function TRQLQuery.sub( Const other: Variant ): TRQLSub;
Begin
  Result := TRQLSub.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.mul( Const other: TRQLQuery ): TRQLMul;
Begin
  Result := TRQLMul.Create([self, other]);
End;

Function TRQLQuery.mul( Const other: Variant ): TRQLMul;
Begin
  Result := TRQLMul.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.div_( Const other: TRQLQuery ): TRQLDiv;
Begin
  Result := TRQLDiv.Create([self, other]);
End;

Function TRQLQuery.div_( Const other: Variant ): TRQLDiv;
Begin
  Result := TRQLDiv.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.mod_( Const other: TRQLQuery ): TRQLMod;
Begin
  Result := TRQLMod.Create([self, other]);
End;

Function TRQLQuery.mod_( Const other: Variant ): TRQLMod;
Begin
  Result := TRQLMod.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.floor: TRQLFloor;
Begin
  Result := TRQLFloor.Create([self]);
End;

Function TRQLQuery.ceil: TRQLCeil;
Begin
  Result := TRQLCeil.Create([self]);
End;

Function TRQLQuery.round: TRQLRound;
Begin
  Result := TRQLRound.Create([self]);
End;

Function TRQLQuery.and_( Const other: TRQLQuery ): TRQLAnd;
Begin
  Result := TRQLAnd.Create([self, other]);
End;

Function TRQLQuery.and_( Const other: Variant ): TRQLAnd;
Begin
  Result := TRQLAnd.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.or_( Const other: TRQLQuery ): TRQLOr;
Begin
  Result := TRQLOr.Create([self, other]);
End;

Function TRQLQuery.or_( Const other: Variant ): TRQLOr;
Begin
  Result := TRQLOr.Create([self, r.expr(other) ]);
End;

Function TRQLQuery.not_: TRQLNot;
Begin
  Result := TRQLNot.Create([self]);
End;

Function TRQLQuery.contains( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLContains;
Begin
  Result := TRQLContains.Create( [ Self, r.expr(f)] );
End;

Function TRQLQuery.contains( Const f: TRQLQuery ): TRQLContains;
Begin
  Result := TRQLContains.Create( [ Self, FuncWrap(f) ] );
End;

Function TRQLQuery.hasFields( Const key: String ): TRQLHasFields;
Begin
  Result := TRQLHasFields.Create( self, [key] );
End;

Function TRQLQuery.hasFields( Const keys: Array of Const ): TRQLHasFields;
Begin
  Result := TRQLHasFields.Create( self, keys );
End;

Function TRQLQuery.withFields( Const key: String ): TRQLWithFields;
Begin
  Result := TRQLWithFields.Create( self, [key] );
End;

Function TRQLQuery.withFields( Const keys: Array of Const ): TRQLWithFields;
Begin
  Result := TRQLWithFields.Create( self, keys );
End;

Function TRQLQuery.keys: TRQLKeys;
Begin
  Result := TRQLKeys.Create( [ self ] );
End;

Function TRQLQuery.changes( Const squash: Boolean; includeStates: Boolean = False ): TRQLChanges;
Begin
  Result := TRQLChanges.Create( [ self ], [ 'squash', squash, 'include_states', includeStates ] );
End;

Function TRQLQuery.changes( Const squash: Double; includeStates: Boolean = False ): TRQLChanges;
Begin
  Result := TRQLChanges.Create( [ self ], [ 'squash', squash, 'include_states', includeStates ] );
End;

Function TRQLQuery.changes( Const includeStates: Boolean = False ): TRQLChanges;
Begin
  If includeStates
    Then Result := TRQLChanges.Create( [ self ], [ 'include_states', includeStates ] )
    Else  Result := TRQLChanges.Create( [ self ], [ ] );
End;

Function TRQLQuery.pluck( Const key: String ): TRQLPluck;
Begin
  Result := TRQLPluck.Create( self, [key] );
End;

Function TRQLQuery.pluck( Const keys: Array of Const ): TRQLPluck;
Begin
  Result := TRQLPluck.Create( self, keys );
End;

Function TRQLQuery.without( Const key: String ): TRQLWithout;
Begin
  Result := TRQLWithout.Create( self, [key] );
End;

Function TRQLQuery.without( Const keys: Array of Const  ): TRQLWithout;
Begin
  Result := TRQLWithout.Create( self, keys );
End;

Function TRQLQuery.map( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ Self, r.expr(f)], [] );
End;

Function TRQLQuery.map( Const f: TRQLQuery ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ Self, FuncWrap(f)], [] );
End;

Function TRQLQuery.filter( Const f: TFunc<TRQLQuery, TRQLQuery>; default: Boolean = False ): TRQLFilter;
Begin
  If default
    Then Result := TRQLFilter.Create( [ Self, r.expr(f)], [ 'default', default ] )
    Else Result := TRQLFilter.Create( [ Self, r.expr(f)], [] );
End;

Function TRQLQuery.filter( Const f: TRQLQuery; default: Boolean = False ): TRQLFilter;
Begin
  If default
    Then Result := TRQLFilter.Create( [ Self, FuncWrap(f)], [ 'default', default ] )
    Else Result := TRQLFilter.Create( [ Self, FuncWrap(f)], [] );
End;

Function TRQLQuery.filter( Const f: TFunc<TRQLQuery, TRQLQuery>; default: TRQLUserError ): TRQLFilter;
Begin
  Result := TRQLFilter.Create( [ Self, r.expr(f)], [ 'default', default ] )
End;

Function TRQLQuery.filter( Const f: TRQLQuery; default: TRQLUserError ): TRQLFilter;
Begin
  Result := TRQLFilter.Create( [ Self, FuncWrap(f)], [ 'default', default ] )
End;

Function TRQLQuery.concatMap( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLConcatMap;
Begin
  Result := TRQLConcatMap.Create( [ Self, r.expr(f)] );
End;

Function TRQLQuery.concatMap( Const f: TRQLQuery ): TRQLConcatMap;
Begin
  Result := TRQLConcatMap.Create( [ Self, FuncWrap(f)] );
End;

Function TRQLQuery.orderBy( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLOrderBy;
Begin
  If isIndex
    Then Result := TRQLOrderBy.Create( [ Self ], [ 'index', field_or_index ] )
    Else Result := TRQLOrderBy.Create( [ Self, field_or_index ], [] );
End;

Function TRQLQuery.orderBy( Const keys: Array of String ): TRQLOrderBy;
Begin
  Result := TRQLOrderBy.Create( Self, keys );
End;

Function TRQLQuery.orderBy( Const keys: Array of Const ): TRQLOrderBy;
Begin
  Result := TRQLOrderBy.Create( Self, keys  );
End;

Function TRQLQuery.between( const lowerKey, upperKey: String; index: String = 'id'; leftBound: String = 'closed'; rightBound: String = 'open'): TRQLBetween;
Begin
  Result := TRQLBetween.Create( [ Self, lowerKey, upperKey ], [ 'index', index, 'left_bound', leftBound, 'right_bound', rightBound ] );
End;

Function TRQLQuery.distinct: TRQLDistinct;
Begin
  Result := TRQLDistinct.Create( [ Self ], [] );
End;

Function TRQLQuery.distinct( Const index: String ): TRQLDistinct;
Begin
  Result := TRQLDistinct.Create( [ Self ], [ 'index', index ] );
End;

Function TRQLQuery.count(Const q : TRQLQuery): TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, FuncWrap(q) ] );
End;

Function TRQLQuery.count(Const v : Variant)  : TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, r.expr(v) ] );
End;

Function TRQLQuery.count(Const a : Array of Variant): TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.count(Const a : Array of Const): TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.count(Const a : TJSONValue):  TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.count( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLCount;
Begin
  Result := TRQLCount.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.union( Const sequence: TRQLQuery ): TRQLUnion;
Begin
  Result := TRQLUnion.Create( [ Self, sequence ] );
End;

Function TRQLQuery.union( Const sequences: Array of Const ): TRQLUnion;
Begin
  Result := TRQLUnion.Create( Self, sequences );
End;

Function TRQLQuery.union( Const sequences: Array of TRQLQuery ): TRQLUnion;
Begin
  Result := TRQLUnion.Create( Self, sequences );
End;

Function TRQLQuery.innerJoin( Const other: TRQLQuery; Const predicate: TFunc<TRQLQuery, TRQLQuery> ): TRQLInnerJoin;
Begin
  Result := TRQLInnerJoin.Create( [ Self, other, r.expr(predicate) ] );
End;

Function TRQLQuery.innerJoin( Const other: TRQLQuery; Const predicate: TRQLQuery ): TRQLInnerJoin;
Begin
  Result := TRQLInnerJoin.Create( [ Self, other, FuncWrap(predicate) ] );
End;

Function TRQLQuery.outerJoin( Const other: TRQLQuery; Const predicate: TFunc<TRQLQuery, TRQLQuery> ): TRQLOuterJoin;
Begin
  Result := TRQLOuterJoin.Create( [ Self, other, r.expr(predicate) ] );
End;

Function TRQLQuery.outerJoin( Const other: TRQLQuery; Const predicate: TRQLQuery ): TRQLOuterJoin;
Begin
  Result := TRQLOuterJoin.Create( [ Self, other, FuncWrap(predicate) ] );
End;

Function TRQLQuery.eqJoin( Const leftField: String; Const rightTable: TRQLTable; Const index: String = 'id' ): TRQLEqJoin;
Begin
  If (index <> 'id')
    Then Result := TRQLEqJoin.Create( [ Self, leftField, rightTable ], [ 'index', index ] )
    Else Result := TRQLEqJoin.Create( [ Self, leftField, rightTable ], [ ] );
End;

Function TRQLQuery.zip: TRQLZip;
Begin
  Result := TRQLZip.Create([ Self ]);
End;

Function TRQLQuery.group(Const q : TRQLQuery; Const index: String = ''; Const multi: Boolean = False): TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, FuncWrap(q) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, FuncWrap(q) ], [ 'index', index ] );
      End
    Else
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, FuncWrap(q) ], [ 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, FuncWrap(q) ], [ ] );
      End;
End;

Function TRQLQuery.group(Const v : Variant; Const index: String = ''; Const multi: Boolean = False)  : TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(v) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(v) ], [ 'index', index ] );
      End
    Else
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(v) ], [ 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(v) ], [ ] );
      End;
End;

Function TRQLQuery.group(Const a : Array of Variant; Const index: String = ''; Const multi: Boolean = False): TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index ] );
              End
            Else
              Begin
                If multi
                  Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'multi', multi ] )
                  Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ ] );
              End;
End;

Function TRQLQuery.group(Const a : Array of Const; Const index: String = ''; Const multi: Boolean = False): TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index ] );
              End
            Else
              Begin
                If multi
                  Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'multi', multi ] )
                  Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ ] );
              End;
End;

Function TRQLQuery.group(Const a : TJSONValue; Const index: String = ''; Const multi: Boolean = False):  TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'index', index ] );
              End
            Else
              Begin
                If multi
                  Then Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ 'multi', multi ] )
                  Else Result := TRQLGroup.Create( [ Self, r.expr(a) ], [ ] );
              End;
End;

Function TRQLQuery.group( Const f: TFunc<TRQLQuery, TRQLQuery>; Const index: String = ''; Const multi: Boolean = False): TRQLGroup;
Begin
  If index <> ''
    Then
      Begin
        If multi
          Then Result := TRQLGroup.Create( [ Self, r.expr(f) ], [ 'index', index, 'multi', multi ] )
          Else Result := TRQLGroup.Create( [ Self, r.expr(f) ], [ 'index', index ] );
              End
            Else
              Begin
                If multi
                  Then Result := TRQLGroup.Create( [ Self, r.expr(f) ], [ 'multi', multi ] )
                  Else Result := TRQLGroup.Create( [ Self, r.expr(f) ], [ ] );
              End;
End;

Function TRQLQuery.forEach( Const f: TRQLQuery ): TRQLForEach;
Begin
  Result := TRQLForEach.Create( [ Self, FuncWrap(f) ] );
End;

Function TRQLQuery.forEach( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLForEach;
Begin
  Result := TRQLForEach.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.info: TRQLInfo;
Begin
  Result := TRQLInfo.Create( [ Self ] );
End;

Function TRQLQuery.insertAt(Const index: Integer; Const value : TRQLQuery): TRQLInsertAt;
Begin
  Result := TRQLInsertAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.insertAt(Const index: Integer; Const value : Variant)  : TRQLInsertAt;
Begin
  Result := TRQLInsertAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.insertAt(Const index: Integer; Const value : Array of Variant): TRQLInsertAt;
Begin
  Result := TRQLInsertAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.insertAt(Const index: Integer; Const value : Array of Const): TRQLInsertAt;
Begin
  Result := TRQLInsertAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.insertAt(Const index: Integer; Const value : TJSONValue):  TRQLInsertAt;
Begin
  Result := TRQLInsertAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.spliceAt( Const index: Integer; Const value : TRQLQuery ): TRQLSpliceAt;
Begin
  Result := TRQLSpliceAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.spliceAt( Const index: Integer; Const value : Array of Variant ): TRQLSpliceAt;
Begin
  Result := TRQLSpliceAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.spliceAt( Const index: Integer; Const value : Array of Const ): TRQLSpliceAt;
Begin
  Result := TRQLSpliceAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.deleteAt( Const index: Integer ): TRQLDeleteAt;
Begin
  Result := TRQLDeleteAt.Create( [ Self, index ] );
End;

Function TRQLQuery.deleteAt( Const index, endIndex: Integer ): TRQLDeleteAt;
Begin
  Result := TRQLDeleteAt.Create( [ Self, index, endIndex ] );
End;

Function TRQLQuery.changeAt(Const index: Integer; Const value : TRQLQuery): TRQLChangeAt;
Begin
  Result := TRQLChangeAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.changeAt(Const index: Integer; Const value : Variant)  : TRQLChangeAt;
Begin
  Result := TRQLChangeAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.changeAt(Const index: Integer; Const value : Array of Variant): TRQLChangeAt;
Begin
  Result := TRQLChangeAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.changeAt(Const index: Integer; Const value : Array of Const): TRQLChangeAt;
Begin
  Result := TRQLChangeAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.changeAt(Const index: Integer; Const value : TJSONValue):  TRQLChangeAt;
Begin
  Result := TRQLChangeAt.Create( [ Self, index, r.expr(value) ] );
End;

Function TRQLQuery.sample( Const n: Integer ): TRQLSample;
Begin
  Result := TRQLSample.Create( [ Self, n ] );
End;

Function TRQLQuery.toISO8601: TRQLToISO8601;
Begin
  Result := TRQLToISO8601.Create( [ Self ] );
End;

Function TRQLQuery.toEpochTime: TRQLToEpochTime;
Begin
  Result := TRQLToEpochTime.Create( [ Self ] );
End;

Function TRQLQuery.during( Const startTime, endTime: TRQLQuery; leftBound: String = 'closed'; rightBound: String = 'open'): TRQLDuring;
Begin
  Result := TRQLDuring.Create( [ Self, startTime, endTime ], [ 'left_bound', leftBound, 'right_bound' , rightBound ] );
End;

Function TRQLQuery.date: TRQLDate;
Begin
  Result := TRQLDate.Create( [ Self ] );
End;

Function TRQLQuery.timeOfDay: TRQLTimeOfDay;
Begin
  Result := TRQLTimeOfDay.Create( [ Self ] );
End;

Function TRQLQuery.timezone: TRQLTimezone;
Begin
  Result := TRQLTimezone.Create( [ Self ] );
End;

Function TRQLQuery.year: TRQLYear;
Begin
  Result := TRQLYear.Create( [ Self ] );
End;

Function TRQLQuery.month: TRQLMonth;
Begin
  Result := TRQLMonth.Create( [ Self ] );
End;

Function TRQLQuery.day: TRQLDay;
Begin
  Result := TRQLDay.Create( [ Self ] );
End;

Function TRQLQuery.dayOfWeek: TRQLDayOfWeek;
Begin
  Result := TRQLDayOfWeek.Create( [ Self ] );
End;

Function TRQLQuery.dayOfYear: TRQLDayOfYear;
Begin
  Result := TRQLDayOfYear.Create( [ Self ] );
End;

Function TRQLQuery.hours: TRQLHours;
Begin
  Result := TRQLHours.Create( [ Self ] );
End;

Function TRQLQuery.minutes: TRQLMinutes;
Begin
  Result := TRQLMinutes.Create( [ Self ] );
End;

Function TRQLQuery.seconds: TRQLSeconds;
Begin
  Result := TRQLSeconds.Create( [ Self ] );
End;

Function TRQLQuery.inTimezone( Const timezone: String ): TRQLInTimezone;
Begin
  Result := TRQLInTimezone.Create( [ Self, timezone ] );
End;

Function TRQLQuery.toGeoJson: TRQLToGeoJson;
Begin
  Result := TRQLToGeoJson.Create( [ Self ] );
End;

Function TRQLQuery.distance( Const geometry: TRQLQuery; geoSystem: String = 'WGS84'; geoUnit: String = 'm' ) : TRQLDistance;
Begin
  Result := TRQLDistance.Create( [ Self, geometry ], [ 'geo_system', geoSystem, 'unit', geoUnit ] );
End;

Function TRQLQuery.intersects( Const geometry: TRQLQuery ): TRQLIntersects;
Begin
  Result := TRQLIntersects.Create( [ Self, geometry ] );
End;

Function TRQLQuery.includes( Const geometry: TRQLQuery ): TRQLIncludes;
Begin
  Result := TRQLIncludes.Create( [ Self, geometry ] );
End;

Function TRQLQuery.fill: TRQLFill;
Begin
  Result := TRQLFill.Create( [ Self ] );
End;

Function TRQLQuery.polygonSub( Const polygon: TRQLQuery ): TRQLPolygonSub;
Begin
  Result := TRQLPolygonSub.Create( [ Self, polygon ] );
End;

Function TRQLQuery.do_( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, 2 );
  arr[0] := Self;
  arr[1] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Function TRQLQuery.do_( Const f: TRQLQuery ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, 2 );
  arr[0] := Self;
  arr[1] := FuncWrap( f );
  Result := TRQLFunCall.Create( arr );
End;

Function TRQLQuery.default(Const q : TRQLQuery): TRQLDefault;
Begin
  Result := TRQLDefault.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.default(Const v : Variant)  : TRQLDefault;
Begin
  Result := TRQLDefault.Create( [ Self, r.expr(v) ] );
End;

Function TRQLQuery.default(Const a : Array of Variant): TRQLDefault;
Begin
  Result := TRQLDefault.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.default(Const a : Array of Const): TRQLDefault;
Begin
  Result := TRQLDefault.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.default(Const a : TJSONValue):  TRQLDefault;
Begin
  Result := TRQLDefault.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.update( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLUpdate;
Begin
  Result := TRQLUpdate.Create( [ Self, FuncWrap(item) ], [ 'durability', durability, 'return_changes', returnChanges, 'non_atomic', nonAtomic ] );
End;

Function TRQLQuery.update( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLUpdate;
Begin
  Result := TRQLUpdate.Create( [ Self, item ], [ 'durability', durability, 'return_changes', returnChanges, 'non_atomic', nonAtomic ] );
End;

Function TRQLQuery.replace( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLReplace;
Begin
  Result := TRQLReplace.Create( [ Self, FuncWrap(item) ], [ 'durability', durability, 'return_changes', returnChanges, 'non_atomic', nonAtomic ] );
End;

Function TRQLQuery.replace( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; nonAtomic: Boolean = False ): TRQLReplace;
Begin
  Result := TRQLReplace.Create( [ Self, item ], [ 'durability', durability, 'return_changes', returnChanges, 'non_atomic', nonAtomic ] );
End;

Function TRQLQuery.delete( durability: String = 'hard'; returnChanges : Boolean = False ): TRQLDelete;
Begin
  Result := TRQLDelete.Create( [ Self ], [ 'durability', durability, 'return_changes', returnChanges ] );
End;

Function TRQLQuery.coerceTo( Const t: String ): TRQLCoerceTo;
Begin
  Result := TRQLCoerceTo.Create( [ Self, t ] );
End;

Function TRQLQuery.ungroup: TRQLUngroup;
Begin
  Result := TRQLUngroup.Create( [ Self ] );
End;

Function TRQLQuery.typeOf: TRQLTypeOf;
Begin
  Result := TRQLTypeOf.Create( [ Self ] );
End;

Function TRQLQuery.merge( Const item: TRQLQuery ): TRQLMerge;
Begin
  Result := TRQLMerge.Create( [ Self, FuncWrap(item) ] );
End;

Function TRQLQuery.merge( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMerge;
Begin
  Result := TRQLMerge.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.append(Const q : TRQLQuery): TRQLAppend;
Begin
  Result := TRQLAppend.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.append(Const v : Variant)  : TRQLAppend;
Begin
  Result := TRQLAppend.Create( [ Self, r.expr(v) ] );
End;

Function TRQLQuery.append(Const a : Array of Variant): TRQLAppend;
Begin
  Result := TRQLAppend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.append(Const a : Array of Const): TRQLAppend;
Begin
  Result := TRQLAppend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.append(Const a : TJSONValue):  TRQLAppend;
Begin
  Result := TRQLAppend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.prepend(Const q : TRQLQuery): TRQLPrepend;
Begin
  Result := TRQLPrepend.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.prepend(Const v : Variant)  : TRQLPrepend;
Begin
  Result := TRQLPrepend.Create( [ Self, r.expr(v) ] );
End;

Function TRQLQuery.prepend(Const a : Array of Variant): TRQLPrepend;
Begin
  Result := TRQLPrepend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.prepend(Const a : Array of Const): TRQLPrepend;
Begin
  Result := TRQLPrepend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.prepend(Const a : TJSONValue):  TRQLPrepend;
Begin
  Result := TRQLPrepend.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.difference( Const q: TRQLQuery ): TRQLDifference;
Begin
  Result := TRQLDifference.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.difference( Const q : Array of Const ): TRQLDifference;
Begin
  Result := TRQLDifference.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setInsert( Const q: TRQLQuery ): TRQLSetInsert;
Begin
  Result := TRQLSetInsert.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setInsert( Const q : Array of Const ): TRQLSetInsert;
Begin
  Result := TRQLSetInsert.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setUnion( Const q: TRQLQuery ): TRQLSetUnion;
Begin
  Result := TRQLSetUnion.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setUnion( Const q : Array of Const ): TRQLSetUnion;
Begin
  Result := TRQLSetUnion.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setIntersection( Const q: TRQLQuery ): TRQLSetIntersection;
Begin
  Result := TRQLSetIntersection.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setIntersection( Const q : Array of Const ): TRQLSetIntersection;
Begin
  Result := TRQLSetIntersection.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setDifference( Const q: TRQLQuery ): TRQLSetDifference;
Begin
  Result := TRQLSetDifference.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.setDifference( Const q : Array of Const ): TRQLSetDifference;
Begin
  Result := TRQLSetDifference.Create( [ Self, r.expr(q) ] );
End;

Function TRQLQuery.getField( Const key: String ): TRQLGetField;
Begin
  Result := TRQLGetField.Create( [ Self, key ] );
End;

Function TRQLQuery.nth( Const index: Integer ): TRQLNth;
Begin
  Result := TRQLNth.Create( [ Self, index ] );
End;

Function TRQLQuery.toJson: TRQLToJsonString;
Begin
  Result := TRQLToJsonString.Create( [ Self ] );
End;

Function TRQLQuery.toJsonString: TRQLToJsonString;
Begin
  Result := TRQLToJsonString.Create( [ Self ] );
End;

Function TRQLQuery.match( Const regex: String ): TRQLMatch;
Begin
  Result := TRQLMatch.Create( [ Self, regex ] );
End;

Function TRQLQuery.split: TRQLSplit;
Begin
  Result := TRQLSplit.Create( [ Self ] );
End;

Function TRQLQuery.split( maxSplits: Integer ): TRQLSplit;
Begin
  Result := TRQLSplit.Create( [ Self, Nil, maxSplits ] );
End;

Function TRQLQuery.split( Const separator: String ): TRQLSplit;
Begin
  Result := TRQLSplit.Create( [ Self, separator ] );
End;

Function TRQLQuery.split( Const separator: String; maxSplits: Integer ): TRQLSplit;
Begin
  Result := TRQLSplit.Create( [ Self, separator, maxSplits ] );
End;

Function TRQLQuery.upcase: TRQLUpcase;
Begin
  Result := TRQLUpcase.Create( [ Self ] );
End;

Function TRQLQuery.downcase: TRQLDowncase;
Begin
  Result := TRQLDowncase.Create( [ Self ] );
End;

Function TRQLQuery.isEmpty: TRQLIsEmpty;
Begin
  Result := TRQLIsEmpty.Create( [ Self ] );
End;

Function TRQLQuery.offsetsOf(Const q : TRQLQuery): TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, FuncWrap(q) ] );
End;

Function TRQLQuery.offsetsOf(Const v : Variant)  : TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, r.expr(v) ] );
End;

Function TRQLQuery.offsetsOf(Const a : Array of Variant): TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.offsetsOf(Const a : Array of Const): TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.offsetsOf(Const a : TJSONValue):  TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, r.expr(a) ] );
End;

Function TRQLQuery.offsetsOf( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLOffsetsOf;
Begin
  Result := TRQLOffsetsOf.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.slice( Const startIndex: Integer; leftBound: String = 'closed'; rightBound: String = 'open' ): TRQLSlice;
Begin
  Result := TRQLSlice.Create( [ Self, startIndex ], [ 'left_bound', leftBound , 'right_bound', rightBound ] );
End;

Function TRQLQuery.slice( Const startIndex, endIndex: Integer; leftBound: String = 'closed'; rightBound: String = 'open' ): TRQLSlice;
Begin
  Result := TRQLSlice.Create( [ Self, startIndex, endIndex ], [ 'left_bound', leftBound , 'right_bound', rightBound ] );
End;

Function TRQLQuery.skip( Const n: Integer ): TRQLSkip;
Begin
  Result := TRQLSkip.Create( [ Self, n ] );
End;

Function TRQLQuery.limit( Const n: Integer ): TRQLLimit;
Begin
  Result := TRQLLimit.Create( [ Self, n ] );
End;

Function TRQLQuery.reduce( Const f: TRQLQuery ): TRQLReduce;
Begin
  Result := TRQLReduce.Create( [ Self, FuncWrap(f) ] );
End;

Function TRQLQuery.reduce( Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLReduce;
Begin
  Result := TRQLReduce.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.sum( Const field: String ): TRQLSum;
Begin
  Result := TRQLSum.Create( [ Self, field ] );
End;

Function TRQLQuery.sum( Const f: TRQLQuery ): TRQLSum;
Begin
  Result := TRQLSum.Create( [ Self, FuncWrap(f) ] );
End;

Function TRQLQuery.sum( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLSum;
Begin
  Result := TRQLSum.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.avg( Const field: String ): TRQLAvg;
Begin
  Result := TRQLAvg.Create( [ Self, field ] );
End;

Function TRQLQuery.avg( Const f: TRQLQuery ): TRQLAvg;
Begin
  Result := TRQLAvg.Create( [ Self, FuncWrap(f) ] );
End;

Function TRQLQuery.avg( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLAvg;
Begin
  Result := TRQLAvg.Create( [ Self, r.expr(f) ] );
End;

Function TRQLQuery.min( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLMin;
Begin
  if isIndex
    Then Result := TRQLMin.Create( [ Self ], [ 'index', field_or_index ] )
    Else Result := TRQLMin.Create( [ Self, field_or_index ], [] );
End;

Function TRQLQuery.min( Const f: TRQLQuery ): TRQLMin;
Begin
  Result := TRQLMin.Create( [ Self, FuncWrap(f) ], [] );
End;

Function TRQLQuery.min( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMin;
Begin
  Result := TRQLMin.Create( [ Self, r.expr(f) ], [] );
End;

Function TRQLQuery.max( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLMax;
Begin
  if isIndex
    Then Result := TRQLMax.Create( [ Self ], [ 'index', field_or_index ] )
    Else Result := TRQLMax.Create( [ Self, field_or_index ], [] );
End;

Function TRQLQuery.max( Const f: TRQLQuery ): TRQLMax;
Begin
  Result := TRQLMax.Create( [ Self, FuncWrap(f) ], [] );
End;

Function TRQLQuery.max( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMax;
Begin
  Result := TRQLMax.Create( [ Self, r.expr(f) ], [] );
End;

Function TRQLTopLevelQuery.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  Result := '';
  For I := Low(Args) To High(Args)
    Do Result := Result + ', ' + Args[I];

  For I := Low(OptArgs) To High(OptArgs)
    Do Result := Result + ', ' + OptArgs[I].Key + '=' + OptArgs[I].Value;

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(', '));

  Result := 'r.' + FTypeString + '(' + Result + ')';
End;

Function TRQLMethodQuery.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  If Length(Args) = 0
    Then
      Begin
        Result := 'r.' + FTypeString + '()';
        Exit;
      End;

  If NeedsWrap( FArgs[0] )
    Then Args[0] := 'r.expr(' + Args[0] +')';

  Result := '';
  For I := Succ(Low(Args)) To High(Args)
    Do Result := Result + ', ' + Args[I];

  For I := Low(OptArgs) To High(OptArgs)
    Do Result := Result + ', ' + OptArgs[I].Key + '=' + OptArgs[I].Value;

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(', '));

  Result := Args[0] + '.' + FTypeString + '(' + Result + ')';
End;

Function TRQLBinaryOperatorQuery.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  Result := '';
  For I := Low(Args) To High(Args)
    Do If NeedsWrap( FArgs[I] )
        Then Result := Result + ' ' + FTypeString + ' r.expr(' + Args[I] + ')'
        Else Result := Result + ' ' + FTypeString + ' ' + Args[I];

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(' ' + FTypeString + ' '));

  Result := '(' + Result + ')';
End;

Function TRQLBooleanOperatorQuery.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  Result := '';
  For I := Low(Args) To High(Args)
    Do If NeedsWrap( FArgs[I] )
        Then Result := Result + ', r.expr(' + Args[I] + ')'
        Else Result := Result + ', ' + Args[I];

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(', '));

  Result := '(' + FTypeString + '(' + Result + ')';
End;

Function TRQLConstant.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  Result := 'r.' + FTypeString;
End;

Constructor TRQLFunc.Create( Func: TFunc<TRQLQuery> );
Begin
  Inherited Create( TERMTYPE_FUNC );
  SetLength(FArgs, 2);
  FArgs[0] := TRQLMakeArray.Create;
  FArgs[1] := r.expr( Func() );
End;

Constructor TRQLFunc.Create( Func: TFunc<TRQLQuery, TRQLQuery> );
Begin
  Inherited Create( TERMTYPE_FUNC );
  SetLength(FArgs, 2);
  FArgs[0] := TRQLMakeArray.Create( [ 1 ] );
  FArgs[1] := r.expr( Func(TRQLVar.Create( [ 1 ], [] )) );
End;

Constructor TRQLFunc.Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> );
Begin
  Inherited Create( TERMTYPE_FUNC );
  SetLength(FArgs, 2);
  FArgs[0] := TRQLMakeArray.Create( [ 1, 2 ] );
  FArgs[1] := r.expr( Func(TRQLVar.Create( [ 1 ], [] ), TRQLVar.Create( [ 2 ], [] )) );
End;

Constructor TRQLFunc.Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> );
Begin
  Inherited Create( TERMTYPE_FUNC );
  SetLength(FArgs, 2);
  FArgs[0] := TRQLMakeArray.Create( [ 1, 2, 3 ] );
  FArgs[1] := r.expr( Func(TRQLVar.Create( [ 1 ], [] ), TRQLVar.Create( [ 2 ], [] ), TRQLVar.Create( [ 3 ], [] )) );
End;

Constructor TRQLFunc.Create( Func: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> );
Begin
  Inherited Create( TERMTYPE_FUNC );
  SetLength(FArgs, 2);
  FArgs[0] := TRQLMakeArray.Create( [ 1, 2, 3, 4 ] );
  FArgs[1] := r.expr( Func(TRQLVar.Create( [ 1 ], [] ), TRQLVar.Create( [ 2 ], [] ), TRQLVar.Create( [ 3 ], [] ), TRQLVar.Create( [ 4 ], [] )) );
End;

Function TRQLFunc.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  Result :=  'lambda';
End;

(** Operators **)
Constructor TRQLOr.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_OR, 'or_', Args );
End;

Constructor TRQLAnd.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_AND, 'and_', Args );
End;

Constructor TRQLNot.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_NOT, '', Args );
End;

Constructor TRQLEq.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_EQ, '==', Args );
End;

Constructor TRQLNe.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_NE, '!=', Args );
End;

Constructor TRQLLt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_LT, '<', Args );
End;

Constructor TRQLLe.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_LE, '<=', Args );
End;

Constructor TRQLGt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GT, '>', Args );
End;

Constructor TRQLGe.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GE, '>=', Args );
End;

Constructor TRQLAdd.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_ADD, '+', Args );
End;

Constructor TRQLSub.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SUB, '-', Args );
End;

Constructor TRQLMul.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MUL, '*', Args );
End;

Constructor TRQLDiv.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DIV, '/', Args );
End;

Constructor TRQLMod.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MOD, '%', Args );
End;

Constructor TRQLFloor.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_FLOOR, 'floor', Args );
End;

Constructor TRQLCeil.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_CEIL, 'ceil', Args );
End;

Constructor TRQLRound.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_ROUND, 'round', Args );
End;

Constructor TRQLAppend.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_APPEND, 'append', Args );
End;

Constructor TRQLPrepend.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_PREPEND, 'prepend', Args );
End;

Constructor TRQLDifference.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DIFFERENCE, 'difference', Args );
End;

Constructor TRQLSetInsert.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SET_INSERT, 'setInsert', Args );
End;

Constructor TRQLSetUnion.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SET_UNION, 'setUnion', Args );
End;

Constructor TRQLSetIntersection.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SET_INTERSECTION, 'setIntersection', Args );
End;

Constructor TRQLSetDifference.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SET_DIFFERENCE, 'setDifference', Args );
End;

Constructor TRQLGetField.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET_FIELD, 'getField', Args, False );
End;

Constructor TRQLSkip.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SKIP, 'skip', Args );
End;

Constructor TRQLLimit.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_LIMIT, 'limit', Args );
End;

Constructor TRQLSlice.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_SLICE, 'slice', Args, OptArgs, True );
End;

Function TRQLSlice.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  If FBracketOperator
    Then
      Begin
        If NeedsWrap( FArgs[0] )
          Then Args[0] := 'r.expr(' + Args[0] + ')';
        Result := Args[0] + '[' + Args[1] + ':' + Args[2] + ']';
      End
    Else Result := Inherited Compose( Args, OptArgs );
End;

Constructor TRQLBracket.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_BRACKET, 'bracket', Args, True );
End;

Constructor TRQLContains.Create( Const Args: Array of Const);
Begin
  Inherited Create( TERMTYPE_CONTAINS, 'contains', Args );
End;

Constructor TRQLHasFields.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_HAS_FIELDS, 'hasFields', obj, Args );
End;

Constructor TRQLWithFields.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_WITH_FIELDS, 'withFields', obj, Args );
End;

Constructor TRQLKeys.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_KEYS, 'keys', Args );
End;

Constructor TRQLObject.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_OBJECT, 'object', Args, OptArgs );
End;

Constructor TRQLPluck.Create( Const Obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_PLUCK, 'pluck', Obj, Args );
End;

Constructor TRQLWithout.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_WITH_FIELDS, 'without', Obj, Args );
End;

Constructor TRQLMerge.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MERGE, 'merge', Args );
End;

Constructor TRQLBetween.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_BETWEEN, 'between', Args, OptArgs );
End;

Constructor TRQLGet.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET, 'get', Args );
End;

Constructor TRQLGetAll.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET_ALL, 'getAll', Args, OptArgs );
End;

Constructor TRQLGetAll.Create( Const Obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET_ALL, 'getAll', Obj, Args );
End;

Constructor TRQLGetAll.Create( Const Obj: TRQLQuery; Const Args: Array of String );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_GET_ALL, 'getAll' );
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := obj;
  For I := Low(Args) To High(Args)
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLGetIntersecting.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET_INTERSECTING, 'getIntersecting', Args, OptArgs );
End;

Constructor TRQLGetNearest.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_GET_NEAREST, 'getNearest', Args, OptArgs );
End;

Constructor TRQLReduce.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_REDUCE, 'reduce', Args );
End;

Constructor TRQLSum.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SUM, 'sum', Args );
End;

Constructor TRQLAvg.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_AVG, 'avg', Args );
End;

Constructor TRQLMin.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_MIN, 'min', Args, OptArgs );
End;

Constructor TRQLMax.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_MAX, 'max', Args, OptArgs );
End;

Constructor TRQLMap.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_MAP, 'map', Args, OptArgs );
End;

Constructor TRQLMap.Create( Const Args: Array of TRQLQuery );
Begin
  Inherited Create( TERMTYPE_MAP, 'map', Args );
End;

Constructor TRQLFilter.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_FILTER, 'filter', Args, OptArgs );
End;

Constructor TRQLConcatMap.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_CONCAT_MAP, 'concatMap', Args );
End;

Constructor TRQLOrderBy.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_ORDER_BY, 'orderBy', Args, OptArgs );
End;

Constructor TRQLOrderBy.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_ORDER_BY, 'orderBy', Obj, Args );
End;

Constructor TRQLOrderBy.Create( Const obj: TRQLQuery; Const Args: Array of String );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_ORDER_BY, 'orderBy' );
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := obj;
  For I := Low(Args) to High(Args)
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLDistinct.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_DISTINCT, 'distinct', Args, OptArgs );
End;

Constructor TRQLCount.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_COUNT, 'count', Args );
End;

Constructor TRQLUnion.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_UNION, 'union', Args );
End;

Constructor TRQLUnion.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_UNION, 'union',Obj, Args );
End;

Constructor TRQLUnion.Create( Const obj: TRQLQuery; Const Args: Array of TRQLQuery );
Begin
  Inherited Create( TERMTYPE_UNION, 'union', Obj, Args );
End;

Constructor TRQLNth.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_NTH, 'nth', Args, False );
End;

Constructor TRQLMatch.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MATCH, 'match', Args );
End;

Constructor TRQLToJSONString.Create( Const Args: Array of Const);
Begin
  Inherited Create( TERMTYPE_TO_JSON_STRING, 'toJsonString', Args );
End;

Constructor TRQLSplit.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SPLIT, 'split', Args );
End;

Constructor TRQLUpcase.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_UPCASE, 'upcase', Args );
End;

Constructor TRQLDowncase.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DOWNCASE, 'downcase', Args );
End;

Constructor TRQLOffsetsOf.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_OFFSETS_OF, 'offsetsOf', Args );
End;

Constructor TRQLIsEmpty.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_IS_EMPTY, 'isEmpty', Args );
End;

Constructor TRQLGroup.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_GROUP, 'group', Args, OptArgs );
End;

Constructor TRQLInnerJoin.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INNER_JOIN, 'innerJoin', Args );
End;

Constructor TRQLOuterJoin.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_OUTER_JOIN, 'outerJoin', Args );
End;

Constructor TRQLEqJoin.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_EQ_JOIN, 'eqJoin', Args, OptArgs );
End;

Constructor TRQLZip.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_ZIP, 'zip', Args );
End;

Constructor TRQLCoerceTo.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_COERCE_TO, 'coerceTo', Args );
End;

Constructor TRQLUngroup.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_UNGROUP, 'ungroup', Args );
End;

Constructor TRQLTypeOf.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TYPE_OF, 'typeOf', Args );
End;

Constructor TRQLUpdate.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_UPDATE, 'update', Args, OptArgs );
End;

Constructor TRQLDelete.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_DELETE, 'delete', Args, OptArgs );
End;

Constructor TRQLReplace.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_REPLACE, 'replace', Args, OptArgs );
End;

Constructor TRQLInsert.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_INSERT, 'insert', Args, OptArgs );
End;

Constructor TRQLInsert.Create( Const Obj: TRQLQuery; Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_INSERT, 'insert', Obj, Args, OptArgs );
End;

Constructor TRQLInsert.Create( Const Obj: TRQLQuery; Const Args: Array of TJSONValue; Const OptArgs: Array of Const );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_INSERT, 'insert', [ obj ], OptArgs );
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := obj;
  For I := Low(Args) to High(Args)
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLInsert.Create( Const Obj: TRQLQuery; Const Args: Array of TRQLQuery; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_INSERT, 'insert', Obj, Args, OptArgs );
End;

Constructor TRQLIndexCreate.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_CREATE, 'indexCreate', Args, OptArgs );
End;

Constructor TRQLIndexDrop.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_DROP, 'indexDrop', Args );
End;

Constructor TRQLIndexRename.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_RENAME, 'indexRename', Args, OptArgs );
End;

Constructor TRQLIndexList.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_LIST, 'indexList', Args );
End;

Constructor TRQLIndexStatus.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_STATUS, 'indexStatus', Args );
End;

Constructor TRQLIndexStatus.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_STATUS, 'indexStatus', Obj, Args );
End;

Constructor TRQLIndexStatus.Create( Const obj: TRQLQuery; Const Args: Array of String );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_INDEX_STATUS, 'indexStatus');
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := obj;
  For I := Low(Args) to High(Args)
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLIndexWait.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_WAIT, 'indexWait', Args );
End;

Constructor TRQLIndexWait.Create( Const obj: TRQLQuery; Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INDEX_WAIT, 'indexWait', Obj, Args );
End;

Constructor TRQLIndexWait.Create( Const obj: TRQLQuery; Const Args: Array of String );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_INDEX_WAIT, 'indexWait' );
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := obj;
  For I := Low(Args) to High(Args)
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLForEach.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_FOR_EACH, 'forEach', Args );
End;

Constructor TRQLInsertAt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INSERT_AT, 'insertAt', Args );
End;

Constructor TRQLSpliceAt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SPLICE_AT, 'spliceAt', Args );
End;

Constructor TRQLDeleteAt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DELETE_AT, 'deleteAt', Args );
End;

Constructor TRQLChangeAt.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_CHANGE_AT, 'changeAt', Args );
End;

Constructor TRQLSample.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SAMPLE, 'sample', Args );
End;

Constructor TRQLRange.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_RANGE, 'range', Args, OptArgs );
End;

Constructor TRQLToISO8601.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TO_ISO8601, 'toIso8601', Args );
End;

Constructor TRQLDuring.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_DURING, 'during', Args, OptArgs );
End;

Constructor TRQLDate.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DATE, 'date', Args );
End;

Constructor TRQLTimeOfDay.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TIME_OF_DAY, 'timeOfDay', Args );
End;

Constructor TRQLTimezone.Create( Const Args: Array of Const);
Begin
  Inherited Create( TERMTYPE_TIMEZONE, 'timezone', Args );
End;

Constructor TRQLYear.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_YEAR, 'year', Args );
End;

Constructor TRQLMonth.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MONTH, 'month', Args );
End;

Constructor TRQLDay.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DAY, 'day', Args );
End;

Constructor TRQLDayOfWeek.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DAY_OF_WEEK, 'dayOfWeek', Args );
End;

Constructor TRQLDayOfYear.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DAY_OF_YEAR, 'dayOfYear', Args );
End;

Constructor TRQLHours.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_HOURS, 'hours', Args );
End;

Constructor TRQLMinutes.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_MINUTES, 'minutes', Args );
End;

Constructor TRQLSeconds.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SECONDS, 'seconds', Args );
End;

Constructor TRQLTime.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_TIME, 'time', Args, OptArgs );
End;

Constructor TRQLISO8601.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_ISO8601, 'iso8601', Args, OptArgs );
End;

Constructor TRQLEpochTime.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_EPOCH_TIME, 'epochTime', Args, OptArgs );
End;

Constructor TRQLNow.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_NOW, 'now', Args, OptArgs );
End;

Constructor TRQLInTimezone.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_IN_TIMEZONE, 'inTimezone', Args );
End;

Constructor TRQLToEpochTime.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TO_EPOCH_TIME, 'toEpochTime', Args );
End;

Constructor TRQLGeoJson.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_GEOJSON, 'geojson', Args );
End;

Constructor TRQLToGeoJson.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TO_GEOJSON, 'toGeojson', Args );
End;

Constructor TRQLPoint.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_POINT, 'point', Args );
End;

Constructor TRQLLine.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_LINE, 'line', Args );
End;

Constructor TRQLLine.Create( Const Args: Array of TRQLQuery );
Begin
  Inherited Create( TERMTYPE_LINE, 'line', Args );
End;

Constructor TRQLPolygon.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_POLYGON, 'polygon', Args );
End;

Constructor TRQLPolygon.Create( Const Args: Array of TRQLQuery );
Begin
  Inherited Create( TERMTYPE_POLYGON, 'polygon', Args );
End;

Constructor TRQLDistance.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_DISTANCE, 'distance', Args, OptArgs );
End;

Constructor TRQLIntersects.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INTERSECTS, 'intersects', Args );
End;

Constructor TRQLIncludes.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INCLUDES, 'includes', Args );
End;

Constructor TRQLCircle.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_CIRCLE, 'circle', Args, OptArgs );
End;

Constructor TRQLFill.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_FILL, 'fill', Args );
End;

Constructor TRQLPolygonSub.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_POLYGON_SUB, 'polygonSub', Args );
End;

Constructor TRQLAsc.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_ASC, 'asc', Args, OptArgs );
End;

Constructor TRQLDesc.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_DESC, 'desc', Args, OptArgs );
End;

Constructor TRQLLiteral.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_LITERAL, 'literal', Args, OptArgs );
End;

(** Datum **)
Constructor TRQLDatum.Create( Value: TJSONValue );
Begin
  Inherited Create( TERMTYPE_DATUM );
  FValue := Value;
End;

Function TRQLDatum.Build: TJSONValue;
Begin
  Result := FValue;
End;

Function TRQLDatum.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  Result := JSONToString(FValue);
End;

(** Binary **)
Constructor TRQLBinary.Create( Stream: TStream );
Begin
  Inherited Create( TERMTYPE_BINARY, 'binary' );
  FData := UTF8Encode(TIdEncoderMIME.EncodeStream( Stream ));
End;

Constructor TRQLBinary.Create( Data: TIdBytes );
Begin
  Inherited Create( TERMTYPE_BINARY, 'binary' );
  FData := UTF8Encode(TIdEncoderMIME.EncodeBytes( Data ));
End;

Constructor TRQLBinary.Create( Data : TRQLQuery );
Begin
  Inherited Create( TERMTYPE_BINARY, 'binary', [ Data ], [] );
End;

Constructor TRQLBinary.Create( Data : String );
Begin
  Inherited Create( TERMTYPE_BINARY, 'binary' );
  FData := UTF8Encode(TIdEncoderMIME.EncodeString( Data, IndyUTF8Encoding ));
End;

Function TRQLBinary.Build: TJSONValue;
Begin
  If Length(FArgs) = 0
    Then
      Begin
        Result := TJSONObject.Create;
        (Result as TJSONObject).AddPair('$reql_type$', 'BINARY');
        (Result as TJSONObject).AddPair('data', FData);
      End
    Else Result := inherited Build;
End;

Function TRQLBinary.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  If Length(FArgs) = 0
    Then Result := 'r.' + FTypeString + '(bytes(<data>))'
    Else Result := inherited Compose(Args, OptArgs);
End;

Constructor TRQLBracketQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; BracketOperator: Boolean );
Begin
  Inherited Create( TermType, TypeString, Args );
  FBracketOperator := BracketOperator;
End;

Constructor TRQLBracketQuery.Create( TermType: TTermType; TypeString: String; Const Args: Array of Const; Const OptArgs: Array of Const; BracketOperator: Boolean  );
Begin
  Inherited Create( TermType, TypeString, Args, OptArgs );
  FBracketOperator := BracketOperator;
End;

Function TRQLBracketQuery.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  If FBracketOperator
    Then
      Begin
        If NeedsWrap( FArgs[0] )
          Then Args[0] := 'r.expr(' + Args[0] + ')';
        Result := '';
        For I := Succ(Low(Args)) To High(Args)
          Do Result := Result + ', ' + Args[I];

        If Length(Result) > 0
          Then System.Delete(Result, 1, Length(','));
        Result := Args[0] + '[' + Result + ']';
      End
    Else Result := Inherited Compose( Args, OptArgs );
End;

Constructor TRQLFunCall.Create( Const Args: Array of Const );
Var I : Integer;
Begin
  Inherited Create( TERMTYPE_FUNCALL);
  SetLength(FArgs, Length(Args));
  FArgs[0] := r.expr( Args[High(Args)] );
  For I := Low(Args) To Pred(High(Args))
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLFunCall.Create( Const Args: Array of TRQLQuery );
Var I : Integer;
Begin
  Inherited Create( TERMTYPE_FUNCALL );
  SetLength(FArgs, Length(Args));
  FArgs[0] := r.expr( Args[High(Args)] );
  For I := Low(Args) To Pred(High(Args))
    Do FArgs[Succ(I)] := r.expr( Args[I] );
End;

Constructor TRQLFunCall.Create( Const Args: Array of TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery> );
Var I : Integer;
Begin
  Inherited Create( TERMTYPE_FUNCALL );
  SetLength(FArgs, Succ(Length(Args)));
  FArgs[0] := r.expr( f );
  For I := Low(Args) To Pred(High(Args))
    Do FArgs[Succ(I)] := Args[I];
End;

Function TRQLFunCall.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  If Length(Args) <> 2
    Then
      Begin
        Result := '';
        For I := Succ(Low(Args)) To High(Args)
          Do Result := Result + ', ' + Args[I];
        If Length(Result) > 0
          Then System.Delete(Result, 1, Length(','));
        Result := 'r.do(' + Result + ', ' + Args[0] + ')';
        Exit;
      End;

  If (FArgs[1] is TRQLDatum)
    Then Args[1] := 'r.expr(' + Args[1] + ')';

  Result := Args[1] + '.do(' + Args[0] + ')';
End;

(** Var **)
Constructor TRQLVar.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_VAR, '', Args, OptArgs );
End;

Function TRQLVar.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  Result := 'var_' + Args[0];
End;

Constructor TRQLImplicitVar.Create;
Begin
  Inherited Create( TERMTYPE_IMPLICIT_VAR );
End;

Constructor TRQLImplicitVar.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_IMPLICIT_VAR, '', Args, OptArgs );
End;

Function TRQLImplicitVar.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Begin
  Result := 'r.row';
End;

(** MakeArray **)
Constructor TRQLMakeArray.Create;
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_MAKE_ARRAY );
End;

Constructor TRQLMakeArray.Create( Const Args: Array of TRQLQuery );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_MAKE_ARRAY );
  SetLength(FArgs, Length(Args));
  For I := Low(Args) To High(Args)
    Do FArgs[I] := Args[I];
End;

Constructor TRQLMakeArray.Create( Const Arr: TJSONArray );
Var I: Integer; Item: TPair<String, TRQLQuery>;
Begin
  Inherited Create( TERMTYPE_MAKE_OBJ );
  SetLength( FArgs, Arr.Size );
  For I := 0 To Arr.Size - 1
    Do FArgs[I] := r.expr( Arr.Get(I) );
End;

Constructor TRQLMakeArray.Create( Const Arr: Array of Variant );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_MAKE_ARRAY );
  SetLength(FArgs, Length(Arr));
  For I := Low(Arr) To High(Arr)
    Do FArgs[I] :=  r.expr(Arr[I]);
End;

Constructor TRQLMakeArray.Create( Const Arr: Array of Const );
Begin
  Inherited Create( TERMTYPE_MAKE_ARRAY, '', Arr );
End;

Function TRQLMakeArray.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  Result := '';
  For I := Low(Args) To High(Args)
    Do Result := Result + ', ' + Args[I];

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(', '));

  Result := '[' + Result + ']';
End;

(** MakeObj **)
Constructor TRQLMakeObj.Create(Const Obj: TDictionary<String, TRQLQuery>);
Var I: Integer; Item: TPair<String, TRQLQuery>;
Begin
  Inherited Create( TERMTYPE_MAKE_OBJ );
  SetLength(FOptArgs, Obj.Count);
  I := 0;
  For Item In Obj Do
    Begin
      FOptArgs[I] := Item;
      Inc(I);
    End;
End;

Constructor TRQLMakeObj.Create( Const Obj: TJSONObject );
Var I: Integer;
Begin
  Inherited Create( TERMTYPE_MAKE_OBJ );
  SetLength(FOptArgs, Obj.Size);
  For I := 0 To Obj.Size - 1
    Do FOptArgs[I] := TPair<String, TRQLQuery>.Create( JSONToString(Obj.Get( I ).JsonString), r.expr( Obj.Get( I ).JsonValue ) );
End;

Constructor TRQLMakeObj.Create( Const Arr: Array of Const );
Var I, J: Integer;
Begin
  Inherited Create( TERMTYPE_MAKE_OBJ );
  SetLength(FOptArgs, Length(Arr) div 2);
  I := 0;
  J := 0;
  While I < Length(Arr) Do
    Begin
      FOptArgs[J] := TPair<String, TRQLQuery>.Create( VarRecToString( Arr[I] ), VarRecToRQLQuery( Arr[Succ(I)] ) );
      Inc(I, 2);
      Inc(J, 1);
    End;
End;

Function TRQLMakeObj.Build: TJSONValue;
Var Obj: TJSONObject; OptArg: TPair<String, TRQLQuery>;
Begin
  Obj := TJSONObject.Create;
  If Length(FOptArgs) > 0
    Then For OptArg in FOptArgs
      Do Obj.AddPair( OptArg.Key, OptArg.Value.Build );
  Result := Obj;
End;

Function TRQLMakeObj.Compose( Args: Array of String; OptArgs: Array of TPair<ShortString, String> ): String;
Var I: Integer;
Begin
  Result := '';
  For I := Low(OptArgs) To High(OptArgs)
    Do Result := Result + ', ' + OptArgs[I].Key + ': ' + OptArgs[I].Value;

  If Length(Result) > 0
    Then System.Delete(Result, 1, Length(', '));
  Result := 'r.expr({' + Result + '})';
End;

(** DB **)
Constructor TRQLDB.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DB, 'db', Args );
End;

Function TRQLDB.tableCreate( Const tableName: String; Const options: Array of Const ): TRQLTableCreate;
Begin
  Result := TRQLTableCreate.Create([ Self, tableName ], options);
End;

Function TRQLDB.tableDrop( Const tableName : String ): TRQLTableDrop;
Begin
  Result := TRQLTableDrop.Create([ Self, tableName ]);
End;

Function TRQLDB.tableList: TRQLTableList;
Begin
  Result := TRQLTableList.Create([ Self ]);
End;

Function TRQLDB.table( Const name: String ): TRQLTable;
Begin
  Result := TRQLTable.Create([ Self, name ], []);
End;

Function TRQLDB.table( Const name: String; Const options: Array of Const ): TRQLTable;
Begin
  Result := TRQLTable.Create([ Self, name ], options);
End;

Function TRQLDB.config: TRQLConfig;
Begin
  Result := TRQLConfig.Create([ Self ]);
End;

Function TRQLDB.rebalance: TRQLRebalance;
Begin
  Result := TRQLRebalance.Create([ Self ]);
End;

Function TRQLDB.reconfigure(Const shards: Integer; Const replicas: Integer; Const dry_run : Boolean = False ): TRQLReconfigure;
Begin
  Result := TRQLReconfigure.Create([self], ['shards', shards, 'replicas', replicas, 'dry_run', dry_run]);
End;

Function TRQLDB.reconfigure(Const shards: Integer; Const replicas: Array of Const; Const primary_replica_tag: String; Const dry_run : Boolean = False ): TRQLReconfigure;
Begin
  Result := TRQLReconfigure.Create([self], ['shards', shards, 'replicas', r.expr(replicas), 'primary_replica_tag', primary_replica_tag, 'dry_run', dry_run]);
End;

Function TRQLDB.wait(Const wait_for : String = 'ready_for_writes'; Const timeout: Integer = -1): TRQLWait;
Begin
  If timeout < 0
    Then Result := TRQLWait.Create([self], ['wait_for', wait_for])
    Else Result := TRQLWait.Create([self], ['wait_for', wait_for, 'timeout', timeout]);
End;

(** Table **)
Constructor TRQLTable.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE, 'table', Args, OptArgs );
End;

Function TRQLTable.insert( Const item: TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert;
Begin
  Result := TRQLInsert.Create( [ Self, FuncWrap(item) ], [ 'durability', durability, 'return_changes', returnChanges, 'conflict', conflict ] );
End;

Function TRQLTable.insert( Const item: TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert;
Begin
  Result := TRQLInsert.Create( [ Self, item ], [ 'durability', durability, 'return_changes', returnChanges, 'conflict', conflict ] );
End;

Function TRQLTable.insert( Const items: Array of Const; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert;
Begin
  Result := TRQLInsert.Create( Self, items, [ 'durability', durability, 'return_changes', returnChanges, 'conflict', conflict ] );
End;

Function TRQLTable.insert( Const items: Array of TRQLQuery; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert;
Begin
  Result := TRQLInsert.Create( Self, items, [ 'durability', durability, 'return_changes', returnChanges, 'conflict', conflict ] );
End;

Function TRQLTable.insert( Const items: Array of TJSONValue; durability: String = 'hard'; returnChanges : Boolean = False; conflict: String = 'error' ): TRQLInsert;
Begin
  Result := TRQLInsert.Create( Self, items, [ 'durability', durability, 'return_changes', returnChanges, 'conflict', conflict ] );
End;

Function TRQLTable.get( Const key: String ): TRQLGet;
Begin
  Result := TRQLGet.Create([ Self, key ]);
End;

Function TRQLTable.getAll( Const field_or_index: String; Const isIndex : Boolean = False ): TRQLGetAll;
Begin
  if isIndex
    Then Result := TRQLGetAll.Create( [ Self ], [ 'index', field_or_index ] )
    Else Result := TRQLGetAll.Create( [ Self, field_or_index ], [] );
End;

Function TRQLTable.getAll( Const keys: Array of Const ): TRQLGetAll;
Begin
  Result := TRQLGetAll.Create( Self, keys );
End;

Function TRQLTable.getAll( Const keys: Array of String ): TRQLGetAll;
Begin
  Result := TRQLGetAll.Create( Self, keys );
End;

Function TRQLTable.indexCreate( Const indexName: String; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate;
Begin
  If multi
    Then
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName ], [ 'multi', multi, 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName ], [ 'multi', multi ] )
      End
    Else
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName ], [ 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName ], [] )
      End;
End;

Function TRQLTable.indexCreate( Const indexName: String; Const f : TFunc<TRQLQuery, TRQLQuery>; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate;
Begin
  If multi
    Then
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName, r.expr(f) ], [ 'multi', multi, 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName, r.expr(f) ], [ 'multi', multi ] )
      End
    Else
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName, r.expr(f) ], [ 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName, r.expr(f) ], [] )
      End;
End;

Function TRQLTable.indexCreate( Const indexName: String;  Const f : TRQLQuery; Const multi: Boolean = False; Const geo: Boolean = False ): TRQLIndexCreate;
Begin
  If multi
    Then
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName, FuncWrap(f) ], [ 'multi', multi, 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName, FuncWrap(f) ], [ 'multi', multi ] )
      End
    Else
      Begin
        If geo
          Then Result := TRQLIndexCreate.Create( [ Self, indexName, FuncWrap(f) ], [ 'geo', geo ] )
          Else Result := TRQLIndexCreate.Create( [ Self, indexName, FuncWrap(f) ], [] )
      End;
End;

Function TRQLTable.indexDrop( Const indexName: String ): TRQLIndexDrop;
Begin
  Result := TRQLIndexDrop.Create([ Self, indexName ]);
End;

Function TRQLTable.indexRename( Const oldName, newName: String; Const overwrite: Boolean = False ): TRQLIndexRename;
Begin
  If overwrite
    Then Result := TRQLIndexRename.Create([ Self, oldName, newName ], [ 'overwrite', overwrite ])
    Else Result := TRQLIndexRename.Create([ Self, oldName, newName ], []);
End;

Function TRQLTable.indexList: TRQLIndexList;
Begin
  Result := TRQLIndexList.Create([ Self ]);
End;

Function TRQLTable.indexStatus: TRQLIndexStatus;
Begin
  Result := TRQLIndexStatus.Create([ Self ]);
End;

Function TRQLTable.indexStatus( Const index: String ): TRQLIndexStatus;
Begin
  Result := TRQLIndexStatus.Create([ Self, index ]);
End;

Function TRQLTable.indexStatus( Const indexes: Array of Const ): TRQLIndexStatus;
Begin
  Result := TRQLIndexStatus.Create( Self, indexes );
End;

Function TRQLTable.indexStatus( Const indexes: Array of String ): TRQLIndexStatus;
Begin
  Result := TRQLIndexStatus.Create( Self, indexes );
End;

Function TRQLTable.indexWait: TRQLIndexWait;
Begin
  Result := TRQLIndexWait.Create([ Self ]);
End;

Function TRQLTable.indexWait( Const index: String ): TRQLIndexWait;
Begin
  Result := TRQLIndexWait.Create([ Self, index ]);
End;

Function TRQLTable.indexWait( Const indexes: Array of Const ): TRQLIndexWait;
Begin
  Result := TRQLIndexWait.Create( Self, indexes );
End;

Function TRQLTable.indexWait( Const indexes: Array of String ): TRQLIndexWait;
Begin
  Result := TRQLIndexWait.Create( Self, indexes );
End;

Function TRQLTable.uuid: TRQLUUID;
Begin
  Result := TRQLUUID.Create([ Self ]);
End;

Function TRQLTable.status: TRQLStatus;
Begin
  Result := TRQLStatus.Create([ Self ]);
End;

Function TRQLTable.config: TRQLConfig;
Begin
  Result := TRQLConfig.Create([ Self ]);
End;

Function TRQLTable.rebalance: TRQLRebalance;
Begin
  Result := TRQLRebalance.Create([ Self ]);
End;

Function TRQLTable.sync: TRQLSync;
Begin
  Result := TRQLSync.Create([ Self ]);
End;

Function TRQLTable.getIntersecting( Const geometry: TRQLQuery; Const index: String ): TRQLGetIntersecting;
Begin
  Result := TRQLGetIntersecting.Create([ Self, geometry ], [ 'index', index ]);
End;

Function TRQLTable.getNearest( Const point: TRQLQuery; Const index: String; Const maxResults: Integer = 100; Const maxDist: Integer = 100000; Const geoSystem: String = 'WGS84'; Const geoUnit: String = 'm'): TRQLGetNearest;
Begin
  Result := TRQLGetNearest.Create([ Self, point ], [ 'index', index, 'max_results', maxResults, 'max_dist', maxDist, 'unit', geoUnit, 'geo_system', geoSystem ] );
End;

Function TRQLTable.reconfigure(Const shards: Integer; Const replicas: Integer; Const dryRun : Boolean = False ): TRQLReconfigure;
Begin
  if dryRun
    Then Result := TRQLReconfigure.Create([ Self ], ['shards', shards, 'replicas', replicas, 'dry_run', dryRun])
    Else Result := TRQLReconfigure.Create([ Self ], ['shards', shards, 'replicas', replicas])
End;

Function TRQLTable.reconfigure(Const shards: Integer; Const replicas: Array of Const; Const primaryReplicaTag: String; Const dryRun : Boolean = False ): TRQLReconfigure;
Var ReplicasObject: TConstArray; I: Integer;
Begin
  Result := TRQLReconfigure.Create([ Self ], ['shards', shards, 'replicas', ReplicasObject, 'primary_replica_tag', primaryReplicaTag, 'dry_run', dryRun]);
End;

Function TRQLTable.wait(Const wait_for : String = 'ready_for_writes'; Const timeout: Integer = -1): TRQLWait;
Begin
  If timeout < 0
    Then Result := TRQLWait.Create([ Self ], ['wait_for', wait_for])
    Else Result := TRQLWait.Create([ Self ], ['wait_for', wait_for, 'timeout', timeout]);
End;

Constructor TRQLTableList.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_LIST, 'table_list', Args );
End;

Constructor TRQLTableDrop.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_DROP, 'table_drop', Args );
End;

Constructor TRQLTableCreate.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_CREATE, 'table_create', Args, OptArgs );
End;

Constructor TRQLTableListTL.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_LIST, 'table_list', Args );
End;

Constructor TRQLTableDropTL.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_DROP, 'table_drop', Args );
End;

Constructor TRQLTableCreateTL.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_TABLE_CREATE, 'table_create', Args, OptArgs );
End;

Constructor TRQLDBCreate.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DB_CREATE, 'db_create', Args );
End;

Constructor TRQLDBDrop.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DB_DROP, 'db_drop', Args );
End;

Constructor TRQLDBList.Create;
Begin
  Inherited Create( TERMTYPE_DB_LIST, 'db_list' );
End;

Constructor TRQLArgs.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_ARGS, 'args', Args );
End;

Constructor TRQLJavascript.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_JAVASCRIPT, 'js', Args );
End;

Constructor TRQLJSON.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_JSON, 'json', Args );
End;

Constructor TRQLHTTP.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_HTTP, 'http', Args, OptArgs );
End;

Constructor TRQLUserError.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_ERROR, 'error', Args, OptArgs );
End;

Constructor TRQLRandom.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_RANDOM, 'random', Args, OptArgs );
End;

Constructor TRQLUUID.Create;
Begin
  Inherited Create( TERMTYPE_UUID, 'uuid' );
End;

Constructor TRQLUUID.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_UUID, 'uuid', Args );
End;

Constructor TRQLInfo.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_INFO, 'info', Args );
End;

Constructor TRQLChanges.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_CHANGES, 'changes', Args, OptArgs );
End;

Constructor TRQLDefault.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_DEFAULT, 'default', Args );
End;

Constructor TRQLConfig.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_CONFIG, 'config', Args );
End;

Constructor TRQLStatus.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_STATUS, 'status', Args );
End;

Constructor TRQLWait.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_WAIT, 'wait', Args, OptArgs );
End;

Constructor TRQLWaitTL.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_WAIT, 'wait', Args, OptArgs );
End;

Constructor TRQLReconfigure.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_RECONFIGURE, 'reconfigure', Args, OptArgs );
End;

Constructor TRQLReconfigureTL.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_RECONFIGURE, 'reconfigure', Args, OptArgs );
End;

Constructor TRQLRebalance.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_REBALANCE, 'rebalance', Args );
End;

Constructor TRQLRebalanceTL.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_REBALANCE, 'rebalance', Args, OptArgs );
End;

Constructor TRQLSync.Create( Const Args: Array of Const );
Begin
  Inherited Create( TERMTYPE_SYNC, 'sync', Args );
End;

Constructor TRQLBranch.Create( Const Args: Array of Const; Const OptArgs: Array of Const );
Begin
  Inherited Create( TERMTYPE_BRANCH, 'branch', Args, OptArgs );
End;

Class Function TRethinkDB.connect(host: String = 'localhost'; port: Word = 28015; db: String = 'test'; auth_key: String = ''; timeout: LongInt = 20): TRethinkDbConnection;
Begin
  Result := TRethinkDBConnection.Create( host, port, db, auth_key, timeout );
  Result.reconnect();
End;

Class Function TRethinkDB.db( Const dbName: String ): TRQLDB;
Begin
  Result := TRQLDB.Create([ dbName ]);
End;

Class Function TRethinkDB.table( Const name: String ): TRQLTable;
Begin
  Result := TRQLTable.Create([ name ], []);
End;

Class Function TRethinkDB.table( Const name: String; Const options: Array of Const ): TRQLTable;
Begin
  Result := TRQLTable.Create([ name ], options);
End;

Class Function TRethinkDB.tableCreate( Const tableName: String ): TRQLTableCreateTL;
Begin
  Result := TRQLTableCreateTL.Create([ tableName ], []);
End;

Class Function TRethinkDB.tableCreate( Const tableName: String; Const options: Array of Const ): TRQLTableCreateTL;
Begin
  Result := TRQLTableCreateTL.Create([ tableName ], options);
End;

Class Function TRethinkDB.tableDrop( Const tableName : String ): TRQLTableDropTL;
Begin
  Result := TRQLTableDropTL.Create([ tableName ]);
End;

Class Function TRethinkDB.tableList: TRQLTableListTL;
Begin
  Result := TRQLTableListTL.Create( [] );
End;

Class Function TRethinkDB.dbCreate( Const dbName: String ): TRQLQuery;
Begin
  Result := TRQLDBCreate.Create([ dbName ]);
End;

Class Function TRethinkDB.dbDrop( Const dbName: String ): TRQLQuery;
Begin
  Result := TRQLDBDrop.Create([ dbName ]);
End;

Class Function TRethinkDB.dbList: TRQLQuery;
Begin
  Result := TRQLDBList.Create;
End;

Class Function TRethinkDB.args( Const arr : Array of Variant ): TRQLArgs;
Begin
  Result := TRQLArgs.Create([ r.expr(arr) ]);
End;

Class Function TRethinkDB.json( Const jsonString: String): TRQLJSON;
Begin
  Result := TRQLJSON.Create([ jsonString ]);
End;

Class Function TRethinkDB.js( Const jsString: String ): TRQLJavascript;
Begin
  Result := TRQLJavascript.Create([ jsString ]);
End;

Class Function TRethinkDB.js( Const jsString: String; Const options: Array of Const ): TRQLJavascript;
Begin
  Result := TRQLJavascript.Create([ jsString ]);
End;

Class Function TRethinkDB.http( Const url: String ): TRQLHttp;
Begin
  Result := TRQLHttp.Create([url], []);
End;

Class Function TRethinkDB.http( Const url: String; Const options: Array of Const ): TRQLHttp;
Begin
  Result := TRQLHttp.Create([url], options);
End;

Class Function TRethinkDB.http( Const url: TRQLQuery ): TRQLHttp;
Begin
  Result := TRQLHttp.Create([ FuncWrap(url) ], []);
End;

Class Function TRethinkDB.http( Const url: TRQLQuery; Const options: Array of Const ): TRQLHttp;
Begin
  Result := TRQLHttp.Create([ FuncWrap(url) ], options);
End;

Class Function TRethinkDB.error( Const msg: String ): TRQLUserError;
Begin
  Result := TRQLUserError.Create([msg], []);
End;

Class Function TRethinkDB.random: TRQLRandom;
Begin
  Result := TRQLRandom.Create([], []);
End;

Class Function TRethinkDB.random( upper: Integer ): TRQLRandom;
Begin
  Result := TRQLRandom.Create([ upper ], []);
End;

Class Function TRethinkDB.random( lower: Integer; upper: Integer): TRQLRandom;
Begin
  Result := TRQLRandom.Create([ lower, upper ], []);
End;

Class Function TRethinkDB.random( upper: Double ): TRQLRandom;
Begin
  Result := TRQLRandom.Create([ upper ], [ 'float', True ]);
End;

Class Function TRethinkDB.random( lower: Double; upper: Double ): TRQLRandom;
Begin
  Result := TRQLRandom.Create([ lower, upper ], [ 'float', True ]);
End;

Class Function TRethinkDB.do_( Const f: TFunc<TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, 1 );
  arr[0] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Class Function TRethinkDB.do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, Succ(Length(args)) );
  For I := Low(Args) To High(Args)
    Do arr[I] := r.expr( Args[I] );
  arr[Length(args)] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Class Function TRethinkDB.do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, Succ(Length(args)) );
  For I := Low(Args) To High(Args)
    Do arr[I] := r.expr( Args[I] );
  arr[Length(args)] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Class Function TRethinkDB.do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, Succ(Length(args)) );
  For I := Low(Args) To High(Args)
    Do arr[I] := r.expr( Args[I] );
  arr[Length(args)] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Class Function TRethinkDB.do_( Const args: Array of Const; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLFunCall;
Var arr : Array of TRQLQuery; I: Integer;
Begin
  SetLength( arr, Succ(Length(args)) );
  For I := Low(Args) To High(Args)
    Do arr[I] := r.expr( Args[I] );
  arr[Length(args)] := r.expr( f );
  Result := TRQLFunCall.Create( arr );
End;

Class Function TRethinkDB.uuid: TRQLUUID;
Begin
  Result := TRQLUUID.Create;
End;

Class Function TRethinkDB.info: TRQLInfo;
Begin
  Result := TRQLInfo.Create( [ ] );
End;

Class Function TRethinkDB.range: TRQLRange;
Begin
  Result := TRQLRange.Create([ ], [ ]);
End;

Class Function TRethinkDB.range( endValue: Integer ): TRQLRange;
Begin
  Result := TRQLRange.Create([ endValue ], [ ]);
End;

Class Function TRethinkDB.range( startValue, endValue: Integer ): TRQLRange;
Begin
  Result := TRQLRange.Create([ startValue, endValue ], [ ]);
End;

Class Function TRethinkDB.literal( Const a: Array of Const ): TRQLLiteral;
Begin
  Result := TRQLLiteral.Create(a, [ ]);
End;

Class Function TRethinkDB.asc( Const key: String ): TRQLAsc;
Begin
  Result := TRQLAsc.Create([ key ], [ ]);
End;

Class Function TRethinkDB.asc( Const f: TRQLQuery ): TRQLAsc;
Begin
  Result := TRQLAsc.Create( [ FuncWrap(f) ] , [ ]);
End;

Class Function TRethinkDB.asc( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLAsc;
Begin
  Result := TRQLAsc.Create( [ r.expr(f) ] , [ ]);
End;

Class Function TRethinkDB.desc( Const key: String ): TRQLDesc;
Begin
  Result := TRQLDesc.Create([ key ], [ ]);
End;

Class Function TRethinkDB.desc( Const f: TRQLQuery ): TRQLDesc;
Begin
  Result := TRQLDesc.Create( [ FuncWrap(f) ] , [ ]);
End;

Class Function TRethinkDB.desc( Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLDesc;
Begin
  Result := TRQLDesc.Create( [ r.expr(f) ], [ ]);
End;

Class Function TRethinkDB.typeOf: TRQLTypeOf;
Begin
  Result := TRQLTypeOf.Create([]);
End;

Class Function TRethinkDB.rebalance: TRQLRebalanceTL;
Begin
  Result := TRQLRebalanceTL.Create([], []);
End;

Class Function TRethinkDB.reconfigure(Const shards: Integer; Const replicas: Integer; Const dryRun : Boolean = False ): TRQLReconfigureTL;
Begin
  Result := TRQLReconfigureTL.Create([ ], ['shards', shards, 'replicas', replicas, 'dry_run', dryRun]);
End;

Class Function TRethinkDB.reconfigure(Const shards: Integer; Const replicas: Array of Const; Const primaryReplicaTag: String; Const dryRun : Boolean = False ): TRQLReconfigureTL;
Var ReplicasObject: TConstArray; I: Integer;
Begin
  Result := TRQLReconfigureTL.Create([ ], ['shards', shards, 'replicas', r.expr(replicas), 'primary_replica_tag', primaryReplicaTag, 'dry_run', dryRun]);
End;

Class Function TRethinkDB.wait(Const waitFor : String = 'ready_for_writes'; Const timeout: Integer = -1): TRQLWaitTL;
Begin
  If timeout < 0
    Then Result := TRQLWaitTL.Create([ ], ['wait_for', waitFor])
    Else Result := TRQLWaitTL.Create([ ], ['wait_for', waitFor, 'timeout', timeout]);
End;

Class Function TRethinkDB.row: TRQLImplicitVar;
Begin
  Result := TRQLImplicitVar.Create;
End;

Class Function TRethinkDB.branch( Const test, trueBranch, falseBranch: TRQLQuery ): TRQLBranch;
Begin
  Result := TRQLBranch.Create([ test, trueBranch, falseBranch ], [ ]);
End;

Class Function TRethinkDB.map( Const sequence: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery> ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ sequence, r.expr(f)], [] );
End;

Class Function TRethinkDB.map( Const sequence1, sequence2: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ sequence1, sequence2, r.expr(f)], [] );
End;

Class Function TRethinkDB.map( Const sequence1, sequence2, sequence3: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ sequence1, sequence2, sequence3, r.expr(f)], [] );
End;

Class Function TRethinkDB.map( Const sequence1, sequence2, sequence3, sequence4: TRQLQuery; Const f: TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery> ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ sequence1, sequence2, sequence3, sequence4, r.expr(f)], [] );
End;

Class Function TRethinkDB.map( Const sequence, f: TRQLQuery ): TRQLMap;
Begin
  Result := TRQLMap.Create( [ sequence, FuncWrap(f)], [] );
End;

Class Function TRethinkDB.map( Const args: Array of TRQLQuery ): TRQLMap;
Begin
  Result := TRQLMap.Create( args );
End;

Class Function TRethinkDB.object_( Const a: Array of Const ): TRQLObject;
Begin
   Result := TRQLObject.Create( a, [ ]);
End;

Class Function TRethinkDB.binary( Const stream: TStream ): TRQLBinary;
Begin
  Result := TRQLBinary.Create( Stream );
End;

Class Function TRethinkDB.binary( Const data: TIdBytes ): TRQLBinary;
Begin
  Result := TRQLBinary.Create( Data );
End;

Class Function TRethinkDB.binary( Const data : TRQLQuery ): TRQLBinary;
Begin
  Result := TRQLBinary.Create( Data );
End;

Class Function TRethinkDB.binary( Const data : UTF8String ): TRQLBinary;
Begin
  Result := TRQLBinary.Create( Data );
End;

Class Function TRethinkDB.eq( Const first, second: TRQLQuery ): TRQLEq;
Begin
  Result := TRQLEq.Create([ first, second ]);
End;

Class Function TRethinkDB.eq( Const first, second: Variant   ): TRQLEq;
Begin
  Result := TRQLEq.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.eq( Const args: TRQLArgs ): TRQLEq;
Begin
  Result := TRQLEq.Create([ args ]);
End;

Class Function TRethinkDB.eq(  Const args: Array of Const) : TRQLEq;
Begin
  Result := TRQLEq.Create( args );
End;

Class Function TRethinkDB.ne( Const first, second: TRQLQuery ): TRQLNe;
Begin
  Result := TRQLNe.Create([ first, second ]);
End;

Class Function TRethinkDB.ne( Const first, second: Variant   ): TRQLNe;
Begin
  Result := TRQLNe.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.ne( Const args: TRQLArgs ): TRQLNe;
Begin
  Result := TRQLNe.Create([ args ]);
End;

Class Function TRethinkDB.ne( Const args: Array of Const ): TRQLNe;
Begin
  Result := TRQLNe.Create( args );
End;

Class Function TRethinkDB.lt( Const first, second: TRQLQuery ): TRQLLt;
Begin
  Result := TRQLLt.Create([ first, second ]);
End;

Class Function TRethinkDB.lt( Const first, second: Variant ): TRQLLt;
Begin
  Result := TRQLLt.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.lt( Const args: TRQLArgs ): TRQLLt;
Begin
  Result := TRQLLt.Create([ args ]);
End;

Class Function TRethinkDB.lt( Const args: Array of Const ): TRQLLt;
Begin
  Result := TRQLLt.Create( args );
End;

Class Function TRethinkDB.le( Const first, second: TRQLQuery ): TRQLLe;
Begin
  Result := TRQLLe.Create([ first, second ]);
End;

Class Function TRethinkDB.le( Const first, second: Variant   ): TRQLLe;
Begin
  Result := TRQLLe.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.le( Const args: TRQLArgs ): TRQLLe;
Begin
  Result := TRQLLe.Create([ args ]);
End;

Class Function TRethinkDB.le( Const args: Array of Const ): TRQLLe;
Begin
  Result := TRQLLe.Create( args );
End;

Class Function TRethinkDB.gt( Const first, second: TRQLQuery ): TRQLGt;
Begin
  Result := TRQLGt.Create([ first, second ]);
End;

Class Function TRethinkDB.gt( Const first, second: Variant   ): TRQLGt;
Begin
  Result := TRQLGt.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.gt( Const args: TRQLArgs ): TRQLGt;
Begin
  Result := TRQLGt.Create([ args ]);
End;

Class Function TRethinkDB.gt( Const args: Array of Const ): TRQLGt;
Begin
  Result := TRQLGt.Create( args );
End;

Class Function TRethinkDB.ge( Const first, second: TRQLQuery ): TRQLGe;
Begin
  Result := TRQLGe.Create([ first, second ]);
End;

Class Function TRethinkDB.ge( Const first, second: Variant   ): TRQLGe;
Begin
  Result := TRQLGe.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.ge( Const args: TRQLArgs ): TRQLGe;
Begin
  Result := TRQLGe.Create([ args ]);
End;

Class Function TRethinkDB.ge( Const args: Array of Const ): TRQLGe;
Begin
  Result := TRQLGe.Create( args );
End;

Class Function TRethinkDB.add( Const first, second: TRQLQuery ): TRQLAdd;
Begin
  Result := TRQLAdd.Create([ first, second ]);
End;

Class Function TRethinkDB.add( Const first, second: Variant   ): TRQLAdd;
Begin
  Result := TRQLAdd.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.add( Const args: TRQLArgs ): TRQLAdd;
Begin
  Result := TRQLAdd.Create([ args ]);
End;

Class Function TRethinkDB.add( Const args: Array of Const ): TRQLAdd;
Begin
  Result := TRQLAdd.Create( args );
End;

Class Function TRethinkDB.sub( Const first, second: TRQLQuery ): TRQLSub;
Begin
  Result := TRQLSub.Create([ first, second ]);
End;

Class Function TRethinkDB.sub( Const first, second: Variant   ): TRQLSub;
Begin
  Result := TRQLSub.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.sub( Const args: TRQLArgs ): TRQLSub;
Begin
  Result := TRQLSub.Create([ args ]);
End;

Class Function TRethinkDB.sub( Const args: Array of Const ): TRQLSub;
Begin
  Result := TRQLSub.Create( args );
End;

Class Function TRethinkDB.mul( Const first, second: TRQLQuery ): TRQLMul;
Begin
  Result := TRQLMul.Create([ first, second ]);
End;

Class Function TRethinkDB.mul( Const first, second: Variant   ): TRQLMul;
Begin
  Result := TRQLMul.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.mul( Const args: TRQLArgs ): TRQLMul;
Begin
  Result := TRQLMul.Create([ args ]);
End;

Class Function TRethinkDB.mul( Const args: Array of Const ): TRQLMul;
Begin
  Result := TRQLMul.Create( args );
End;

Class Function TRethinkDB.div_( Const first, second: TRQLQuery ): TRQLDiv;
Begin
  Result := TRQLDiv.Create([ first, second ]);
End;

Class Function TRethinkDB.div_( Const first, second: Variant   ): TRQLDiv;
Begin
  Result := TRQLDiv.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.div_( Const args: TRQLArgs ): TRQLDiv;
Begin
  Result := TRQLDiv.Create([ args ]);
End;

Class Function TRethinkDB.div_( Const args: Array of Const ): TRQLDiv;
Begin
  Result := TRQLDiv.Create( args );
End;

Class Function TRethinkDB.mod_( Const first, second: TRQLQuery ): TRQLMod;
Begin
  Result := TRQLMod.Create([ first, second ]);
End;

Class Function TRethinkDB.mod_( Const first, second: Variant   ): TRQLMod;
Begin
  Result := TRQLMod.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.mod_( Const args: TRQLArgs ): TRQLMod;
Begin
  Result := TRQLMod.Create([ args ]);
End;

Class Function TRethinkDB.mod_( Const args: Array of Const ): TRQLMod;
Begin
  Result := TRQLMod.Create( args );
End;

Class Function TRethinkDB.and_( Const first, second: TRQLQuery ): TRQLAnd;
Begin
  Result := TRQLAnd.Create([ first, second ]);
End;

Class Function TRethinkDB.and_( Const first, second: Variant   ): TRQLAnd;
Begin
  Result := TRQLAnd.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.and_( Const args: TRQLArgs ): TRQLAnd;
Begin
  Result := TRQLAnd.Create([ args ]);
End;

Class Function TRethinkDB.and_( Const args: Array of Const ): TRQLAnd;
Begin
  Result := TRQLAnd.Create( args );
End;

Class Function TRethinkDB.or_( Const first, second: TRQLQuery ): TRQLOr;
Begin
  Result := TRQLOr.Create([ first, second ]);
End;

Class Function TRethinkDB.or_( Const first, second: Variant   ): TRQLOr;
Begin
  Result := TRQLOr.Create([ r.expr(first), r.expr(second) ]);
End;

Class Function TRethinkDB.or_( Const args: TRQLArgs ): TRQLOr;
Begin
  Result := TRQLOr.Create([ args ]);
End;

Class Function TRethinkDB.or_( Const args: Array of Const ): TRQLOr;
Begin
  Result := TRQLOr.Create( args );
End;

Class Function TRethinkDB.not_( Const value: TRQLQuery ): TRQLNot;
Begin
  Result := TRQLNot.Create([ value ]);
End;

Class Function TRethinkDB.not_( Const value: Variant   ): TRQLNot;
Begin
  Result := TRQLNot.Create([ r.expr(value) ]);
End;

Class Function TRethinkDB.floor( Const value: TRQLQuery ): TRQLFloor;
Begin
  Result := TRQLFloor.Create([ value ]);
End;

Class Function TRethinkDB.floor( Const value: Variant   ): TRQLFloor;
Begin
  Result := TRQLFloor.Create([ r.expr(value) ]);
End;

Class Function TRethinkDB.ceil( Const value: TRQLQuery ): TRQLCeil;
Begin
  Result := TRQLCeil.Create([ value ]);
End;

Class Function TRethinkDB.ceil( Const value: Variant   ): TRQLCeil;
Begin
  Result := TRQLCeil.Create([ r.expr(value) ]);
End;

Class Function TRethinkDB.round( Const value: TRQLQuery ): TRQLRound;
Begin
  Result := TRQLRound.Create([ value ]);
End;

Class Function TRethinkDB.round( Const value: Variant   ): TRQLRound;
Begin
  Result := TRQLRound.Create([ r.expr(value) ]);
End;

Class Function TRethinkDB.time( Const year, month, day: Word; Const timezone: String ): TRQLTime;
Begin
  Result := TRQLTime.Create([ year, month, day, timezone ], [ ]);
End;

Class Function TRethinkDB.time( Const year, month, day, hour, minutes, seconds: Word; Const timezone: String ): TRQLTime;
Begin
  Result := TRQLTime.Create([ year, month, day, hour, minutes, seconds, timezone ], [ ]);
End;

Class Function TRethinkDB.iso8601( Const iso8601Date: String; Const defaultTimezone: String = '' ): TRQLISO8601;
Begin
  If Length(defaultTimezone) > 0
    Then Result := TRQLISO8601.Create([ iso8601Date ], [ 'default_timezone', defaultTimezone ])
    Else Result := TRQLISO8601.Create([ iso8601Date ], [ ]);
End;

Class Function TRethinkDB.epochTime( Const t: double ): TRQLEpochTime;
Begin
  Result := TRQLEpochTime.Create([ t ], [ ]);
End;

Class Function TRethinkDB.now: TRQLNow;
Begin
  Result := TRQLNow.Create([ ], [ ]);
End;


Class Function TRethinkDB.monday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_MONDAY, 'monday');
End;

Class Function TRethinkDB.tuesday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_TUESDAY, 'tuesday');
End;

Class Function TRethinkDB.wednesday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_WEDNESDAY, 'wednesday');
End;

Class Function TRethinkDB.thursday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_THURSDAY, 'thursday');
End;

Class Function TRethinkDB.friday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_FRIDAY, 'friday');
End;

Class Function TRethinkDB.saturday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_SATURDAY, 'saturday');
End;

Class Function TRethinkDB.sunday: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_SUNDAY, 'sunday');
End;

Class Function TRethinkDB.january: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_JANUARY, 'january');
End;

Class Function TRethinkDB.february: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_FEBRUARY, 'february');
End;

Class Function TRethinkDB.march: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_MARCH, 'march');
End;

Class Function TRethinkDB.april: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_APRIL, 'april');
End;

Class Function TRethinkDB.may: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_MAY, 'may');
End;

Class Function TRethinkDB.june: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_JUNE, 'june');
End;

Class Function TRethinkDB.july: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_JULY, 'july');
End;

Class Function TRethinkDB.august: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_AUGUST, 'august');
End;

Class Function TRethinkDB.september: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_SEPTEMBER, 'september');
End;

Class Function TRethinkDB.october: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_OCTOBER, 'october');
End;

Class Function TRethinkDB.november: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_NOVEMBER, 'november');
End;

Class Function TRethinkDB.december: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_DECEMBER, 'december');
End;

Class Function TRethinkDB.minval: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_MINVAL, 'minval');
End;

Class Function TRethinkDB.maxval: TRQLConstant;
Begin
  Result := TRQLConstant.Create( TERMTYPE_MAXVAL, 'maxval');
End;

Class Function TRethinkDB.geojson(Const geojson: TJSONObject): TRQLGeoJson;
Begin
    Result := TRQLGeoJson.Create( [ geojson ] );
End;

Class Function TRethinkDB.point( Const longitude, latitude: Double ): TRQLPoint;
Begin
    Result := TRQLPoint.Create( [ longitude, latitude ] );
End;

Class Function TRethinkDB.line( Const point1, point2: TRQLQuery ): TRQLLine;
Begin
    Result := TRQLLine.Create( [ point1, point2 ] );
End;

Class Function TRethinkDB.line( Const points: Array of TRQLQuery ): TRQLLine;
Begin
    Result := TRQLLine.Create( points );
End;

Class Function TRethinkDB.polygon( Const point1, point2: TRQLQuery ): TRQLPolygon;
Begin
    Result := TRQLPolygon.Create( [ point1, point2 ] );
End;

Class Function TRethinkDB.polygon( Const points: Array of TRQLQuery ): TRQLPolygon;
Begin
    Result := TRQLPolygon.Create( points );
End;

Class Function TRethinkDB.distance( Const point1, point2: TRQLQuery; Const geoSystem: String = 'WGS84'; Const geoUnit: String = 'm'): TRQLDistance;
Begin
    Result := TRQLDistance.Create( [ point1, point2 ], [ 'geo_system', geoSystem, 'unit', geoUnit ] );
End;

Class Function TRethinkDB.intersects( Const point1, point2: TRQLQuery ): TRQLIntersects;
Begin
    Result := TRQLIntersects.Create( [ point1, point2 ]);
End;

Class Function TRethinkDB.circle( Const point: TRQLQuery; radius: Double; numVertices: Integer = 32; geoSystem: String = 'WGS84'; geoUnit: String = 'm'; fill: Boolean = True ): TRQLCircle;
Begin
    Result := TRQLCircle.Create( [ point, radius ], [ 'num_vertices', numVertices, 'geo_system', geoSystem, 'unit', geoUnit, 'fill', fill ] );
End;

(** expr **)
Class Function TRethinkDB.expr(Const q : TRQLQuery): TRQLQuery;
Begin
  Result := q;
End;

Class Function TRethinkDB.expr(Const v : Variant): TRQLQuery;
Begin
  Result := TRQLDatum.Create( VariantToJSONValue( v ) );
End;

Class Function TRethinkDB.expr(Const a : Array of Variant): TRQLQuery;
Var I : Integer; arr: Array of TRQLQuery;
Begin
  SetLength( arr, Length(a) );
  For I := Low(a) To High(a)
    Do arr[I] := expr( a[I] );
  Result := TRQLMakeArray.Create( arr );
End;

Class Function TRethinkDB.makeArray(Const a : Array of Const): TRQLQuery;
Var I : Integer; arr: Array of TRQLQuery;
Begin
  SetLength( arr, Length(a) );
  For I := Low(a) To High(a)
    Do arr[I] := expr( a[I] );
  Result := TRQLMakeArray.Create( arr );
End;

Class Function TRethinkDB.makeObject(Const a : Array of Const): TRQLQuery;
Begin
  Result := TRQLMakeObj.Create( a );
End;

Class Function TRethinkDB.expr(Const a : Array of Const): TRQLQuery;
Var IsObject: Boolean; I : Integer;
Begin
  If (Length(a) > 0) and (Length(a) mod 2 = 0)
    Then
      Begin
        IsObject := True;
        I := Low(a);
        While I < High(a) Do
          Begin
            If Not VarRecIsString( a[I] )
              Then
                Begin
                  IsObject := False;
                  Break;
                End;
            Inc(I, 2);
          End;
      End
    Else IsObject := False;

  If IsObject
    Then Result := makeObject( a )
    Else Result := makeArray( a );
End;

Class Function TRethinkDB.expr(Const a : TJSONValue): TRQLQuery;
Var I : Integer; arr: Array of TRQLQuery; obj: TDictionary<String, TRQLQuery>;
Begin
  If (a is TJSONArray)
    Then Result := TRQLMakeArray.Create( a as TJSONArray )
    Else If (a is TJSONObject)
      Then Result := TRQLMakeObj.Create( a as TJSONObject )
      Else Result := TRQLDatum.Create( a );
End;

Class Function TRethinkDB.expr(Const f : TFunc<TRQLQuery>): TRQLQuery;
Begin
  Result := TRQLFunc.Create( f );
End;

Class Function TRethinkDB.expr(Const f : TFunc<TRQLQuery, TRQLQuery>): TRQLQuery;
Begin
  Result := TRQLFunc.Create( f );
End;

Class Function TRethinkDB.expr(Const f : TFunc<TRQLQuery,TRQLQuery, TRQLQuery>): TRQLQuery;
Begin
  Result := TRQLFunc.Create( f );
End;

Class Function TRethinkDB.expr(Const f : TFunc<TRQLQuery, TRQLQuery, TRQLQuery, TRQLQuery>): TRQLQuery;
Begin
  Result := TRQLFunc.Create( f );
End;

Class Function TRethinkDB.expr(Const f : TFunc<TRQLQuery, TRQLQuery,TRQLQuery, TRQLQuery, TRQLQuery>): TRQLQuery;
Begin
  Result := TRQLFunc.Create( f );
End;

Class Function TRethinkDB.expr(Const v : TVarRec) : TRQLQuery;
Begin
  Result :=  VarRecToRQLQuery( v );
End;

Class Function TRethinkDB.expr(Const b; Count: Longint): TRQLQuery;
Var s: TMemoryStream;
Begin
  s := TMemoryStream.Create;
  s.Write(b, count);
  s.Position := 0;
  Result := TRQLBinary.Create( s );
  s.Free;
End;

Class Function TRethinkDB.expr(const b: TBytes): TRQLQuery;
Var s: TBytesStream;
Begin
  s := TBytesStream.Create( b );
  Result := TRQLBinary.Create( s );
  s.Free;
End;

Class Function TRethinkDB.expr(Const s : TStream): TRQLQuery;
Begin
  Result := TRQLBinary.Create( s );
End;

Class Function TRethinkDB.expr(Const d : TDateTime): TRQLQuery;
Begin
  With TXSDateTime.Create() Do
    Try
      AsDateTime := d;
      Result := TRQLISO8601.Create([ NativeToXS ], []);
    Finally
      Free();
    End;
End;

Class Function TRethinkDB.expr(Const d : TTime): TRQLQuery;
Begin
  With TXSDateTime.Create() Do
    Try
      AsDateTime := d;
      Result := TRQLISO8601.Create([ NativeToXS ], []);
    Finally
      Free();
    End;
End;

Class Function TRethinkDB.expr(Const d : TDate): TRQLQuery;
Begin
  With TXSDateTime.Create() Do
    Try
      AsDateTime := d;
      Result := TRQLISO8601.Create([ NativeToXS ], []);
    Finally
      Free();
    End;
End;

End.
