/**
 * Copyright 2014 Sunny Gleason
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar Query;

options {
  output=AST;
}

tokens {
  AND  = 'and' ;
  EQ   = 'eq' ;
  NE   = 'ne' ;
  LT   = 'lt' ;
  LE   = 'le' ;
  GT   = 'gt' ;
  GE   = 'ge' ;
  IN   = 'in' ;
  TRUE = 'true' ;
  FALSE = 'false' ;
  NULL  = 'null' ;
  COMMA = ',' ;
  LPAREN = '(' ;
  RPAREN = ')' ; 
}

@lexer::header  {package io.kazuki.v0.store.index.query;}

@lexer::members {
  @Override
  public void reportError(RecognitionException e) {
    throw new IllegalArgumentException(e);
  }
}

@parser::header {package io.kazuki.v0.store.index.query;}

@members {
@Override
public void reportError(RecognitionException e) {
    throw new RuntimeException(e);
}
}


/*------------------------------------------------------------------
 * PARSER RULES
 *------------------------------------------------------------------*/

term_list [List<QueryTerm> inList] :
        term[$inList] (AND term[$inList])* -> (term)+;

term [List<QueryTerm> inList] :
        (f=field o=op^ v=value)             { inList.add(new QueryTerm(o.value, f.value, v.value)); }
        | (f=field IN vl=value_list[new ArrayList<ValueHolder>()])    { inList.add(new QueryTerm(QueryOperator.IN, f.value, vl.outList)); }
        ;

field returns [String value] :
        i=IDENT                            { $value = i.getText(); }
        ;

op returns [QueryOperator value] :
        EQ                                 { $value = QueryOperator.EQ; }
        | NE                               { $value = QueryOperator.NE; }
        | LT                               { $value = QueryOperator.LT; }
        | LE                               { $value = QueryOperator.LE; }
        | GT                               { $value = QueryOperator.GT; }
        | GE                               { $value = QueryOperator.GE; }
        ;

value returns [ValueHolder value] :
        s=STRING_LITERAL                   { $value = new ValueHolder(ValueType.STRING, s.getText()); }
        | i=INTEGER                        { $value = new ValueHolder(ValueType.INTEGER, i.getText()); }
        | d=DECIMAL                        { $value = new ValueHolder(ValueType.DECIMAL, d.getText()); }
        | TRUE                             { $value = new ValueHolder(ValueType.BOOLEAN, "true"); }
        | FALSE                            { $value = new ValueHolder(ValueType.BOOLEAN, "false"); }
        | NULL                             { $value = new ValueHolder(ValueType.NULL, "null"); }
        ;

value_in_list [List<ValueHolder> valueList] :
        v=value                            { valueList.add(v.value); }
        ;

value_list [List<ValueHolder> valueList] returns [ValueHolderList outList] :
        LPAREN value_in_list[$valueList] (COMMA value_in_list[$valueList])* RPAREN
        { $outList = new ValueHolderList($valueList); }
        ;

/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/

IDENT
  : ('_'|'a'..'z'|'A'..'Z') ('_'|'a'..'z'|'A'..'Z'|'0'..'9')*
  ;

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT : '0'..'9' ;
INTEGER : '-'? (DIGIT)+ ;
DECIMAL : '-'? (DIGIT)+ ('.' (DIGIT)+ )? ;


STRING_LITERAL
@init{StringLiteral sl = new StringLiteral();}
  :
  '"'
  ( escaped=ESC {sl.appendEscaped(escaped.getText());} | 
    normal= ~('"'|'\\'|'\n'|'\r') {sl.append(normal);} )* 
  '"'    
  {setText("\"" + sl.toString() + "\"");}
  ;

fragment
ESC : '\\' ( 'n' | 't' | '"' | '\\' ) ;

