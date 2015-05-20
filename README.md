# Delphi RethinkDB Client driver

A Delphi client driver for [RethinkDB](http://rethinkdb.com/).

Fully supports all operations of the [RethinkDB API](http://rethinkdb.com/api) and is compliant with the JSON driver protocol and at least RethinkDB 1.15 ReQL terms.

## Example

    Procedure TestRethinkDB;
    Var conn : TRethinkDbConnection; res: TRQLResult;
    Begin
      conn := r.connect( 'localhost' );
      res := r.db('test').table_create('mytable').run( conn );
    End;

## Note

This driver uses the synchronous Indy socket library for networked communication. For application performance it is advised to run these operations in a non-gui thread, or make use of the TIdAntiFreeze component.

## Todo

 - API Documentation
 - Example project
 - Cursor and changefeed example
 - Multithreaded example
