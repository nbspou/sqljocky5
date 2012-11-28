part of sqljocky;

class ConnectionRequest {
  final Completer<_Connection> c;
  final bool retain;
  
  ConnectionRequest(this.c, this.retain);
}

class ConnectionPool {
  final Logger log;

  final String _host;
  final int _port;
  final String _user;
  final String _password;
  final String _db;
  
  int _max;
  
  final Queue<ConnectionRequest> _pendingConnections;
  final List<_Connection> _pool;
  
  addPendingConnection(Completer<_Connection> pendingConnection, bool retain) {
    _pendingConnections.add(new ConnectionRequest(pendingConnection, retain));
  }

  ConnectionPool({String host: 'localhost', int port: 3306, String user, 
      String password, String db, int max: 5}) :
        _pendingConnections = new Queue<ConnectionRequest>(),
        _pool = new List<_Connection>(),
        _host = host,
        _port = port,
        _user = user,
        _password = password,
        _db = db,
        _max = max,
        log = new Logger("ConnectionPool");
  
  Future<_Connection> _getConnection({bool retain: false}) {
    log.finest("Getting a connection");
    var c = new Completer<_Connection>();

    var inUseCount = 0;
    for (var cnx in _pool) {
      if (cnx.inUse) {
        inUseCount++;
      }
    }
    log.finest("Number of in-use connections: $inUseCount");
    
    for (var cnx in _pool) {
      if (!cnx.inUse) {
        log.finest("Reusing existing pooled connection #${cnx.number}");
        print("handler: ${cnx._handler}");
        cnx.use();
        c.complete(cnx);
        return c.future;
      }
    }
    
    if (_pool.length < _max) {
      log.finest("Creating new pooled connection #${_pool.length}");
      _Connection cnx = new _Connection(this, _pool.length);
      var future = cnx.connect(
          host: _host, 
          port: _port, 
          user: _user, 
          password: _password, 
          db: _db);
      cnx.use();
      _pool.add(cnx);
      future.then((x) {
        log.finest("Logged in on connection #${cnx.number}, use original retain settings now");
        c.complete(cnx);
      });
      handleFutureException(future, c, cnx);
    } else {
      log.finest("Waiting for an available connection");
      addPendingConnection(c, retain);
    }
    return c.future;
  }
  
  releaseConnection(_Connection cnx) {
    cnx.release();
    log.finest("Finished with connection #${cnx.number}");
    if (!_pool.contains(cnx)) {
      print("_connectionFinishedWith handler called for unmanaged connection");
      return;
    }
    
    if (_pendingConnections.length > 0) {
      log.finest("Reusing connection #${cnx.number} for a queued operation");
      var c = _pendingConnections.removeFirst();
    } else {
      log.finest("Marking pooled connection #${cnx.number} as not in use");
    }
  }
  
  // dangerous - would need to switch all connections
//  Future useDatabase(String dbName) {
//    var completer = new Completer();
//    
//    var cnxFuture = _getConnection();
//    cnxFuture.then((cnx) {
//      var handler = new UseDbHandler(dbName);
//      var future = _connection.processHandler(handler);
//      future.then((x) {
//        completer.completer();
//      });
//      handleFutureException(future, completer);
//    });
//    handleFutureException(cnxFuture, completer);
//    
//    return completer.future;
//  }
  
  void close() {
    for (_Connection cnx in _pool) {
      cnx.close();
      
    }
  }

  Future<Results> query(String sql) {
    log.info("Running query: ${sql}");
    var c = new Completer<Results>();
    
    var cnxFuture = _getConnection();
    cnxFuture.then((cnx) {
      log.fine("#${cnx.number} Got connection for query");
      var handler = new QueryHandler(sql);
      var queryFuture = cnx.processHandler(handler);
      queryFuture.then((results) {
        log.fine("#${cnx.number} Got query results for: ${sql}");
        releaseConnection(cnx);
        c.complete(results);
      });
      handleFutureException(queryFuture, c, cnx);
    });
    handleFutureException(cnxFuture, c);

    return c.future;
  }
  
  Future<int> update(String sql) {
  }
  
  Future ping() {
    log.info("Pinging server");
    var c = new Completer<Results>();
    
    var cnxFuture = _getConnection();
    cnxFuture.then((cnx) {
      var handler = new PingHandler();
      var future = cnx.processHandler(handler);
      future.then((x) {
        log.fine("Pinged");
        releaseConnection(cnx);
        c.complete(x);
      });
      handleFutureException(future, c);
    });
    handleFutureException(cnxFuture, c);
    
    return c.future;
  }
  
  Future debug() {
    log.info("Sending debug message");
    var c = new Completer<Results>();
    
    var cnxFuture = _getConnection();
    cnxFuture.then((cnx) {
      var handler = new DebugHandler();
      var future = cnx.processHandler(handler);
      future.then((x) {
        log.fine("Message sent");
        releaseConnection(cnx);
        c.complete(x);
      });
      handleFutureException(future, c, cnx);
    });
    handleFutureException(cnxFuture, c);
    
    return c.future;
  }
  
  void _closeQuery(Query q) {
    log.finest("Closing query: ${q.sql}");
    for (var cnx in _pool) {
      var preparedQuery = cnx.removePreparedQueryFromCache(q.sql);
      if (preparedQuery != null) {
        cnx.whenReady().then((x) {
          log.finest("Connection ready - closing query: ${q.sql}");
          cnx.use();
          var handler = new CloseStatementHandler(preparedQuery.statementHandlerId);
          var future = cnx.processHandler(handler, noResponse: true);
          future.then((x) {
            releaseConnection(cnx);
          });
          future.handleException((e) {
            releaseConnection(cnx);
          });
        });
      }
    }
  }
  
  Future<Query> prepare(String sql) {
    var query = new Query._internal(this, sql);
    var c = new Completer<Query>();
    var future = query._getValueCount();
    future.then((preparedQuery) {
print("got value count");
      releaseConnection(preparedQuery.cnx);
      c.complete(query);
    });
    handleFutureException(future, c); //TODO release connection here?
    return c.future;
  }
  
  Future<Transaction> startTransaction({bool consistent: false}) {
    log.info("Starting transaction");
    var c = new Completer<Transaction>();
    
    var cnxFuture = _getConnection();
    cnxFuture.then((cnx) {
      cnx.use();
      var sql;
      if (consistent) {
        sql = "start transaction with consistent snapshot";
      } else {
        sql = "start transaction";
      }
      var handler = new QueryHandler(sql);
      var queryFuture = cnx.processHandler(handler);
      queryFuture.then((results) {
        log.fine("Transaction started");
        var transaction = new Transaction._internal(cnx, this);
        c.complete(transaction);
      });
      handleFutureException(queryFuture, c, cnx);
    });
    handleFutureException(cnxFuture, c);
    
    return c.future;
  }
  
  Future<Results> prepareExecute(String sql, List<dynamic> parameters) {
    var c = new Completer<Results>();
    Future<Query> future = prepare(sql);
    future.then((Query q) {
      for (int i = 0; i < parameters.length; i++) {
        q[i] = parameters[i];
      }
      var future = q.execute();
      future.then((Results results) {
        c.complete(results);
      });
      handleFutureException(future, c);
    });
    handleFutureException(future, c);
    return c.future;
  }
  
  handleFutureException(Future f, Completer c, [_Connection cnx]) {
    f.handleException((e) {
      c.completeException(e);
      if (?cnx) {
        releaseConnection(cnx);
      }
      return true;
    });
  }

//  dynamic fieldList(String table, [String column]);
//  dynamic refresh(bool grant, bool log, bool tables, bool hosts,
//                  bool status, bool threads, bool slave, bool master);
//  dynamic shutdown(bool def, bool waitConnections, bool waitTransactions,
//                   bool waitUpdates, bool waitAllBuffers,
//                   bool waitCriticalBuffers, bool killQuery, bool killConnection);
//  dynamic statistics();
//  dynamic processInfo();
//  dynamic processKill(int id);
//  dynamic changeUser(String user, String password, [String db]);
//  dynamic binlogDump(options);
//  dynamic registerSlave(options);
//  dynamic setOptions(int option);
}

class Query {
  final ConnectionPool _pool;
  final _Connection _cnx;
  List<dynamic> _values;
  final String sql;
  bool _executed = false;
  final Logger log;
  
//  int get statementId => _preparedQuery.statementHandlerId;
  
  Query._internal(this._pool, this.sql) :
      _cnx = null,
      log = new Logger("Query");

  Query._withConnection(this._pool, _Connection cnx, this.sql) :
      _cnx = cnx,
      log = new Logger("Query");
  
  Future<PreparedQuery> _getValueCount() {
    return _prepare();
  }

  Future<_Connection> _getConnection() {
    if (_cnx != null) {
      var c = new Completer<_Connection>();
      c.complete(_cnx);
      return c.future;
    }
    return _pool._getConnection();
  }

  Future<PreparedQuery> _prepare() {
    log.fine("Getting prepared query for: $sql");
    var c = new Completer<PreparedQuery>();
    
    var cnxFuture = _getConnection();
    cnxFuture.then((cnx) {
      log.fine("Got connection #${cnx.number}");
      var preparedQuery = cnx.getPreparedQueryFromCache(sql);
      if (preparedQuery != null) {
        log.fine("Got prepared query from cache in connection #${cnx.number} for: $sql");
        c.complete(preparedQuery);
        return c.future;
      }
      
      log.fine("Preparing new query in connection #${cnx.number} for: $sql");
      var handler = new PrepareHandler(sql);
      Future<PreparedQuery> queryFuture = cnx.processHandler(handler);
      queryFuture.then((preparedQuery) {
        if (preparedQuery is Results) {
          print("Oops");
        }
        log.fine("Prepared new query in connection #${cnx.number} for: $sql");
        preparedQuery.cnx = cnx;
        cnx.putPreparedQueryInCache(sql, preparedQuery);
        if (_values == null) {
          _values = new List<dynamic>(preparedQuery.parameters.length);
        }
        c.complete(preparedQuery);
      });
      handleFutureException(queryFuture, c, cnx);
    });
    handleFutureException(cnxFuture, c);
    return c.future;
  }
      
  handleFutureException(Future f, Completer c, [_Connection cnx]) {
    f.handleException((e) {
      c.completeException(e);
      if (?cnx) {
        _pool.releaseConnection(cnx);
      }
      return true;
    });
  }

  void close() {
    _pool._closeQuery(this);
  }
  
  Future<Results> execute() {
    var c = new Completer<Results>();
    var future = _prepare();
    future.then((preparedQuery) {
      var future = _execute(preparedQuery, retain: false);
      future.then((Results results) {
        c.complete(results);
      });
      handleFutureException(future, c);
    });
    handleFutureException(future, c);
    return c.future;
  }
  
  Future<Results> _execute(PreparedQuery preparedQuery, {bool retain: false}) {
    var c = new Completer<Results>();
    var handler = new ExecuteQueryHandler(preparedQuery, _executed, _values);
    var handlerFuture = preparedQuery.cnx.processHandler(handler);
    handlerFuture.then((results) {
      log.finest("Prepared query finished with connection");
      c.complete(results);
    });
    handleFutureException(handlerFuture, c, preparedQuery.cnx);
    return c.future;
  }
  
  Future<List<Results>> executeMulti(List<List<dynamic>> parameters) {
    var c = new Completer<List<Results>>();
    var future = _prepare();
    future.then((preparedQuery) {
      var resultList = new List<Results>();
      exec(int i) {
        _values.setRange(0, _values.length, parameters[i]);
        var future = _execute(preparedQuery, retain: true);
        future.then((Results results) {
          resultList.add(results);
          if (i < parameters.length - 1) {
            exec(i + 1);
          } else {
            _pool.releaseConnection(preparedQuery.cnx);
            c.complete(resultList);
          }
        });
        handleFutureException(future, c, preparedQuery.cnx);
      }
      exec(0);
    });
    handleFutureException(future, c);
    return c.future;
  } 
  
  Future<int> executeUpdate() {
    
  }

  dynamic operator [](int pos) => _values[pos];
  
  void operator []=(int index, dynamic value) {
    _values[index] = value;
    _executed = false;
  }
  
//  dynamic longData(int index, data);
//  dynamic reset();
//  dynamic fetch(int rows);
}

class Transaction {
  _Connection cnx;
  ConnectionPool _pool;
  bool _finished;
  
  // TODO: maybe give the connection a link to its transaction?

  handleFutureException(Future f, Completer c) {
    f.handleException((e) {
      c.completeException(e);
      return true;
    });
  }
  
  Transaction._internal(this.cnx, this._pool) : _finished = false;
  
  Future commit() {
    _checkFinished();
    _finished = true;
    var c = new Completer();
  
    var handler = new QueryHandler("commit");
    var queryFuture = cnx.processHandler(handler);
    queryFuture.then((results) {
      _pool.releaseConnection(cnx);
      c.complete(results);
    });
    handleFutureException(queryFuture, c);

    return c.future;
  }
  
  Future rollback() {
    _checkFinished();
    _finished = true;
    var c = new Completer();
  
    var handler = new QueryHandler("rollback");
    var queryFuture = cnx.processHandler(handler);
    queryFuture.then((results) {
      _pool.releaseConnection(cnx);
      c.complete(results);
    });
    handleFutureException(queryFuture, c);

    return c.future;
  }

  Future<Results> query(String sql) {
    _checkFinished();
    var c = new Completer<Results>();
    
    var handler = new QueryHandler(sql);
    var queryFuture = cnx.processHandler(handler);
    queryFuture.then((results) {
      c.complete(results);
    });
    handleFutureException(queryFuture, c);

    return c.future;
  }
  
  Future<Query> prepare(String sql) {
    _checkFinished();
    var query = new Query._withConnection(_pool, cnx, sql);
    var c = new Completer<Query>();
    var future = query._getValueCount();
    future.then((preparedQuery) {
      preparedQuery.cnx.release();
      c.complete(query);
    });
    handleFutureException(future, c);
    return c.future;
  }
  
  Future<Results> prepareExecute(String sql, List<dynamic> parameters) {
    _checkFinished();
  }

  void _checkFinished() {
    if (_finished) {
      throw new StateError("Transaction has already finished");
    }
  }
}
